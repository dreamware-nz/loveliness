package router

import "strings"

// TraversalInfo describes a relationship traversal pattern in a Cypher query.
// Used to detect cross-shard traversals that need multi-hop resolution.
type TraversalInfo struct {
	SourceVar   string // variable name for source node (e.g., "a")
	SourceLabel string // label for source node (e.g., "Person")
	RelType     string // relationship type (e.g., "KNOWS"), empty if untyped
	TargetVar   string // variable name for target node (e.g., "b")
	TargetLabel string // label for target node, empty if not specified
}

// ReturnColumns lists which variables have properties requested in the RETURN clause.
// e.g., "RETURN b.name, b.age, a.city" → {"b": ["name", "age"], "a": ["city"]}
type ReturnColumns map[string][]string

// extractTraversal finds the first relationship traversal pattern in a Cypher string.
// Returns nil if no traversal is found.
//
// Supported patterns:
//
//	(a:Person)-[:KNOWS]->(b:Person)
//	(a:Person)-[r:KNOWS]->(b)
//	(a)-[:KNOWS]->(b:Person)
//	(a:Person)<-[:KNOWS]-(b:Person)
func extractTraversal(cypher string) *TraversalInfo {
	// Find relationship patterns by looking for ]->(  or ]-( or ]<-(
	// Work backwards from there to find the full pattern.

	var info *TraversalInfo

	// Try outgoing: )-[ ... ]->(
	if info = findTraversalPattern(cypher, ")->"); info != nil {
		// ok
	} else if info = findTraversalPattern(cypher, ")<-"); info != nil {
		// Incoming: swap source and target.
		info.SourceVar, info.TargetVar = info.TargetVar, info.SourceVar
		info.SourceLabel, info.TargetLabel = info.TargetLabel, info.SourceLabel
	} else {
		info = findTraversalPattern(cypher, ")-")
		// Undirected.
	}

	if info == nil {
		return nil
	}

	// Resolve missing labels from other node patterns in the query.
	// e.g., MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)
	// The CREATE part has (a) and (b) without labels, but the MATCH part defines them.
	if info.SourceLabel == "" || info.TargetLabel == "" {
		varLabels := buildVarLabelMap(cypher)
		if info.SourceLabel == "" && info.SourceVar != "" {
			info.SourceLabel = varLabels[info.SourceVar]
		}
		if info.TargetLabel == "" && info.TargetVar != "" {
			info.TargetLabel = varLabels[info.TargetVar]
		}
	}

	return info
}

// buildVarLabelMap scans all (variable:Label) patterns in the query
// and builds a map of variable name → label.
func buildVarLabelMap(cypher string) map[string]string {
	m := make(map[string]string)
	labels := extractLabels(cypher)
	for _, li := range labels {
		if li.variable != "" && li.name != "" {
			if _, exists := m[li.variable]; !exists {
				m[li.variable] = li.name
			}
		}
	}
	return m
}

func findTraversalPattern(cypher, arrow string) *TraversalInfo {
	// Find )-[ which starts the relationship pattern.
	relMarker := ")-["
	if arrow == ")<-" {
		relMarker = ")<-["
	}

	idx := strings.Index(cypher, relMarker)
	if idx == -1 {
		return nil
	}

	// Parse source node: walk back from idx to find opening (.
	sourceClose := idx // position of )
	sourceOpen := findOpenParen(cypher, sourceClose)
	if sourceOpen == -1 {
		return nil
	}
	sourceInner := cypher[sourceOpen+1 : sourceClose]
	sourceVar, sourceLabel := parseNodeInner(sourceInner)

	// Parse relationship: find [ ... ].
	bracketOpen := idx + len(relMarker) - 1 // position of [
	bracketClose := strings.IndexByte(cypher[bracketOpen:], ']')
	if bracketClose == -1 {
		return nil
	}
	bracketClose += bracketOpen
	relBody := cypher[bracketOpen+1 : bracketClose]
	relType := parseRelType(relBody)

	// Parse target node: find ( ... ) after the arrow.
	afterBracket := cypher[bracketClose+1:]
	var targetOpenOffset int
	if strings.HasPrefix(afterBracket, "->(") {
		targetOpenOffset = 3
	} else if strings.HasPrefix(afterBracket, "-(") {
		targetOpenOffset = 2
	} else {
		return nil
	}
	targetOpen := bracketClose + 1 + targetOpenOffset - 1 // position of (
	targetClose := findMatchingParen(cypher, targetOpen)
	if targetClose == -1 {
		return nil
	}
	targetInner := cypher[targetOpen+1 : targetClose]
	targetVar, targetLabel := parseNodeInner(targetInner)

	return &TraversalInfo{
		SourceVar:   sourceVar,
		SourceLabel: sourceLabel,
		RelType:     relType,
		TargetVar:   targetVar,
		TargetLabel: targetLabel,
	}
}

// findOpenParen walks backwards from closePos to find the matching (.
func findOpenParen(s string, closePos int) int {
	depth := 0
	for i := closePos; i >= 0; i-- {
		if s[i] == ')' {
			depth++
		} else if s[i] == '(' {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// parseNodeInner parses "var:Label {props}" or "var" or ":Label" from inside parens.
func parseNodeInner(inner string) (variable, label string) {
	// Strip inline properties.
	braceIdx := strings.IndexByte(inner, '{')
	if braceIdx != -1 {
		inner = strings.TrimSpace(inner[:braceIdx])
	}

	colonIdx := strings.IndexByte(inner, ':')
	if colonIdx == -1 {
		return strings.TrimSpace(inner), ""
	}
	variable = strings.TrimSpace(inner[:colonIdx])
	label = strings.TrimSpace(inner[colonIdx+1:])
	// Label might have trailing spaces or other chars.
	if spaceIdx := strings.IndexAny(label, " \t{"); spaceIdx != -1 {
		label = label[:spaceIdx]
	}
	return variable, label
}

// parseRelType extracts the relationship type from inside brackets.
// Input: ":KNOWS" or "r:KNOWS" or "r:KNOWS {since: 2024}" or ""
func parseRelType(body string) string {
	body = strings.TrimSpace(body)
	// Strip properties.
	if braceIdx := strings.IndexByte(body, '{'); braceIdx != -1 {
		body = strings.TrimSpace(body[:braceIdx])
	}
	colonIdx := strings.IndexByte(body, ':')
	if colonIdx == -1 {
		return ""
	}
	relType := strings.TrimSpace(body[colonIdx+1:])
	// Remove any trailing whitespace or pipe characters (for multi-type).
	if spaceIdx := strings.IndexAny(relType, " \t|"); spaceIdx != -1 {
		relType = relType[:spaceIdx]
	}
	return relType
}

// extractReturnColumns parses the RETURN clause to find which variables
// have properties requested.
// "RETURN b.name, b.age, a.city" → {"b": ["name", "age"], "a": ["city"]}
func extractReturnColumns(cypher string) ReturnColumns {
	upper := strings.ToUpper(cypher)
	retIdx := strings.LastIndex(upper, "RETURN ")
	if retIdx == -1 {
		return nil
	}

	retClause := cypher[retIdx+7:]
	// Truncate at ORDER BY, LIMIT, etc.
	for _, term := range []string{"ORDER BY", "LIMIT", "SKIP"} {
		if idx := strings.Index(strings.ToUpper(retClause), term); idx != -1 {
			retClause = retClause[:idx]
		}
	}

	cols := make(ReturnColumns)
	parts := strings.Split(retClause, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Handle "var.prop" or "var.prop AS alias"
		if asIdx := strings.Index(strings.ToUpper(part), " AS "); asIdx != -1 {
			part = strings.TrimSpace(part[:asIdx])
		}
		dotIdx := strings.IndexByte(part, '.')
		if dotIdx == -1 {
			continue
		}
		varName := part[:dotIdx]
		propName := part[dotIdx+1:]
		cols[varName] = append(cols[varName], propName)
	}
	return cols
}
