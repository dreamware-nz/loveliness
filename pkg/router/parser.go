package router

import (
	"fmt"
	"strings"
	"unicode"
)

// QueryType indicates what kind of Cypher statement was parsed.
type QueryType int

const (
	QueryMatch  QueryType = iota // MATCH ... RETURN
	QueryCreate                  // CREATE
)

// ParsedQuery is the result of parsing a Cypher subset for shard routing.
//
//	Supported:
//	  MATCH (v:Label) WHERE v.prop = 'value' RETURN ...
//	  MATCH (v:Label {prop: 'value'}) RETURN ...
//	  MATCH (a:L)-[:R]->(b:L) WHERE ... RETURN ...
//	  CREATE (v:Label {prop: 'value', ...})
//	  CREATE (a)-[:R]->(b)
//	  RETURN ... LIMIT N / ORDER BY ...
//
//	Not supported (returns error):
//	  MERGE, SET, DELETE, WITH, UNWIND, CALL, OPTIONAL MATCH, subqueries
type ParsedQuery struct {
	Type      QueryType
	ShardKeys []string // extracted property values for shard routing
	Raw       string   // original Cypher string (passed to LadybugDB as-is)
}

// NeedsScatterGather returns true if no shard key could be extracted,
// meaning the query must be sent to all shards.
func (pq *ParsedQuery) NeedsScatterGather() bool {
	return len(pq.ShardKeys) == 0
}

var unsupportedClauses = []string{
	"MERGE", "SET ", "DELETE", "REMOVE", "DETACH",
	"WITH ", "UNWIND", "CALL", "FOREACH", "LOAD CSV",
	"OPTIONAL MATCH",
}

// Parse parses a Cypher query string and extracts shard routing keys.
// It does NOT fully parse Cypher — it extracts just enough to determine
// which shard(s) the query should be routed to.
func Parse(cypher string) (*ParsedQuery, error) {
	trimmed := strings.TrimSpace(cypher)
	if trimmed == "" {
		return nil, fmt.Errorf("empty query")
	}

	upper := strings.ToUpper(trimmed)

	// Check for unsupported clauses.
	for _, clause := range unsupportedClauses {
		if strings.Contains(upper, clause) {
			return nil, fmt.Errorf("unsupported clause: %s", strings.TrimSpace(clause))
		}
	}

	pq := &ParsedQuery{Raw: trimmed}

	if strings.HasPrefix(upper, "CREATE") {
		pq.Type = QueryCreate
	} else if strings.HasPrefix(upper, "MATCH") {
		pq.Type = QueryMatch
	} else {
		return nil, fmt.Errorf("unsupported statement: must start with MATCH or CREATE")
	}

	// Extract shard keys from inline properties: {prop: 'value'}
	keys := extractInlineProperties(trimmed)
	pq.ShardKeys = append(pq.ShardKeys, keys...)

	// Extract shard keys from WHERE clause: WHERE v.prop = 'value'
	keys = extractWhereEquality(trimmed)
	pq.ShardKeys = append(pq.ShardKeys, keys...)

	// Deduplicate.
	pq.ShardKeys = dedupe(pq.ShardKeys)

	return pq, nil
}

// extractInlineProperties extracts string values from {key: 'value'} patterns.
func extractInlineProperties(cypher string) []string {
	var keys []string
	i := 0
	for i < len(cypher) {
		// Find opening brace.
		start := strings.IndexByte(cypher[i:], '{')
		if start == -1 {
			break
		}
		start += i
		end := strings.IndexByte(cypher[start:], '}')
		if end == -1 {
			break
		}
		end += start

		block := cypher[start+1 : end]
		// Parse key: 'value' pairs.
		pairs := strings.Split(block, ",")
		for _, pair := range pairs {
			parts := strings.SplitN(pair, ":", 2)
			if len(parts) != 2 {
				continue
			}
			val := strings.TrimSpace(parts[1])
			if extracted, ok := extractStringLiteral(val); ok {
				keys = append(keys, extracted)
			}
		}
		i = end + 1
	}
	return keys
}

// extractWhereEquality extracts values from WHERE ... prop = 'value' patterns.
func extractWhereEquality(cypher string) []string {
	upper := strings.ToUpper(cypher)
	whereIdx := strings.Index(upper, "WHERE ")
	if whereIdx == -1 {
		return nil
	}

	// Find the end of the WHERE clause (RETURN, ORDER BY, LIMIT, or end of string).
	whereClause := cypher[whereIdx+6:]
	for _, terminator := range []string{"RETURN", "ORDER BY", "LIMIT"} {
		if idx := strings.Index(strings.ToUpper(whereClause), terminator); idx != -1 {
			whereClause = whereClause[:idx]
		}
	}

	var keys []string
	// Split on AND/OR and look for equality conditions.
	conditions := splitConditions(whereClause)
	for _, cond := range conditions {
		cond = strings.TrimSpace(cond)
		// Look for = sign (but not !=, <=, >=).
		eqIdx := -1
		for j := 0; j < len(cond); j++ {
			if cond[j] == '=' && j > 0 && cond[j-1] != '!' && cond[j-1] != '<' && cond[j-1] != '>' {
				eqIdx = j
				break
			}
		}
		if eqIdx == -1 {
			continue
		}
		rhs := strings.TrimSpace(cond[eqIdx+1:])
		if extracted, ok := extractStringLiteral(rhs); ok {
			keys = append(keys, extracted)
		}
	}
	return keys
}

// splitConditions splits a WHERE clause on AND/OR boundaries.
func splitConditions(clause string) []string {
	upper := strings.ToUpper(clause)
	var result []string
	current := 0
	for {
		andIdx := strings.Index(upper[current:], " AND ")
		orIdx := strings.Index(upper[current:], " OR ")

		next := -1
		width := 0
		if andIdx != -1 && (orIdx == -1 || andIdx < orIdx) {
			next = andIdx
			width = 5 // " AND "
		} else if orIdx != -1 {
			next = orIdx
			width = 4 // " OR "
		}

		if next == -1 {
			result = append(result, clause[current:])
			break
		}
		result = append(result, clause[current:current+next])
		current = current + next + width
	}
	return result
}

// extractStringLiteral extracts a string from 'value' or "value" delimiters.
func extractStringLiteral(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 2 {
		return "", false
	}
	if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
		return s[1 : len(s)-1], true
	}
	return "", false
}

func dedupe(items []string) []string {
	seen := make(map[string]struct{}, len(items))
	out := make([]string, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

// IsIdentChar returns true if c is a valid identifier character.
func IsIdentChar(c byte) bool {
	return unicode.IsLetter(rune(c)) || unicode.IsDigit(rune(c)) || c == '_'
}
