package router

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/johnjansen/loveliness/pkg/schema"
)

// QueryType classifies statements for routing decisions.
type QueryType int

const (
	QueryRead   QueryType = iota // MATCH, OPTIONAL MATCH, CALL, UNWIND — safe to scatter-gather
	QueryWrite                   // CREATE, MERGE, SET, DELETE, REMOVE — needs shard routing
	QuerySchema                  // CREATE NODE TABLE, DROP TABLE, etc. — broadcast to all shards
)

// ParsedQuery is the result of analyzing a Cypher statement for shard routing.
// The parser does NOT validate Cypher — LadybugDB does that. The parser only
// extracts enough information to decide where to send the query.
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

// IsWrite returns true if this is a data-modifying statement.
func (pq *ParsedQuery) IsWrite() bool {
	return pq.Type == QueryWrite
}

// writeClauses are Cypher clauses that modify data.
var writeClauses = []string{
	"CREATE ", "MERGE ", "SET ", "DELETE ", "REMOVE ", "DETACH ",
}

// schemaPrefixes are DDL statements that must be broadcast to all shards.
var schemaPrefixes = []string{
	"CREATE NODE TABLE", "CREATE REL TABLE", "CREATE RELATIONSHIP TABLE",
	"CREATE TABLE", "DROP TABLE", "DROP ", "ALTER ",
}

// Parse analyzes a Cypher query string and extracts shard routing keys.
// If a schema registry is provided, it uses declared shard keys per table.
// Otherwise, it falls back to extracting the first string literal (legacy behavior).
func Parse(cypher string) (*ParsedQuery, error) {
	return ParseWithSchema(cypher, nil)
}

// ParseWithSchema analyzes a Cypher query using the schema registry
// for table-aware shard key extraction.
func ParseWithSchema(cypher string, reg *schema.Registry) (*ParsedQuery, error) {
	trimmed := strings.TrimSpace(cypher)
	if trimmed == "" {
		return nil, fmt.Errorf("empty query")
	}

	upper := strings.ToUpper(trimmed)
	pq := &ParsedQuery{Raw: trimmed}

	// Classify the query type.
	pq.Type = classifyQuery(upper)

	// For schema DDL, handle registration and return early.
	if pq.Type == QuerySchema && reg != nil {
		handleSchemaRegistration(trimmed, reg)
	}

	// Extract shard keys.
	if reg != nil {
		pq.ShardKeys = extractShardKeysWithSchema(trimmed, reg)

		// Only fall back to legacy if no labels in the query matched a registered table.
		// If a table IS registered but its shard key isn't in the query, that means
		// scatter-gather (reads) or reject (writes) — NOT wrong-shard routing.
		if len(pq.ShardKeys) == 0 && !queryReferencesRegisteredTable(trimmed, reg) {
			pq.ShardKeys = legacyExtract(trimmed)
		}
	} else {
		pq.ShardKeys = legacyExtract(trimmed)
	}

	pq.ShardKeys = dedupe(pq.ShardKeys)
	return pq, nil
}

// handleSchemaRegistration registers/removes table schemas from DDL statements.
func handleSchemaRegistration(cypher string, reg *schema.Registry) {
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	if strings.HasPrefix(upper, "CREATE NODE TABLE") {
		name, key, err := schema.ParseCreateNodeTable(cypher)
		if err == nil {
			reg.Register(name, key)
		}
	} else if strings.HasPrefix(upper, "DROP TABLE") {
		name, err := schema.ParseDropTable(cypher)
		if err == nil {
			reg.Remove(name)
		}
	}
}

// extractShardKeysWithSchema uses the schema registry to find the correct
// shard key value for the query's target table.
//
// Given: MATCH (p:Person {name: 'Alice', city: 'Auckland'}) RETURN p
// And:   Person's shard key is 'name'
// Returns: ["Alice"]  (not "Auckland")
func extractShardKeysWithSchema(cypher string, reg *schema.Registry) []string {
	var keys []string

	// Find all (variable:Label) patterns and look up their shard keys.
	labels := extractLabels(cypher)
	for _, label := range labels {
		shardKeyProp := reg.GetShardKey(label.name)
		if shardKeyProp == "" {
			continue
		}

		// Look for the shard key property in inline properties of this label.
		if label.propsStart > 0 && label.propsEnd > label.propsStart {
			props := cypher[label.propsStart+1 : label.propsEnd]
			if val, ok := extractPropertyValue(props, shardKeyProp); ok {
				keys = append(keys, val)
			}
		}

		// Look for the shard key property in WHERE clause.
		if val, ok := extractWherePropertyValue(cypher, label.variable, shardKeyProp); ok {
			keys = append(keys, val)
		}
	}

	return keys
}

// labelInfo stores parsed (variable:Label {props}) position info.
type labelInfo struct {
	variable   string // e.g., "p"
	name       string // e.g., "Person"
	propsStart int    // index of '{', or -1
	propsEnd   int    // index of '}', or -1
}

// extractLabels finds all (variable:Label) patterns in Cypher.
func extractLabels(cypher string) []labelInfo {
	var labels []labelInfo
	i := 0
	for i < len(cypher) {
		// Find opening paren.
		paren := strings.IndexByte(cypher[i:], '(')
		if paren == -1 {
			break
		}
		paren += i

		// Find closing paren.
		closeParen := findMatchingParen(cypher, paren)
		if closeParen == -1 {
			break
		}

		inner := cypher[paren+1 : closeParen]

		// Look for variable:Label pattern.
		colonIdx := strings.IndexByte(inner, ':')
		if colonIdx != -1 {
			variable := strings.TrimSpace(inner[:colonIdx])
			rest := inner[colonIdx+1:]

			// Label ends at space, {, ), or end.
			labelEnd := len(rest)
			for j, c := range rest {
				if c == ' ' || c == '{' || c == ')' || c == '-' {
					labelEnd = j
					break
				}
			}
			labelName := strings.TrimSpace(rest[:labelEnd])

			li := labelInfo{
				variable:   variable,
				name:       labelName,
				propsStart: -1,
				propsEnd:   -1,
			}

			// Check for inline properties.
			braceIdx := strings.IndexByte(inner, '{')
			if braceIdx != -1 {
				braceClose := strings.IndexByte(inner[braceIdx:], '}')
				if braceClose != -1 {
					li.propsStart = paren + 1 + braceIdx
					li.propsEnd = paren + 1 + braceIdx + braceClose
				}
			}

			if labelName != "" {
				labels = append(labels, li)
			}
		}

		i = closeParen + 1
	}
	return labels
}

// findMatchingParen finds the closing ')' for an opening '(' at position start.
func findMatchingParen(s string, start int) int {
	depth := 0
	for i := start; i < len(s); i++ {
		if s[i] == '(' {
			depth++
		} else if s[i] == ')' {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// extractPropertyValue extracts the value for a specific key from a {key: 'value', ...} block.
func extractPropertyValue(propsBlock, targetKey string) (string, bool) {
	pairs := strings.Split(propsBlock, ",")
	for _, pair := range pairs {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if strings.EqualFold(key, targetKey) {
			val := strings.TrimSpace(parts[1])
			if extracted, ok := extractStringLiteral(val); ok {
				return extracted, true
			}
		}
	}
	return "", false
}

// extractWherePropertyValue looks for `variable.property = 'value'` in WHERE clause.
func extractWherePropertyValue(cypher, variable, property string) (string, bool) {
	upper := strings.ToUpper(cypher)
	whereIdx := strings.Index(upper, "WHERE ")
	if whereIdx == -1 {
		return "", false
	}

	whereClause := cypher[whereIdx+6:]
	for _, terminator := range []string{"RETURN", "ORDER BY", "LIMIT", " SET ", " DELETE", " REMOVE"} {
		if idx := strings.Index(strings.ToUpper(whereClause), terminator); idx != -1 {
			whereClause = whereClause[:idx]
		}
	}

	// Build the pattern: "variable.property"
	pattern := variable + "." + property

	conditions := splitConditions(whereClause)
	for _, cond := range conditions {
		cond = strings.TrimSpace(cond)
		// Check if this condition references our variable.property
		if !containsIgnoreCase(cond, pattern) {
			continue
		}
		eqIdx := findEqualitySign(cond)
		if eqIdx == -1 {
			continue
		}
		rhs := strings.TrimSpace(cond[eqIdx+1:])
		if extracted, ok := extractStringLiteral(rhs); ok {
			return extracted, true
		}
	}
	return "", false
}

func containsIgnoreCase(s, substr string) bool {
	return strings.Contains(strings.ToUpper(s), strings.ToUpper(substr))
}

func findEqualitySign(cond string) int {
	for j := 0; j < len(cond); j++ {
		if cond[j] == '=' && j > 0 && cond[j-1] != '!' && cond[j-1] != '<' && cond[j-1] != '>' {
			return j
		}
	}
	return -1
}

// classifyQuery determines whether a statement is a read, write, or schema operation.
func classifyQuery(upper string) QueryType {
	for _, prefix := range schemaPrefixes {
		if strings.HasPrefix(upper, prefix) {
			return QuerySchema
		}
	}
	for _, clause := range writeClauses {
		if strings.Contains(upper, clause) {
			return QueryWrite
		}
	}
	return QueryRead
}

// queryReferencesRegisteredTable returns true if the query mentions any label
// that has a registered schema. This determines whether to fall back to legacy
// extraction or let the query scatter-gather.
func queryReferencesRegisteredTable(cypher string, reg *schema.Registry) bool {
	labels := extractLabels(cypher)
	for _, label := range labels {
		if reg.GetShardKey(label.name) != "" {
			return true
		}
	}
	return false
}

// legacyExtract returns shard keys using the legacy (non-schema-aware) method.
func legacyExtract(cypher string) []string {
	var keys []string
	keys = append(keys, extractInlineProperties(cypher)...)
	keys = append(keys, extractWhereEquality(cypher)...)
	return keys
}

// --- Legacy extraction (used when no schema registry is available) ---

// extractInlineProperties extracts string values from {key: 'value'} patterns.
func extractInlineProperties(cypher string) []string {
	var keys []string
	i := 0
	for i < len(cypher) {
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

	whereClause := cypher[whereIdx+6:]
	for _, terminator := range []string{"RETURN", "ORDER BY", "LIMIT", " SET ", " DELETE", " REMOVE"} {
		if idx := strings.Index(strings.ToUpper(whereClause), terminator); idx != -1 {
			whereClause = whereClause[:idx]
		}
	}

	var keys []string
	conditions := splitConditions(whereClause)
	for _, cond := range conditions {
		cond = strings.TrimSpace(cond)
		eqIdx := findEqualitySign(cond)
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
			width = 5
		} else if orIdx != -1 {
			next = orIdx
			width = 4
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
