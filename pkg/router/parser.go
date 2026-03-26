package router

import (
	"fmt"
	"strings"
	"unicode"
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
// It classifies the query as read, write, or schema, and extracts property
// values that can be used as shard keys. The raw Cypher is passed through
// to LadybugDB unmodified.
func Parse(cypher string) (*ParsedQuery, error) {
	trimmed := strings.TrimSpace(cypher)
	if trimmed == "" {
		return nil, fmt.Errorf("empty query")
	}

	upper := strings.ToUpper(trimmed)
	pq := &ParsedQuery{Raw: trimmed}

	// Classify the query type.
	pq.Type = classifyQuery(upper)

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

// classifyQuery determines whether a statement is a read, write, or schema operation.
func classifyQuery(upper string) QueryType {
	// Schema DDL takes priority — must be broadcast to all shards.
	for _, prefix := range schemaPrefixes {
		if strings.HasPrefix(upper, prefix) {
			return QuerySchema
		}
	}

	// Check for write clauses anywhere in the statement.
	// A MATCH ... SET or MATCH ... DELETE is still a write.
	for _, clause := range writeClauses {
		if strings.Contains(upper, clause) {
			return QueryWrite
		}
	}

	return QueryRead
}

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
