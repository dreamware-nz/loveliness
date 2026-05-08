package mcp

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

// inlineParams substitutes $name placeholders in a Cypher query with
// escaped literals from the params map. Loveliness's HTTP /cypher
// endpoint accepts only a raw Cypher body, so MCP-side param binding
// must flatten to literals.
//
// Supported literal types: string, bool, int/float (number), nil, and
// slices/maps of the above (rendered as JSON-ish Cypher literals). We
// intentionally keep this conservative — exotic types become an error.
//
// Placeholder matching follows openCypher: a '$' followed by one or
// more identifier characters ([A-Za-z_][A-Za-z0-9_]*). Occurrences
// inside single/double-quoted strings are left alone.
func inlineParams(query string, params map[string]any) (string, error) {
	// We still need to scan even when params is empty, so that unknown
	// $placeholders surface as errors rather than being silently passed
	// through to the server.
	if params == nil {
		params = map[string]any{}
	}
	if len(params) == 0 && !hasPlaceholder(query) {
		return query, nil
	}

	var out strings.Builder
	out.Grow(len(query))

	runes := []rune(query)
	i := 0
	inString := rune(0) // 0 = not in a string, else the opening quote
	for i < len(runes) {
		r := runes[i]
		// Track string boundaries (with minimal escape handling).
		if inString != 0 {
			out.WriteRune(r)
			if r == '\\' && i+1 < len(runes) {
				out.WriteRune(runes[i+1])
				i += 2
				continue
			}
			if r == inString {
				inString = 0
			}
			i++
			continue
		}
		if r == '\'' || r == '"' {
			inString = r
			out.WriteRune(r)
			i++
			continue
		}
		if r == '$' && i+1 < len(runes) && isIdentStart(runes[i+1]) {
			j := i + 1
			for j < len(runes) && isIdentCont(runes[j]) {
				j++
			}
			name := string(runes[i+1 : j])
			val, ok := params[name]
			if !ok {
				return "", fmt.Errorf("cypher param %q referenced but not provided", name)
			}
			lit, err := cypherLiteral(val)
			if err != nil {
				return "", fmt.Errorf("cypher param %q: %w", name, err)
			}
			out.WriteString(lit)
			i = j
			continue
		}
		out.WriteRune(r)
		i++
	}
	return out.String(), nil
}

// hasPlaceholder reports whether q contains a $name placeholder outside
// of string literals. It's a cheap pre-filter — callers that have real
// params will always fall through to the full scanner anyway.
func hasPlaceholder(q string) bool {
	runes := []rune(q)
	inString := rune(0)
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if inString != 0 {
			if r == '\\' && i+1 < len(runes) {
				i++
				continue
			}
			if r == inString {
				inString = 0
			}
			continue
		}
		if r == '\'' || r == '"' {
			inString = r
			continue
		}
		if r == '$' && i+1 < len(runes) && isIdentStart(runes[i+1]) {
			return true
		}
	}
	return false
}

func isIdentStart(r rune) bool {
	return r == '_' || unicode.IsLetter(r)
}

func isIdentCont(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// cypherLiteral renders a Go value as a Cypher literal.
// Strings use single quotes with minimal escaping; numbers/bools/nil
// render as their Cypher forms. Complex values are JSON-encoded —
// openCypher accepts JSON-shaped map/list literals for most drivers.
func cypherLiteral(v any) (string, error) {
	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if x {
			return "true", nil
		}
		return "false", nil
	case string:
		return quoteCypherString(x), nil
	case json.Number:
		return x.String(), nil
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return fmt.Sprintf("%v", x), nil
	case []any, map[string]any:
		// Fallback: JSON encoding. Cypher map/list syntax is a superset
		// compatible with JSON for scalar leaves, so this is acceptable
		// for the "pass a struct as a param" use case.
		b, err := json.Marshal(x)
		if err != nil {
			return "", err
		}
		return string(b), nil
	default:
		// Best-effort JSON fallback; if it fails, surface a clear error.
		b, err := json.Marshal(x)
		if err != nil {
			return "", fmt.Errorf("unsupported param type %T", v)
		}
		return string(b), nil
	}
}

func quoteCypherString(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('\'')
	for _, r := range s {
		switch r {
		case '\'':
			b.WriteString("\\'")
		case '\\':
			b.WriteString("\\\\")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('\'')
	return b.String()
}
