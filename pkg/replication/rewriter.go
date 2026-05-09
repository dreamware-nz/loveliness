package replication

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mathrand "math/rand/v2"
	"strings"
	"time"
)

// Rewriter pre-computes non-deterministic Cypher functions so that replicas
// replay the same statement and reach the same state as the primary. The
// rewriter must run BEFORE the Cypher is appended to the WAL and BEFORE the
// primary executes the query, so that primary, WAL record, and every replica
// see the identical (deterministic) statement.
//
// Supported substitutions (case-insensitive on the function name, scoped to
// token boundaries — matches inside string literals or backtick-quoted
// identifiers are left untouched):
//
//	now()           → datetime('<RFC3339Nano UTC>')
//	datetime()      → datetime('<RFC3339Nano UTC>')
//	localdatetime() → localdatetime('<RFC3339 without TZ>')
//	date()          → date('YYYY-MM-DD')
//	time()          → time('HH:MM:SS.NNNNNNNNN+00:00')
//	localtime()     → localtime('HH:MM:SS.NNNNNNNNN')
//	timestamp()     → integer ms-since-epoch literal
//	rand()          → float literal in [0,1)
//	random()        → float literal in [0,1)
//	randomUUID()    → string literal containing a v4 UUID
//
// `datetime('...')` and `date('...')` calls with arguments are deterministic
// and are passed through unchanged. The rewriter only fires when the function
// is invoked with empty parentheses.
type Rewriter struct {
	Now  func() time.Time
	Rand func() float64
	UUID func() string
}

// DefaultRewriter returns a Rewriter that uses real wall-clock time, the
// math/rand/v2 PRNG (seeded by the runtime), and a crypto-random v4 UUID
// generator.
func DefaultRewriter() *Rewriter {
	return &Rewriter{
		Now:  time.Now,
		Rand: mathrand.Float64,
		UUID: newUUIDv4,
	}
}

// Rewrite returns cypher with non-deterministic function calls replaced by
// literal values. If no rewrites are needed the original string is returned
// unchanged.
func (rw *Rewriter) Rewrite(cypher string) string {
	if cypher == "" {
		return cypher
	}
	// Cheap pre-check: if the input doesn't contain a paren or any keyword
	// candidate, skip the full walk.
	if !mayContainNonDet(cypher) {
		return cypher
	}
	return walkAndRewrite(cypher, rw)
}

// mayContainNonDet does a fast lower-cased substring scan. False positives
// are fine — the full walker re-validates token boundaries.
func mayContainNonDet(s string) bool {
	if !strings.ContainsRune(s, '(') {
		return false
	}
	lower := strings.ToLower(s)
	for _, k := range nonDetKeywords {
		if strings.Contains(lower, k) {
			return true
		}
	}
	return false
}

var nonDetKeywords = []string{
	"now", "datetime", "localdatetime", "date", "time", "localtime",
	"timestamp", "rand", "random", "randomuuid",
}

func walkAndRewrite(s string, rw *Rewriter) string {
	var out strings.Builder
	out.Grow(len(s))
	i := 0
	for i < len(s) {
		c := s[i]

		// Single-, double-, and backtick-quoted regions are passed through
		// verbatim. We do not interpret their contents.
		if c == '\'' || c == '"' || c == '`' {
			j := skipQuoted(s, i)
			out.WriteString(s[i:j])
			i = j
			continue
		}

		// Line and block comments — passed through verbatim.
		if c == '/' && i+1 < len(s) {
			if s[i+1] == '/' {
				j := i + 2
				for j < len(s) && s[j] != '\n' {
					j++
				}
				out.WriteString(s[i:j])
				i = j
				continue
			}
			if s[i+1] == '*' {
				j := i + 2
				for j+1 < len(s) && (s[j] != '*' || s[j+1] != '/') {
					j++
				}
				if j+1 < len(s) {
					j += 2
				}
				out.WriteString(s[i:j])
				i = j
				continue
			}
		}

		// Identifier candidate — must start at a token boundary.
		if isIdentStart(c) && (i == 0 || !isIdentRune(s[i-1])) {
			end := i + 1
			for end < len(s) && isIdentRune(s[end]) {
				end++
			}
			ident := s[i:end]
			if replacement, consumed, ok := matchNonDet(s, i, end, ident, rw); ok {
				out.WriteString(replacement)
				i += consumed
				continue
			}
			out.WriteString(ident)
			i = end
			continue
		}

		out.WriteByte(c)
		i++
	}
	return out.String()
}

// skipQuoted returns the index just past the closing quote at s[i]. Handles
// backslash escapes for single/double quotes; backtick-quoted identifiers
// have no escape syntax and end at the next backtick.
func skipQuoted(s string, i int) int {
	quote := s[i]
	j := i + 1
	for j < len(s) {
		ch := s[j]
		if quote != '`' && ch == '\\' && j+1 < len(s) {
			j += 2
			continue
		}
		if ch == quote {
			return j + 1
		}
		j++
	}
	return j // unterminated — consume to end
}

// matchNonDet checks whether the identifier at s[start:end] is a
// non-deterministic function call we should rewrite. Returns (replacement,
// total bytes consumed from start, ok). If ok is false the caller emits the
// original identifier unchanged.
func matchNonDet(s string, start, end int, ident string, rw *Rewriter) (string, int, bool) {
	lower := strings.ToLower(ident)

	// Most rewrites require empty parens. randomUUID and timestamp also
	// require parens (to avoid clobbering property names).
	parenEnd, hasEmptyParens := scanEmptyParens(s, end)

	switch lower {
	case "now":
		if !hasEmptyParens {
			return "", 0, false
		}
		ts := rw.Now().UTC()
		return fmt.Sprintf("datetime('%s')", ts.Format(time.RFC3339Nano)), parenEnd - start, true

	case "datetime":
		if !hasEmptyParens {
			return "", 0, false
		}
		ts := rw.Now().UTC()
		return fmt.Sprintf("datetime('%s')", ts.Format(time.RFC3339Nano)), parenEnd - start, true

	case "localdatetime":
		if !hasEmptyParens {
			return "", 0, false
		}
		ts := rw.Now()
		return fmt.Sprintf("localdatetime('%s')", ts.Format("2006-01-02T15:04:05.999999999")), parenEnd - start, true

	case "date":
		if !hasEmptyParens {
			return "", 0, false
		}
		ts := rw.Now()
		return fmt.Sprintf("date('%s')", ts.Format("2006-01-02")), parenEnd - start, true

	case "time":
		if !hasEmptyParens {
			return "", 0, false
		}
		ts := rw.Now().UTC()
		return fmt.Sprintf("time('%s')", ts.Format("15:04:05.999999999-07:00")), parenEnd - start, true

	case "localtime":
		if !hasEmptyParens {
			return "", 0, false
		}
		ts := rw.Now()
		return fmt.Sprintf("localtime('%s')", ts.Format("15:04:05.999999999")), parenEnd - start, true

	case "timestamp":
		if !hasEmptyParens {
			return "", 0, false
		}
		ms := rw.Now().UnixMilli()
		return fmt.Sprintf("%d", ms), parenEnd - start, true

	case "rand", "random":
		if !hasEmptyParens {
			return "", 0, false
		}
		return formatFloat(rw.Rand()), parenEnd - start, true

	case "randomuuid":
		if !hasEmptyParens {
			return "", 0, false
		}
		return "'" + rw.UUID() + "'", parenEnd - start, true
	}

	return "", 0, false
}

// scanEmptyParens checks whether s[end:] begins (after optional whitespace)
// with "()" — i.e. the function is invoked with no arguments. Returns the
// index just past the closing paren and true. If args are present (or no
// parens), returns 0/false.
func scanEmptyParens(s string, end int) (int, bool) {
	j := end
	for j < len(s) && (s[j] == ' ' || s[j] == '\t') {
		j++
	}
	if j >= len(s) || s[j] != '(' {
		return 0, false
	}
	j++
	for j < len(s) && (s[j] == ' ' || s[j] == '\t') {
		j++
	}
	if j >= len(s) || s[j] != ')' {
		return 0, false
	}
	return j + 1, true
}

func isIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isIdentRune(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9')
}

// formatFloat returns a Cypher-safe decimal representation. We avoid
// scientific notation because some Cypher dialects refuse it.
func formatFloat(v float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.15f", v), "0"), ".")
}

// newUUIDv4 returns an RFC 4122 v4 UUID string. We avoid pulling in a
// third-party library for one function.
func newUUIDv4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "00000000-0000-4000-8000-000000000000"
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	hexed := hex.EncodeToString(b[:])
	return hexed[0:8] + "-" + hexed[8:12] + "-" + hexed[12:16] + "-" + hexed[16:20] + "-" + hexed[20:32]
}
