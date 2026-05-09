package router

import "regexp"

// allShortestPattern matches `ALL SHORTEST` as bare keywords, allowing
// any whitespace (including newlines) between them. Word boundaries
// keep `OVERALL SHORTEST` or `ALLSHORTEST` from triggering.
var allShortestPattern = regexp.MustCompile(`(?i)\bALL\s+SHORTEST\b`)

// containsAllShortest reports whether the query uses LadybugDB's
// `ALL SHORTEST` variable-length path syntax once string literals and
// Cypher comments are stripped — those are the contexts where the
// keywords could appear without being parsed as the unsafe construct.
//
// This is a defensive scanner, not a full parser. It accepts false
// positives only on inputs that look syntactically like Cypher with
// `ALL SHORTEST` at the top level. False negatives would require a
// quoting/comment style we don't recognise, which would also fail to
// parse downstream.
func containsAllShortest(cypher string) bool {
	return allShortestPattern.MatchString(stripLiteralsAndComments(cypher))
}

// stripLiteralsAndComments replaces the contents of Cypher string
// literals (single-quoted, double-quoted, backticked) and comments
// (`//...` to end of line, `/* ... */`) with spaces, preserving
// length so error positions still line up. Only the keyword scanner
// uses this — the original cypher is what gets handed to LadybugDB.
func stripLiteralsAndComments(s string) string {
	out := []byte(s)
	i := 0
	for i < len(out) {
		c := out[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			quote := c
			out[i] = ' '
			i++
			for i < len(out) {
				if out[i] == '\\' && i+1 < len(out) {
					out[i] = ' '
					out[i+1] = ' '
					i += 2
					continue
				}
				if out[i] == quote {
					out[i] = ' '
					i++
					break
				}
				out[i] = ' '
				i++
			}
		case c == '/' && i+1 < len(out) && out[i+1] == '/':
			for i < len(out) && out[i] != '\n' {
				out[i] = ' '
				i++
			}
		case c == '/' && i+1 < len(out) && out[i+1] == '*':
			out[i] = ' '
			out[i+1] = ' '
			i += 2
			for i+1 < len(out) {
				if out[i] == '*' && out[i+1] == '/' {
					out[i] = ' '
					out[i+1] = ' '
					i += 2
					break
				}
				if out[i] != '\n' {
					out[i] = ' '
				}
				i++
			}
			// If we exited without finding `*/`, blank the rest.
			for ; i < len(out); i++ {
				if out[i] != '\n' {
					out[i] = ' '
				}
			}
		default:
			i++
		}
	}
	return string(out)
}

// errAllShortestUnsafe is the message returned when an `ALL SHORTEST`
// query is rejected. Includes a workaround pointer so end users have
// somewhere to go; see GitHub issue #1.
const errAllShortestUnsafe = "ALL SHORTEST variable-length path queries are disabled — they segfault the LadybugDB native layer (see github.com/johnjansen/loveliness#1). Use SHORTEST (single shortest path) instead."

// allShortestEnabled is set by the binary when the operator opts in
// via LOVELINESS_ALLOW_ALL_SHORTEST_UNSAFE=true. Default is off; the
// query rejection is the only thing standing between the user and a
// host-process segfault, so flipping this requires a deliberate act.
var allShortestEnabled = false

// SetAllowAllShortestUnsafe toggles the runtime gate at startup.
// Tests use it to assert both branches; production code wires it from
// config.AllowAllShortestUnsafe.
func SetAllowAllShortestUnsafe(allow bool) {
	allShortestEnabled = allow
}

// rejectIfUnsafe returns a *QueryError if cypher contains a known
// crash-trigger pattern and the unsafe gate is closed. Returning nil
// means the query is safe to forward.
func rejectIfUnsafe(cypher string) *QueryError {
	if allShortestEnabled {
		return nil
	}
	if containsAllShortest(cypher) {
		return &QueryError{Code: "UNSAFE_QUERY", Message: errAllShortestUnsafe}
	}
	return nil
}

