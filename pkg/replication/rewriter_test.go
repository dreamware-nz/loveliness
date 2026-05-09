package replication

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

// frozenAt builds a Rewriter with a frozen clock and deterministic rand/UUID
// so output assertions are exact.
func frozenAt(t time.Time, randVal float64, uuid string) *Rewriter {
	return &Rewriter{
		Now:  func() time.Time { return t },
		Rand: func() float64 { return randVal },
		UUID: func() string { return uuid },
	}
}

func TestRewriter_NoOpForCleanCypher(t *testing.T) {
	rw := DefaultRewriter()
	in := "MATCH (n:Person {name: 'Alice'}) RETURN n"
	if got := rw.Rewrite(in); got != in {
		t.Errorf("expected unchanged, got: %q", got)
	}
}

func TestRewriter_NowToDatetimeLiteral(t *testing.T) {
	ts := time.Date(2026, 5, 9, 12, 34, 56, 0, time.UTC)
	rw := frozenAt(ts, 0, "")
	got := rw.Rewrite("CREATE (n:Event {ts: now()})")
	want := "CREATE (n:Event {ts: datetime('2026-05-09T12:34:56Z')})"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRewriter_DatetimeEmptyParens(t *testing.T) {
	ts := time.Date(2026, 5, 9, 12, 34, 56, 0, time.UTC)
	rw := frozenAt(ts, 0, "")
	got := rw.Rewrite("RETURN datetime()")
	want := "RETURN datetime('2026-05-09T12:34:56Z')"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRewriter_DatetimeWithArgsUnchanged(t *testing.T) {
	rw := DefaultRewriter()
	in := "RETURN datetime('2024-01-01T00:00:00Z')"
	if got := rw.Rewrite(in); got != in {
		t.Errorf("datetime(arg) must not be rewritten; got %q", got)
	}
}

func TestRewriter_TimestampReturnsInteger(t *testing.T) {
	ts := time.Date(2026, 5, 9, 12, 34, 56, 0, time.UTC)
	rw := frozenAt(ts, 0, "")
	got := rw.Rewrite("RETURN timestamp()")
	wantMs := ts.UnixMilli()
	want := "RETURN " + itoa(wantMs)
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRewriter_Rand(t *testing.T) {
	rw := frozenAt(time.Now(), 0.5, "")
	got := rw.Rewrite("RETURN rand()")
	if got != "RETURN 0.5" {
		t.Errorf("got %q, want %q", got, "RETURN 0.5")
	}
}

func TestRewriter_RandomAlias(t *testing.T) {
	rw := frozenAt(time.Now(), 0.25, "")
	got := rw.Rewrite("RETURN random()")
	if got != "RETURN 0.25" {
		t.Errorf("got %q, want %q", got, "RETURN 0.25")
	}
}

func TestRewriter_RandomUUID(t *testing.T) {
	rw := frozenAt(time.Now(), 0, "abc-uuid")
	got := rw.Rewrite("CREATE (n {id: randomUUID()})")
	want := "CREATE (n {id: 'abc-uuid'})"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRewriter_CaseInsensitive(t *testing.T) {
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	rw := frozenAt(ts, 0, "")
	got := rw.Rewrite("RETURN NOW(), Datetime(), TIMESTAMP()")
	if !strings.Contains(got, "datetime('2026-05-09T12:00:00Z')") {
		t.Errorf("expected NOW() and Datetime() rewritten; got: %q", got)
	}
	wantMs := itoa(ts.UnixMilli())
	if !strings.Contains(got, wantMs) {
		t.Errorf("expected TIMESTAMP() rewritten to ms-since-epoch %s; got: %q", wantMs, got)
	}
}

func TestRewriter_PreservesSingleQuotedStrings(t *testing.T) {
	rw := DefaultRewriter()
	in := "CREATE (n:Note {body: 'see now() in docs'})"
	if got := rw.Rewrite(in); got != in {
		t.Errorf("must not rewrite inside '...'; got %q", got)
	}
}

func TestRewriter_PreservesDoubleQuotedStrings(t *testing.T) {
	rw := DefaultRewriter()
	in := `CREATE (n:Note {body: "rand() should stay literal"})`
	if got := rw.Rewrite(in); got != in {
		t.Errorf("must not rewrite inside \"...\"; got %q", got)
	}
}

func TestRewriter_PreservesBacktickIdentifiers(t *testing.T) {
	rw := DefaultRewriter()
	in := "MATCH (n) WHERE n.`now()` IS NOT NULL RETURN n"
	if got := rw.Rewrite(in); got != in {
		t.Errorf("must not rewrite inside `...`; got %q", got)
	}
}

func TestRewriter_BareNowNotRewritten(t *testing.T) {
	rw := DefaultRewriter()
	in := "MATCH (n) WHERE n.now > 5 RETURN n"
	if got := rw.Rewrite(in); got != in {
		t.Errorf("bare 'now' identifier must not be rewritten; got %q", got)
	}
}

func TestRewriter_NowNeedsParens(t *testing.T) {
	rw := DefaultRewriter()
	// `now` followed by something other than `()` is a property name, not
	// the function. Leave alone.
	in := "RETURN nowhere"
	if got := rw.Rewrite(in); got != in {
		t.Errorf("'nowhere' must not be split; got %q", got)
	}
}

func TestRewriter_LineCommentSafe(t *testing.T) {
	rw := DefaultRewriter()
	in := "RETURN 1 // call now() later\n"
	if got := rw.Rewrite(in); got != in {
		t.Errorf("must not rewrite inside line comment; got %q", got)
	}
}

func TestRewriter_BlockCommentSafe(t *testing.T) {
	rw := DefaultRewriter()
	in := "/* now() */ RETURN 1"
	if got := rw.Rewrite(in); got != in {
		t.Errorf("must not rewrite inside block comment; got %q", got)
	}
}

func TestRewriter_DeterministicForFrozenClock(t *testing.T) {
	ts := time.Date(2026, 5, 9, 12, 34, 56, 0, time.UTC)
	rw := frozenAt(ts, 0.5, "uuid-x")
	in := "CREATE (n {ts: now(), r: rand(), id: randomUUID()})"
	a := rw.Rewrite(in)
	b := rw.Rewrite(in)
	if a != b {
		t.Errorf("rewriter must be deterministic for frozen inputs:\n a: %q\n b: %q", a, b)
	}
}

func TestRewriter_MultipleSubstitutionsInOneStatement(t *testing.T) {
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	rw := frozenAt(ts, 0.1, "")
	in := "CREATE (n {a: now(), b: now(), c: rand()})"
	got := rw.Rewrite(in)
	if strings.Count(got, "datetime('2026-05-09T12:00:00Z')") != 2 {
		t.Errorf("expected 2 datetime() rewrites; got: %q", got)
	}
	if !strings.Contains(got, "0.1") {
		t.Errorf("expected rand() rewrite; got: %q", got)
	}
}

func TestRewriter_EmptyParenSpaceTolerant(t *testing.T) {
	ts := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	rw := frozenAt(ts, 0, "")
	got := rw.Rewrite("RETURN now (   )")
	want := "RETURN datetime('2026-05-09T12:00:00Z')"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRewriter_DateAndTimeFamily(t *testing.T) {
	ts := time.Date(2026, 5, 9, 12, 34, 56, 0, time.UTC)
	rw := frozenAt(ts, 0, "")

	got := rw.Rewrite("RETURN date()")
	if got != "RETURN date('2026-05-09')" {
		t.Errorf("date(): got %q", got)
	}

	got = rw.Rewrite("RETURN time()")
	if !strings.HasPrefix(got, "RETURN time('12:34:56") {
		t.Errorf("time(): got %q", got)
	}

	got = rw.Rewrite("RETURN localdatetime()")
	if !strings.HasPrefix(got, "RETURN localdatetime('2026-05-09T12:34:56") {
		t.Errorf("localdatetime(): got %q", got)
	}

	got = rw.Rewrite("RETURN localtime()")
	if !strings.HasPrefix(got, "RETURN localtime('12:34:56") {
		t.Errorf("localtime(): got %q", got)
	}
}

func TestRewriter_FormatFloatNoScientific(t *testing.T) {
	if formatFloat(0.0000001) == "" || strings.Contains(formatFloat(0.0000001), "e") {
		t.Errorf("formatFloat must not return scientific notation: %q", formatFloat(0.0000001))
	}
}

func TestRewriter_NilSafe_EmptyInput(t *testing.T) {
	rw := DefaultRewriter()
	if rw.Rewrite("") != "" {
		t.Error("empty input should pass through")
	}
}

func TestRewriter_PreservesEscapedQuotes(t *testing.T) {
	rw := DefaultRewriter()
	in := `CREATE (n {body: 'it\'s now() o\'clock'})`
	if got := rw.Rewrite(in); got != in {
		t.Errorf("escaped quote should keep us inside the string; got %q", got)
	}
}

func TestRewriter_UUIDFormat(t *testing.T) {
	uuid := newUUIDv4()
	// 8-4-4-4-12 hex chars = 36 with dashes
	if len(uuid) != 36 {
		t.Errorf("UUID length expected 36, got %d (%q)", len(uuid), uuid)
	}
	// Version 4: 14th nibble must be '4'
	if uuid[14] != '4' {
		t.Errorf("UUID v4 marker missing: %q", uuid)
	}
	// Variant: 19th nibble must be 8/9/a/b
	switch uuid[19] {
	case '8', '9', 'a', 'b':
	default:
		t.Errorf("UUID variant marker invalid: %q", uuid)
	}
}
