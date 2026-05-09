package router

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/johnjansen/loveliness/pkg/shard"
)

func TestContainsAllShortest(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "canonical positive",
			query: "MATCH (a:Person)-[r:KNOWS* ALL SHORTEST 1..6]->(b:Person) RETURN length(r)",
			want:  true,
		},
		{
			name:  "lowercase positive",
			query: "match (a)-[r* all shortest 1..3]->(b) return r",
			want:  true,
		},
		{
			name:  "mixed case + tab",
			query: "MATCH (a)-[r*\tAll\tShortest 1..3]->(b) RETURN r",
			want:  true,
		},
		{
			name:  "newline between keywords",
			query: "MATCH (a)-[r* ALL\n      SHORTEST 1..3]->(b) RETURN r",
			want:  true,
		},
		{
			name:  "SHORTEST without ALL is safe",
			query: "MATCH (a)-[r* SHORTEST 1..6]->(b) RETURN r",
			want:  false,
		},
		{
			name:  "string literal containing keywords does not trigger",
			query: "MATCH (n {comment: 'use ALL SHORTEST instead'}) RETURN n",
			want:  false,
		},
		{
			name:  "double-quoted literal does not trigger",
			query: `MATCH (n {note: "ALL SHORTEST"}) RETURN n`,
			want:  false,
		},
		{
			name:  "backtick identifier does not trigger",
			query: "MATCH (n:`ALL SHORTEST table`) RETURN n",
			want:  false,
		},
		{
			name:  "line comment masks keywords",
			query: "MATCH (n) RETURN n // ALL SHORTEST commentary\n",
			want:  false,
		},
		{
			name:  "block comment masks keywords",
			query: "MATCH (n) /* ALL SHORTEST in a comment */ RETURN n",
			want:  false,
		},
		{
			name:  "OVERALL SHORTEST should not trigger (word boundary)",
			query: "MATCH (n) RETURN n.overall_shortest",
			want:  false,
		},
		{
			name:  "ALLSHORTEST run together does not trigger",
			query: "RETURN 'ALLSHORTEST'",
			want:  false,
		},
		{
			name:  "escaped quote inside literal still treated as literal",
			query: `MATCH (n {note: 'it\'s ALL SHORTEST'}) RETURN n`,
			want:  false,
		},
		{
			name:  "ALL SHORTEST after comment is detected",
			query: "// safe comment\nMATCH (a)-[r* ALL SHORTEST 1..3]->(b) RETURN r",
			want:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := containsAllShortest(tc.query)
			if got != tc.want {
				t.Fatalf("containsAllShortest(%q) = %v, want %v", tc.query, got, tc.want)
			}
		})
	}
}

func TestRejectIfUnsafeRespectsGate(t *testing.T) {
	t.Cleanup(func() { SetAllowAllShortestUnsafe(false) })

	q := "MATCH (a)-[r* ALL SHORTEST 1..3]->(b) RETURN r"

	SetAllowAllShortestUnsafe(false)
	if err := rejectIfUnsafe(q); err == nil {
		t.Fatal("expected rejection when gate is closed, got nil")
	} else if err.Code != "UNSAFE_QUERY" {
		t.Fatalf("unexpected error code: %s", err.Code)
	}

	SetAllowAllShortestUnsafe(true)
	if err := rejectIfUnsafe(q); err != nil {
		t.Fatalf("expected pass when gate is open, got %v", err)
	}
}

func TestRouterExecuteRejectsAllShortest(t *testing.T) {
	t.Cleanup(func() { SetAllowAllShortestUnsafe(false) })
	SetAllowAllShortestUnsafe(false)

	r := NewRouter([]*shard.Shard{}, 1*time.Second)

	_, err := r.Execute(context.Background(),
		"MATCH (a:Person)-[r* ALL SHORTEST 1..6]->(b:Person) RETURN length(r)")
	if err == nil {
		t.Fatal("expected Router.Execute to reject ALL SHORTEST, got nil")
	}
	qe, ok := err.(*QueryError)
	if !ok {
		t.Fatalf("expected *QueryError, got %T: %v", err, err)
	}
	if qe.Code != "UNSAFE_QUERY" {
		t.Fatalf("unexpected error code: %s", qe.Code)
	}
	if !strings.Contains(qe.Message, "SHORTEST") {
		t.Fatalf("error message should mention SHORTEST: %s", qe.Message)
	}
}

func TestStripLiteralsAndCommentsPreservesLength(t *testing.T) {
	in := `MATCH (n {note: 'ALL SHORTEST'}) /* and ALL SHORTEST */ RETURN n // line\n`
	out := stripLiteralsAndComments(in)
	if len(out) != len(in) {
		t.Fatalf("length changed: %d -> %d", len(in), len(out))
	}
}
