package mcp

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBuildCreateNodeTableDDL(t *testing.T) {
	cases := []struct {
		name string
		in   CreateNodeTableInput
		want string
		err  string
	}{
		{
			name: "happy path",
			in: CreateNodeTableInput{
				Table: "Person",
				Properties: []PropertyInput{
					{Name: "name", Type: "STRING", PrimaryKey: true},
					{Name: "age", Type: "INT64"},
				},
			},
			want: "CREATE NODE TABLE Person(name STRING PRIMARY KEY, age INT64)",
		},
		{
			name: "rejects bad table name",
			in: CreateNodeTableInput{
				Table: "Person; DROP TABLE Other",
				Properties: []PropertyInput{
					{Name: "name", Type: "STRING", PrimaryKey: true},
				},
			},
			err: "invalid table name",
		},
		{
			name: "rejects bad property name",
			in: CreateNodeTableInput{
				Table: "Person",
				Properties: []PropertyInput{
					{Name: "name STRING; DROP", Type: "STRING", PrimaryKey: true},
				},
			},
			err: "invalid property name",
		},
		{
			name: "rejects type with quote injection",
			in: CreateNodeTableInput{
				Table: "Person",
				Properties: []PropertyInput{
					{Name: "name", Type: "STRING'); DROP --", PrimaryKey: true},
				},
			},
			err: "invalid property type",
		},
		{
			name: "requires exactly one primary key",
			in: CreateNodeTableInput{
				Table: "Person",
				Properties: []PropertyInput{
					{Name: "name", Type: "STRING"},
				},
			},
			err: "exactly one primary key",
		},
		{
			name: "rejects multiple primary keys",
			in: CreateNodeTableInput{
				Table: "Person",
				Properties: []PropertyInput{
					{Name: "id", Type: "STRING", PrimaryKey: true},
					{Name: "alt", Type: "STRING", PrimaryKey: true},
				},
			},
			err: "exactly one primary key",
		},
		{
			name: "requires at least one property",
			in:   CreateNodeTableInput{Table: "Person"},
			err:  "at least one property",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildCreateNodeTableDDL(tc.in)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("want error containing %q, got %v", tc.err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestBuildCreateEdgeTableDDL(t *testing.T) {
	cases := []struct {
		name string
		in   CreateEdgeTableInput
		want string
		err  string
	}{
		{
			name: "no properties",
			in:   CreateEdgeTableInput{Table: "KNOWS", From: "Person", To: "Person"},
			want: "CREATE REL TABLE KNOWS(FROM Person TO Person)",
		},
		{
			name: "with properties",
			in: CreateEdgeTableInput{
				Table: "KNOWS", From: "Person", To: "Person",
				Properties: []PropertyInput{{Name: "since", Type: "DATE"}},
			},
			want: "CREATE REL TABLE KNOWS(FROM Person TO Person, since DATE)",
		},
		{
			name: "rejects edge primary key",
			in: CreateEdgeTableInput{
				Table: "KNOWS", From: "Person", To: "Person",
				Properties: []PropertyInput{{Name: "since", Type: "DATE", PrimaryKey: true}},
			},
			err: "cannot have primary key",
		},
		{
			name: "rejects bad from",
			in:   CreateEdgeTableInput{Table: "KNOWS", From: "Person; --", To: "Person"},
			err:  "invalid from table",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildCreateEdgeTableDDL(tc.in)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("want error containing %q, got %v", tc.err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestBuildDropTableDDL(t *testing.T) {
	got, err := buildDropTableDDL("Person")
	if err != nil || got != "DROP TABLE Person" {
		t.Fatalf("got %q err %v", got, err)
	}
	if _, err := buildDropTableDDL("Person; DROP"); err == nil {
		t.Fatal("expected error for bad ident")
	}
}

// TestDDLToolsInvalidateCache verifies that a successful DDL call busts
// the schema cache so the next schema() refetches.
func TestDDLToolsInvalidateCache(t *testing.T) {
	var ddlCalls atomic.Int32
	var schemaCalls atomic.Int32
	srv := fakeBackend(t, map[string]http.HandlerFunc{
		"/cypher": func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			s := string(body)
			switch {
			case strings.Contains(s, "CREATE NODE TABLE"):
				ddlCalls.Add(1)
				writeJSON(w, map[string]any{"columns": []string{}, "rows": []map[string]any{}})
			case strings.Contains(s, "show_tables"):
				schemaCalls.Add(1)
				writeJSON(w, map[string]any{"columns": []string{"name", "type"}, "rows": []map[string]any{}})
			default:
				http.Error(w, "unexpected: "+s, 400)
			}
		},
	})
	t.Cleanup(srv.Close)

	c := NewClient(srv.URL, "", 2*time.Second)
	cache := newSchemaCache(30 * time.Second)

	// Prime the cache.
	if _, err := cache.get(context.Background(), c); err != nil {
		t.Fatalf("prime: %v", err)
	}
	if got := schemaCalls.Load(); got != 1 {
		t.Fatalf("expected 1 schema fetch, got %d", got)
	}

	// Run DDL directly via the build path then invalidate as the tool would.
	ddl, err := buildCreateNodeTableDDL(CreateNodeTableInput{
		Table: "Foo",
		Properties: []PropertyInput{{Name: "id", Type: "STRING", PrimaryKey: true}},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := c.Cypher(context.Background(), ddl, nil); err != nil {
		t.Fatalf("cypher: %v", err)
	}
	if got := ddlCalls.Load(); got != 1 {
		t.Fatalf("expected 1 DDL call, got %d", got)
	}
	cache.invalidate()

	// Next get() must re-fetch.
	if _, err := cache.get(context.Background(), c); err != nil {
		t.Fatalf("post-invalidate fetch: %v", err)
	}
	if got := schemaCalls.Load(); got != 2 {
		t.Fatalf("expected 2 schema fetches after invalidate, got %d", got)
	}
}
