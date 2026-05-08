package mcp

import "testing"

func TestFirstKeyword(t *testing.T) {
	tests := map[string]string{
		"MATCH (n) RETURN n":                    "MATCH",
		"  match (n)":                           "MATCH",
		"/* comment */ RETURN 1":                "RETURN",
		"// line comment\nCREATE (n)":           "CREATE",
		"/* a */ /* b */ \n DELETE n":           "DELETE",
		"\t\n  DETACH DELETE n":                 "DETACH",
		"":                                      "",
		"   ":                                   "",
	}
	for q, want := range tests {
		t.Run(q, func(t *testing.T) {
			got := firstKeyword(q)
			if got != want {
				t.Errorf("firstKeyword(%q) = %q, want %q", q, got, want)
			}
		})
	}
}

func TestIsWrite(t *testing.T) {
	writes := []string{
		"CREATE (n)",
		"MERGE (n)",
		"  SET n.x = 1",
		"DELETE n",
		"DETACH DELETE n",
		"REMOVE n.x",
		"DROP TABLE x",
		"LOAD CSV",
		"COPY Person FROM 'x.csv'",
		"ALTER TABLE Person ADD COLUMN y INT64",
		"/* writer */ CREATE (n)",
	}
	for _, q := range writes {
		if !isWrite(q) {
			t.Errorf("isWrite(%q) = false, want true", q)
		}
	}
	reads := []string{
		"MATCH (n) RETURN n",
		"RETURN 1",
		"CALL show_tables()",
		"CALL table_info('Person')",
		"// note\nMATCH (n) RETURN n",
	}
	for _, q := range reads {
		if isWrite(q) {
			t.Errorf("isWrite(%q) = true, want false", q)
		}
	}

	mutatingCalls := []string{
		"CALL drop_table('Person')",
		"CALL DROP_TABLE('Person')",
		"  call  export_database('/tmp/x')",
		"/* x */ CALL create_node_table('T', 'id INT64, PRIMARY KEY (id)')",
		"CALL alter_table_rename('A', 'B')",
	}
	for _, q := range mutatingCalls {
		if !isWrite(q) {
			t.Errorf("isWrite(%q) = false, want true (mutating CALL)", q)
		}
	}
}

func TestCallTarget(t *testing.T) {
	tests := map[string]string{
		"CALL show_tables()":             "show_tables",
		"  call drop_table('x')":         "drop_table",
		"/* hi */ CALL  table_info ('a')": "table_info",
		"MATCH (n)":                       "",
		"":                                "",
	}
	for q, want := range tests {
		got := callTarget(q)
		if got != want {
			t.Errorf("callTarget(%q) = %q, want %q", q, got, want)
		}
	}
}
