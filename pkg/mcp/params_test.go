package mcp

import "testing"

func TestInlineParams(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		params  map[string]any
		want    string
		wantErr bool
	}{
		{
			name:   "no params",
			query:  "MATCH (n) RETURN n",
			params: nil,
			want:   "MATCH (n) RETURN n",
		},
		{
			name:   "string substitution",
			query:  "MATCH (p:Person {name: $name}) RETURN p",
			params: map[string]any{"name": "Alice"},
			want:   "MATCH (p:Person {name: 'Alice'}) RETURN p",
		},
		{
			name:   "string escapes single quote",
			query:  "RETURN $s",
			params: map[string]any{"s": "O'Brien"},
			want:   "RETURN 'O\\'Brien'",
		},
		{
			name:   "int and bool",
			query:  "RETURN $n, $b",
			params: map[string]any{"n": 42, "b": true},
			want:   "RETURN 42, true",
		},
		{
			name:   "null",
			query:  "RETURN $x",
			params: map[string]any{"x": nil},
			want:   "RETURN NULL",
		},
		{
			name:   "leave placeholders inside strings alone",
			query:  "RETURN 'this is $not_a_param'",
			params: map[string]any{"x": "y"},
			want:   "RETURN 'this is $not_a_param'",
		},
		{
			name:    "missing param errors",
			query:   "RETURN $missing",
			params:  map[string]any{},
			wantErr: true,
		},
		{
			name:   "multi-char identifier",
			query:  "RETURN $a1_b",
			params: map[string]any{"a1_b": 3.14},
			want:   "RETURN 3.14",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := inlineParams(tc.query, tc.params)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}
