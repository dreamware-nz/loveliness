package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/johnjansen/loveliness/pkg/router"
)

// The mid-stream error trailer is the contract documented in
// docs/arrow-mapping.md under "Mid-stream errors". Three flat
// metadata keys ride alongside the structured `loveliness.errors`
// JSON array so a client that only reads schema metadata can detect
// failure with one lookup. The structured array remains the source
// of truth — these tests pin both surfaces so a client written
// against either can't quietly miss errors.

func TestArrowTrailer_NoErrorsLeavesTrailerKeysAbsent(t *testing.T) {
	// A clean result must NOT carry trailer keys — their presence is
	// itself the "errors happened" signal. Emitting them with empty
	// values would force every client to disambiguate.
	res := &router.Result{
		Columns: []string{"x"},
		Rows:    []map[string]any{{"x": "ok"}},
	}

	for _, tc := range []struct {
		name   string
		encode func(*router.Result) ([]byte, error)
		read   func(*testing.T, []byte) (interface{ GetValue(string) (string, bool) }, error)
	}{
		{
			name:   "stream",
			encode: encodeResultAsArrowStream,
			read: func(t *testing.T, buf []byte) (interface{ GetValue(string) (string, bool) }, error) {
				schema, _ := readArrowStream(t, buf)
				md := schema.Metadata()
				return &md, nil
			},
		},
		{
			name:   "file",
			encode: encodeResultAsArrow,
			read: func(t *testing.T, buf []byte) (interface{ GetValue(string) (string, bool) }, error) {
				schema, _ := readArrowFile(t, buf)
				md := schema.Metadata()
				return &md, nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := tc.encode(res)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			md, err := tc.read(t, buf)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			for _, key := range []string{
				"loveliness.error",
				"loveliness.error.code",
				"loveliness.error.message",
				"loveliness.errors",
			} {
				if _, ok := md.GetValue(key); ok {
					t.Errorf("clean result must not carry %s metadata", key)
				}
			}
		})
	}
}

func TestArrowTrailer_SingleShardError(t *testing.T) {
	res := &router.Result{
		Columns: []string{"x"},
		Rows:    []map[string]any{{"x": "ok"}},
		Partial: true,
		Errors: []router.ShardError{
			{ShardID: 2, Error: "shard timeout"},
		},
	}

	for _, tc := range []struct {
		name   string
		encode func(*router.Result) ([]byte, error)
	}{
		{"stream", encodeResultAsArrowStream},
		{"file", encodeResultAsArrow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, err := tc.encode(res)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			var md interface {
				GetValue(string) (string, bool)
			}
			if tc.name == "stream" {
				schema, _ := readArrowStream(t, buf)
				m := schema.Metadata()
				md = &m
			} else {
				schema, _ := readArrowFile(t, buf)
				m := schema.Metadata()
				md = &m
			}

			if got, ok := md.GetValue("loveliness.error"); !ok || got != "true" {
				t.Errorf("loveliness.error = %q (ok=%v), want true", got, ok)
			}
			if got, ok := md.GetValue("loveliness.error.code"); !ok || got != "SHARD_ERROR" {
				t.Errorf("loveliness.error.code = %q (ok=%v), want SHARD_ERROR", got, ok)
			}
			got, ok := md.GetValue("loveliness.error.message")
			if !ok {
				t.Fatal("loveliness.error.message missing")
			}
			if !strings.Contains(got, "shard 2") || !strings.Contains(got, "shard timeout") {
				t.Errorf("loveliness.error.message = %q, want it to mention shard 2 and the underlying error", got)
			}

			// Structured list still rides along — clients that prefer
			// it over the trailer keys must keep working.
			raw, ok := md.GetValue("loveliness.errors")
			if !ok {
				t.Fatal("loveliness.errors missing alongside trailer")
			}
			var decoded []router.ShardError
			if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
				t.Fatalf("loveliness.errors not parseable: %v", err)
			}
			if len(decoded) != 1 || decoded[0].ShardID != 2 {
				t.Errorf("decoded loveliness.errors = %+v, want [{ShardID:2, ...}]", decoded)
			}
		})
	}
}

func TestArrowTrailer_MultipleShardErrors(t *testing.T) {
	res := &router.Result{
		Columns: []string{"x"},
		Rows:    []map[string]any{{"x": "ok"}},
		Partial: true,
		Errors: []router.ShardError{
			{ShardID: 0, Error: "first boom"},
			{ShardID: 3, Error: "second boom"},
			{ShardID: 5, Error: "third boom"},
		},
	}

	buf, err := encodeResultAsArrowStream(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	schema, _ := readArrowStream(t, buf)
	md := schema.Metadata()

	if got, ok := md.GetValue("loveliness.error"); !ok || got != "true" {
		t.Errorf("loveliness.error = %q (ok=%v), want true", got, ok)
	}
	if got, ok := md.GetValue("loveliness.error.code"); !ok || got != "SHARD_ERRORS" {
		t.Errorf("loveliness.error.code = %q (ok=%v), want SHARD_ERRORS for >1 errors", got, ok)
	}
	got, ok := md.GetValue("loveliness.error.message")
	if !ok {
		t.Fatal("loveliness.error.message missing")
	}
	if !strings.Contains(got, "3 shards") {
		t.Errorf("loveliness.error.message = %q, want it to mention the shard count", got)
	}
	if !strings.Contains(got, "first boom") {
		t.Errorf("loveliness.error.message = %q, want it to surface the first shard error", got)
	}
}

func TestErrorTrailer_EmptyInput(t *testing.T) {
	// Defensive: errorTrailer with no errors must return empty
	// strings so callers that forget the len-check don't accidentally
	// emit a trailer.
	code, msg := errorTrailer(nil)
	if code != "" || msg != "" {
		t.Errorf("errorTrailer(nil) = (%q, %q), want empty", code, msg)
	}
	code, msg = errorTrailer([]router.ShardError{})
	if code != "" || msg != "" {
		t.Errorf("errorTrailer([]) = (%q, %q), want empty", code, msg)
	}
}
