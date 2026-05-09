package api

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/johnjansen/loveliness/pkg/router"
)

// readArrowStream reads an Arrow IPC *stream* (not file) and flattens
// every record batch into a row slice. It is the symmetric helper to
// readArrowFile in arrow_test.go; the distinction matters because
// stream and file formats are not interchangeable.
func readArrowStream(t *testing.T, buf []byte) (*arrow.Schema, [][]any) {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(buf), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("read arrow stream: %v", err)
	}
	defer r.Release()

	out := [][]any{}
	for r.Next() {
		rec := r.RecordBatch()
		for row := int64(0); row < rec.NumRows(); row++ {
			rowOut := make([]any, rec.NumCols())
			for c := int64(0); c < rec.NumCols(); c++ {
				col := rec.Column(int(c))
				if col.IsNull(int(row)) {
					rowOut[c] = nil
					continue
				}
				rowOut[c] = col.GetOneForMarshal(int(row))
			}
			out = append(out, rowOut)
		}
	}
	if err := r.Err(); err != nil {
		t.Fatalf("stream reader err: %v", err)
	}
	return r.Schema(), out
}

func TestArrowStream_RoundTrip(t *testing.T) {
	res := &router.Result{
		Columns: []string{"name", "age", "score", "active"},
		Rows: []map[string]any{
			{"name": "Alice", "age": int64(30), "score": 1.5, "active": true},
			{"name": "Bob", "age": int64(25), "score": 2.0, "active": false},
		},
	}
	buf, err := encodeResultAsArrowStream(res)
	if err != nil {
		t.Fatalf("encode stream: %v", err)
	}
	if len(buf) == 0 {
		t.Fatal("encoded buffer is empty")
	}

	schema, rows := readArrowStream(t, buf)
	if schema.NumFields() != 4 {
		t.Errorf("expected 4 fields, got %d", schema.NumFields())
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][0] != "Alice" {
		t.Errorf("row[0][name]=%v, want Alice", rows[0][0])
	}
}

// The stream and file formats are NOT interchangeable. If the
// dispatch ever regresses to writing the file format under the
// stream content-type, this assertion will catch it: a file payload
// always starts with the magic "ARROW1" header, a stream never
// does.
func TestArrowStream_NotFileFormat(t *testing.T) {
	res := &router.Result{Columns: []string{"x"}, Rows: []map[string]any{{"x": int64(1)}}}
	buf, err := encodeResultAsArrowStream(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if bytes.HasPrefix(buf, []byte("ARROW1")) {
		t.Fatal("stream encoder leaked the file-format magic header")
	}
	// Conversely, the file encoder MUST start with that magic — proves
	// the two encoders produce genuinely different bytes.
	fileBuf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode file: %v", err)
	}
	if !bytes.HasPrefix(fileBuf, []byte("ARROW1")) {
		t.Fatal("file encoder did not produce the ARROW1 magic header")
	}
}

func TestArrowStream_EmptyResultStillValid(t *testing.T) {
	res := &router.Result{Columns: []string{"x"}, Rows: nil}
	buf, err := encodeResultAsArrowStream(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	schema, rows := readArrowStream(t, buf)
	if schema.NumFields() != 1 {
		t.Errorf("expected 1 field, got %d", schema.NumFields())
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestArrowStream_MetadataPreserved(t *testing.T) {
	res := &router.Result{
		Columns: []string{"x"},
		Rows:    []map[string]any{{"x": "ok"}},
		Partial: true,
		Errors:  []router.ShardError{{ShardID: 1, Error: "boom"}},
	}
	buf, err := encodeResultAsArrowStream(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	schema, _ := readArrowStream(t, buf)
	md := schema.Metadata()
	if got, ok := md.GetValue("loveliness.partial"); !ok || got != "true" {
		t.Errorf("loveliness.partial metadata missing/wrong: %q (ok=%v)", got, ok)
	}
	if got, ok := md.GetValue("loveliness.errors"); !ok || got == "" {
		t.Errorf("loveliness.errors metadata missing/empty: %q (ok=%v)", got, ok)
	}
}
