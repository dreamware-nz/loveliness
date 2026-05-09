package api

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/johnjansen/loveliness/pkg/router"
)

func readArrowFile(t *testing.T, buf []byte) (*arrow.Schema, [][]any) {
	t.Helper()
	r, err := ipc.NewFileReader(bytes.NewReader(buf), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("read arrow file: %v", err)
	}
	defer r.Close()

	out := [][]any{}
	for i := 0; i < r.NumRecords(); i++ {
		rec, err := r.RecordBatch(i)
		if err != nil {
			t.Fatalf("read record %d: %v", i, err)
		}
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
	return r.Schema(), out
}

func TestArrow_ScalarRoundTrip(t *testing.T) {
	res := &router.Result{
		Columns: []string{"name", "age", "score", "active"},
		Rows: []map[string]any{
			{"name": "Alice", "age": int64(30), "score": 1.5, "active": true},
			{"name": "Bob", "age": int64(25), "score": 2.0, "active": false},
			{"name": "Charlie", "age": nil, "score": 3.5, "active": true},
		},
	}

	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(buf) == 0 {
		t.Fatal("encoded buffer is empty")
	}

	schema, rows := readArrowFile(t, buf)
	if schema.NumFields() != 4 {
		t.Errorf("expected 4 fields, got %d", schema.NumFields())
	}
	if got := schema.Field(0).Type.ID(); got != arrow.STRING {
		t.Errorf("name should be utf8, got %v", got)
	}
	if got := schema.Field(1).Type.ID(); got != arrow.INT64 {
		t.Errorf("age should be int64, got %v", got)
	}
	if got := schema.Field(2).Type.ID(); got != arrow.FLOAT64 {
		t.Errorf("score should be float64, got %v", got)
	}
	if got := schema.Field(3).Type.ID(); got != arrow.BOOL {
		t.Errorf("active should be bool, got %v", got)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[2][1] != nil {
		t.Errorf("Charlie's age must be null, got %v", rows[2][1])
	}
	if rows[0][0] != "Alice" {
		t.Errorf("expected first name=Alice, got %v", rows[0][0])
	}
}

func TestArrow_MetadataRecordsErrorsAndPartial(t *testing.T) {
	res := &router.Result{
		Columns: []string{"x"},
		Rows:    []map[string]any{{"x": "ok"}},
		Partial: true,
		Errors:  []router.ShardError{{ShardID: 1, Error: "boom"}},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	schema, _ := readArrowFile(t, buf)
	md := schema.Metadata()
	if got, ok := md.GetValue("loveliness.partial"); !ok || got != "true" {
		t.Errorf("loveliness.partial metadata missing/wrong: %q (ok=%v)", got, ok)
	}
	if got, ok := md.GetValue("loveliness.errors"); !ok || got == "" {
		t.Errorf("loveliness.errors metadata missing/empty: %q (ok=%v)", got, ok)
	}
}

func TestArrow_NumericPromotionToFloat(t *testing.T) {
	res := &router.Result{
		Columns: []string{"v"},
		Rows: []map[string]any{
			{"v": int64(1)},
			{"v": 2.5},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	schema, rows := readArrowFile(t, buf)
	if got := schema.Field(0).Type.ID(); got != arrow.FLOAT64 {
		t.Errorf("mixed int/float must promote to float64, got %v", got)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestArrow_ComplexValueFallsBackToJSON(t *testing.T) {
	res := &router.Result{
		Columns: []string{"v"},
		Rows: []map[string]any{
			{"v": map[string]any{"nested": int64(1)}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	schema, rows := readArrowFile(t, buf)
	if got := schema.Field(0).Type.ID(); got != arrow.STRING {
		t.Errorf("complex value must fall back to utf8 JSON, got %v", got)
	}
	if len(rows) != 1 || rows[0][0] == nil {
		t.Errorf("expected one non-null JSON-encoded row, got %v", rows)
	}
}

func TestArrow_EmptyResultStillValid(t *testing.T) {
	res := &router.Result{Columns: []string{"x"}, Rows: nil}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	schema, rows := readArrowFile(t, buf)
	if schema.NumFields() != 1 {
		t.Errorf("expected 1 field, got %d", schema.NumFields())
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}
