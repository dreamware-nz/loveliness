package api

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/johnjansen/loveliness/pkg/router"
)

// fakeMapItem matches the duck-typed shape the reflection-based
// detector looks for (`Key any, Value any`). Keeps the encoder
// decoupled from the concrete `lbug.MapItem` type — these tests
// could exercise the detector against any binding that produced a
// compatible struct.
type fakeMapItem struct {
	Key   any
	Value any
}

func TestAsMapEntries_PicksUpFakeShape(t *testing.T) {
	items := []fakeMapItem{
		{Key: "a", Value: int64(1)},
		{Key: "b", Value: int64(2)},
	}
	got, ok := asMapEntries(items)
	if !ok {
		t.Fatal("expected map shape to be detected")
	}
	if len(got) != 2 || got[0].key != "a" || got[1].key != "b" {
		t.Errorf("entries = %+v, want [{a 1} {b 2}]", got)
	}
}

func TestAsMapEntries_RejectsPlainListOfScalars(t *testing.T) {
	if _, ok := asMapEntries([]any{int64(1), int64(2)}); ok {
		t.Error("plain []any of ints must not be detected as a map")
	}
}

func TestAsMapEntries_RejectsEmptySlice(t *testing.T) {
	// Empty slice is ambiguous. We keep list-detection's existing
	// behavior on `[]any{}` and refuse to claim it as a map.
	if _, ok := asMapEntries([]fakeMapItem{}); ok {
		t.Error("empty slice must not be detected as a map (let the list path handle it)")
	}
}

func TestArrow_MapStringToInt64Schema(t *testing.T) {
	res := &router.Result{
		Columns: []string{"props"},
		Rows: []map[string]any{
			{"props": []fakeMapItem{
				{Key: "x", Value: int64(1)},
				{Key: "y", Value: int64(2)},
			}},
			{"props": []fakeMapItem{
				{Key: "z", Value: int64(3)},
			}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	field := rec.Schema().Field(0)
	if field.Type.ID() != arrow.MAP {
		t.Fatalf("props must be MAP, got %v", field.Type)
	}
	mt := field.Type.(*arrow.MapType)
	if mt.KeyType().ID() != arrow.STRING {
		t.Errorf("key type = %v, want STRING", mt.KeyType())
	}
	if mt.ItemType().ID() != arrow.INT64 {
		t.Errorf("value type = %v, want INT64", mt.ItemType())
	}
}

func TestArrow_MapStringToInt64ValuesRoundTrip(t *testing.T) {
	res := &router.Result{
		Columns: []string{"props"},
		Rows: []map[string]any{
			{"props": []fakeMapItem{
				{Key: "x", Value: int64(1)},
				{Key: "y", Value: int64(2)},
			}},
			{"props": []fakeMapItem{
				{Key: "z", Value: int64(3)},
			}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	mp := rec.Column(0).(*array.Map)
	keys := mp.Keys().(*array.String)
	values := mp.Items().(*array.Int64)

	row0Start, row0End := mp.ValueOffsets(0)
	if row0End-row0Start != 2 {
		t.Errorf("row 0 entry count = %d, want 2", row0End-row0Start)
	}
	gotKeys := []string{keys.Value(int(row0Start)), keys.Value(int(row0Start) + 1)}
	if gotKeys[0] != "x" || gotKeys[1] != "y" {
		t.Errorf("row 0 keys = %v, want [x y]", gotKeys)
	}
	if values.Value(int(row0Start)) != 1 || values.Value(int(row0Start)+1) != 2 {
		t.Errorf("row 0 values = (%d, %d), want (1, 2)",
			values.Value(int(row0Start)), values.Value(int(row0Start)+1))
	}

	row1Start, row1End := mp.ValueOffsets(1)
	if row1End-row1Start != 1 {
		t.Errorf("row 1 entry count = %d, want 1", row1End-row1Start)
	}
	if keys.Value(int(row1Start)) != "z" {
		t.Errorf("row 1 key = %q, want z", keys.Value(int(row1Start)))
	}
	if values.Value(int(row1Start)) != 3 {
		t.Errorf("row 1 value = %d, want 3", values.Value(int(row1Start)))
	}
}

func TestArrow_MapStringToStringSchema(t *testing.T) {
	res := &router.Result{
		Columns: []string{"props"},
		Rows: []map[string]any{
			{"props": []fakeMapItem{
				{Key: "name", Value: "Alice"},
				{Key: "city", Value: "NYC"},
			}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	mt := rec.Schema().Field(0).Type.(*arrow.MapType)
	if mt.KeyType().ID() != arrow.STRING {
		t.Errorf("key type = %v, want STRING", mt.KeyType())
	}
	if mt.ItemType().ID() != arrow.STRING {
		t.Errorf("value type = %v, want STRING", mt.ItemType())
	}
}

func TestArrow_MapValuePromotesIntToFloat(t *testing.T) {
	// Mixing int64 and float64 across map values follows the same
	// promotion rule used for scalar columns and list elements.
	res := &router.Result{
		Columns: []string{"props"},
		Rows: []map[string]any{
			{"props": []fakeMapItem{
				{Key: "a", Value: int64(1)},
				{Key: "b", Value: 2.5},
			}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	mt := rec.Schema().Field(0).Type.(*arrow.MapType)
	if mt.ItemType().ID() != arrow.FLOAT64 {
		t.Errorf("value type = %v, want FLOAT64", mt.ItemType())
	}
}

func TestArrow_MapHeterogeneousValuesFallBackToJSON(t *testing.T) {
	// String + int values can't be expressed as map<utf8, V>, so the
	// column degrades to JSON like any other heterogeneous payload.
	res := &router.Result{
		Columns: []string{"props"},
		Rows: []map[string]any{
			{"props": []fakeMapItem{
				{Key: "a", Value: "hello"},
				{Key: "b", Value: int64(1)},
			}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	if got := rec.Schema().Field(0).Type.ID(); got != arrow.STRING {
		t.Errorf("heterogeneous map values must fall back to STRING (JSON), got %v", got)
	}
}

func TestArrow_MapNonStringKeyFallsBackToJSON(t *testing.T) {
	// Cypher MAP keys are always strings on the wire. A non-string
	// key (which can happen in tests or from other bindings) is the
	// signal that the value isn't a real Cypher MAP and must be
	// expressed as JSON.
	res := &router.Result{
		Columns: []string{"weird"},
		Rows: []map[string]any{
			{"weird": []fakeMapItem{
				{Key: int64(1), Value: "a"},
			}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	if got := rec.Schema().Field(0).Type.ID(); got != arrow.STRING {
		t.Errorf("non-string map key must force JSON fallback, got %v", got)
	}
}

func TestArrow_MapAndScalarInSameColumnFallsBackToJSON(t *testing.T) {
	// Map+scalar across rows can't be represented without union types.
	res := &router.Result{
		Columns: []string{"x"},
		Rows: []map[string]any{
			{"x": []fakeMapItem{{Key: "a", Value: int64(1)}}},
			{"x": int64(99)},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	if got := rec.Schema().Field(0).Type.ID(); got != arrow.STRING {
		t.Errorf("map+scalar column must fall back to STRING (JSON), got %v", got)
	}
}

func TestArrow_MapAndListInSameColumnFallsBackToJSON(t *testing.T) {
	// Map+list across rows is also unrepresentable.
	res := &router.Result{
		Columns: []string{"x"},
		Rows: []map[string]any{
			{"x": []fakeMapItem{{Key: "a", Value: int64(1)}}},
			{"x": []any{int64(1), int64(2)}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	if got := rec.Schema().Field(0).Type.ID(); got != arrow.STRING {
		t.Errorf("map+list column must fall back to STRING (JSON), got %v", got)
	}
}

func TestArrow_MapNullRowProducesNullMask(t *testing.T) {
	res := &router.Result{
		Columns: []string{"props"},
		Rows: []map[string]any{
			{"props": []fakeMapItem{{Key: "a", Value: int64(1)}}},
			{"props": nil},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	mp := rec.Column(0).(*array.Map)
	if mp.IsNull(0) {
		t.Errorf("row 0 should not be null")
	}
	if !mp.IsNull(1) {
		t.Errorf("row 1 should be null")
	}
}

func TestArrow_MapPlainListNotMisDetected(t *testing.T) {
	// A column carrying real Cypher LIST values must keep landing as
	// list<T>, not map<utf8, V> — guards against the detector greedily
	// claiming any slice.
	res := &router.Result{
		Columns: []string{"xs"},
		Rows: []map[string]any{
			{"xs": []any{int64(1), int64(2), int64(3)}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	r, err := ipc.NewFileReader(bytes.NewReader(buf), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	defer r.Close()
	if got := r.Schema().Field(0).Type.ID(); got != arrow.LIST {
		t.Errorf("plain list value must stay as LIST, got %v", got)
	}
}
