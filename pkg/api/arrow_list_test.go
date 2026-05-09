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

// readArrow loads an Arrow file payload back into a record batch so the
// list tests can poke at the typed columns directly. Wrapping the
// open/close dance keeps each test focused on the assertion.
func readArrow(t *testing.T, buf []byte) (arrow.RecordBatch, func()) {
	t.Helper()
	r, err := ipc.NewFileReader(bytes.NewReader(buf), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("open arrow: %v", err)
	}
	rec, err := r.RecordBatch(0)
	if err != nil {
		_ = r.Close()
		t.Fatalf("record: %v", err)
	}
	return rec, func() { _ = r.Close() }
}

func TestArrow_ListInt64ColumnSchema(t *testing.T) {
	res := &router.Result{
		Columns: []string{"xs"},
		Rows: []map[string]any{
			{"xs": []any{int64(1), int64(2), int64(3)}},
			{"xs": []any{int64(4)}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	field := rec.Schema().Field(0)
	if field.Type.ID() != arrow.LIST {
		t.Fatalf("xs must be LIST, got %v", field.Type)
	}
	lt := field.Type.(*arrow.ListType)
	if lt.Elem().ID() != arrow.INT64 {
		t.Errorf("element type = %v, want INT64", lt.Elem())
	}
}

func TestArrow_ListInt64ValuesRoundTrip(t *testing.T) {
	res := &router.Result{
		Columns: []string{"xs"},
		Rows: []map[string]any{
			{"xs": []any{int64(1), int64(2), int64(3)}},
			{"xs": []any{int64(4)}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	lst := rec.Column(0).(*array.List)
	values := lst.ListValues().(*array.Int64)

	row0Start, row0End := lst.ValueOffsets(0)
	if row0End-row0Start != 3 {
		t.Errorf("row 0 length = %d, want 3", row0End-row0Start)
	}
	for i, want := range []int64{1, 2, 3} {
		if got := values.Value(int(row0Start) + i); got != want {
			t.Errorf("row 0 elem %d = %d, want %d", i, got, want)
		}
	}

	row1Start, row1End := lst.ValueOffsets(1)
	if row1End-row1Start != 1 {
		t.Errorf("row 1 length = %d, want 1", row1End-row1Start)
	}
	if got := values.Value(int(row1Start)); got != 4 {
		t.Errorf("row 1 elem 0 = %d, want 4", got)
	}
}

func TestArrow_ListStringColumnSchema(t *testing.T) {
	res := &router.Result{
		Columns: []string{"tags"},
		Rows: []map[string]any{
			{"tags": []any{"a", "b"}},
			{"tags": []any{"c"}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	lt := rec.Schema().Field(0).Type.(*arrow.ListType)
	if lt.Elem().ID() != arrow.STRING {
		t.Errorf("element type = %v, want STRING", lt.Elem())
	}
}

func TestArrow_ListFloat64PromotesIntElements(t *testing.T) {
	// Mixing int64 and float64 inside a list follows the same int→float
	// promotion rule used for scalar columns. The schema must end up
	// list<float64>, not the JSON fallback.
	res := &router.Result{
		Columns: []string{"mixed"},
		Rows: []map[string]any{
			{"mixed": []any{int64(1), 2.5}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	lt := rec.Schema().Field(0).Type.(*arrow.ListType)
	if lt.Elem().ID() != arrow.FLOAT64 {
		t.Errorf("element type = %v, want FLOAT64", lt.Elem())
	}
	values := rec.Column(0).(*array.List).ListValues().(*array.Float64)
	if got := values.Value(0); got != 1.0 {
		t.Errorf("elem 0 = %v, want 1.0", got)
	}
	if got := values.Value(1); got != 2.5 {
		t.Errorf("elem 1 = %v, want 2.5", got)
	}
}

func TestArrow_ListBoolColumnSchema(t *testing.T) {
	res := &router.Result{
		Columns: []string{"flags"},
		Rows: []map[string]any{
			{"flags": []any{true, false, true}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	lt := rec.Schema().Field(0).Type.(*arrow.ListType)
	if lt.Elem().ID() != arrow.BOOL {
		t.Errorf("element type = %v, want BOOL", lt.Elem())
	}
}

func TestArrow_HeterogeneousListFallsBackToJSON(t *testing.T) {
	// Mixing string + int in the same list isn't expressible as
	// list<T>, so the column should drop down to JSON utf8 like any
	// other heterogeneous payload.
	res := &router.Result{
		Columns: []string{"x"},
		Rows: []map[string]any{
			{"x": []any{"hello", int64(1)}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	if got := rec.Schema().Field(0).Type.ID(); got != arrow.STRING {
		t.Errorf("heterogeneous list column must fall back to STRING (JSON), got %v", got)
	}
}

func TestArrow_ListAndScalarInSameColumnFallsBackToJSON(t *testing.T) {
	// A column that's "sometimes a list, sometimes a scalar" can't be
	// expressed without a union type — JSON fallback is the documented
	// escape hatch.
	res := &router.Result{
		Columns: []string{"x"},
		Rows: []map[string]any{
			{"x": []any{int64(1), int64(2)}},
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
		t.Errorf("list+scalar column must fall back to STRING (JSON), got %v", got)
	}
}

func TestArrow_EmptyListDefaultsToStringElement(t *testing.T) {
	// All-empty / all-null lists give us no element kind to infer
	// from, but the column still needs a concrete type. utf8 is the
	// safe pick and stays compatible with downstream readers.
	res := &router.Result{
		Columns: []string{"xs"},
		Rows: []map[string]any{
			{"xs": []any{}},
			{"xs": []any{}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	lt, ok := rec.Schema().Field(0).Type.(*arrow.ListType)
	if !ok {
		t.Fatalf("xs must be LIST, got %v", rec.Schema().Field(0).Type)
	}
	if lt.Elem().ID() != arrow.STRING {
		t.Errorf("empty-list default elem = %v, want STRING", lt.Elem())
	}

	lst := rec.Column(0).(*array.List)
	for i := 0; i < 2; i++ {
		s, e := lst.ValueOffsets(i)
		if e-s != 0 {
			t.Errorf("row %d length = %d, want 0", i, e-s)
		}
	}
}

func TestArrow_ListWithNullElementsRoundTrips(t *testing.T) {
	// Null elements inside a list need to land as null mask bits in the
	// child array, not as the JSON fallback. Lets clients distinguish
	// "list<int64>{1, null, 3}" from "list<int64>{1, 3}".
	res := &router.Result{
		Columns: []string{"xs"},
		Rows: []map[string]any{
			{"xs": []any{int64(1), nil, int64(3)}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	lst := rec.Column(0).(*array.List)
	values := lst.ListValues().(*array.Int64)
	start, end := lst.ValueOffsets(0)
	if end-start != 3 {
		t.Fatalf("row 0 length = %d, want 3", end-start)
	}
	if values.IsNull(int(start) + 1) != true {
		t.Errorf("middle element should be null")
	}
	if got := values.Value(int(start)); got != 1 {
		t.Errorf("first elem = %d, want 1", got)
	}
	if got := values.Value(int(start) + 2); got != 3 {
		t.Errorf("third elem = %d, want 3", got)
	}
}

func TestArrow_NullListRowProducesNullMask(t *testing.T) {
	// A row that's missing the column entirely (nil) on a list-typed
	// column should land as a null list, not an empty list.
	res := &router.Result{
		Columns: []string{"xs"},
		Rows: []map[string]any{
			{"xs": []any{int64(1), int64(2)}},
			{"xs": nil},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	rec, done := readArrow(t, buf)
	defer done()

	lst := rec.Column(0).(*array.List)
	if lst.IsNull(0) {
		t.Errorf("row 0 should not be null")
	}
	if !lst.IsNull(1) {
		t.Errorf("row 1 should be null")
	}
}
