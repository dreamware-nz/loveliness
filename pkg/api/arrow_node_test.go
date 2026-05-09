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

// fakeInternalID matches lbug.InternalID's exported field set so the
// reflection-based extractor sees both shapes equivalently. We keep
// this as a local fixture rather than importing the lbug binding so
// the encoder stays decoupled from any specific store backend.
type fakeInternalID struct {
	TableID uint64
	Offset  uint64
}

type fakeNode struct {
	ID         fakeInternalID
	Label      string
	Properties map[string]any
}

type fakeRelationship struct {
	ID            fakeInternalID
	SourceID      fakeInternalID
	DestinationID fakeInternalID
	Label         string
	Properties    map[string]any
}

func TestExtractNode_PicksUpFakeShape(t *testing.T) {
	v := fakeNode{
		ID:         fakeInternalID{TableID: 1, Offset: 42},
		Label:      "Person",
		Properties: map[string]any{"name": "Alice"},
	}
	got, ok := extractNode(v)
	if !ok {
		t.Fatal("expected node shape to be detected")
	}
	if got.id.tableID != 1 || got.id.offset != 42 {
		t.Errorf("id = %+v, want {1, 42}", got.id)
	}
	if len(got.labels) != 1 || got.labels[0] != "Person" {
		t.Errorf("labels = %v, want [Person]", got.labels)
	}
	if got.propsJSON == "" {
		t.Errorf("propsJSON is empty")
	}
}

func TestExtractNode_RejectsRelationshipShape(t *testing.T) {
	rel := fakeRelationship{
		ID:            fakeInternalID{TableID: 1, Offset: 1},
		SourceID:      fakeInternalID{TableID: 1, Offset: 1},
		DestinationID: fakeInternalID{TableID: 1, Offset: 2},
		Label:         "KNOWS",
		Properties:    map[string]any{},
	}
	if _, ok := extractNode(rel); ok {
		t.Error("relationship struct must not be detected as a node")
	}
}

func TestExtractRelationship_PicksUpFakeShape(t *testing.T) {
	v := fakeRelationship{
		ID:            fakeInternalID{TableID: 2, Offset: 7},
		SourceID:      fakeInternalID{TableID: 1, Offset: 1},
		DestinationID: fakeInternalID{TableID: 1, Offset: 2},
		Label:         "KNOWS",
		Properties:    map[string]any{"since": int64(2020)},
	}
	got, ok := extractRelationship(v)
	if !ok {
		t.Fatal("expected relationship shape to be detected")
	}
	if got.label != "KNOWS" {
		t.Errorf("label = %q, want KNOWS", got.label)
	}
	if got.startID.offset != 1 || got.endID.offset != 2 {
		t.Errorf("start/end ids = %+v / %+v", got.startID, got.endID)
	}
}

func TestArrow_NodeColumnHasStructSchema(t *testing.T) {
	res := &router.Result{
		Columns: []string{"p"},
		Rows: []map[string]any{
			{"p": fakeNode{
				ID:         fakeInternalID{TableID: 1, Offset: 10},
				Label:      "Person",
				Properties: map[string]any{"name": "Alice", "age": int64(30)},
			}},
			{"p": fakeNode{
				ID:         fakeInternalID{TableID: 1, Offset: 11},
				Label:      "Person",
				Properties: map[string]any{"name": "Bob"},
			}},
		},
	}
	buf, err := encodeResultAsArrow(res)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	r, err := ipc.NewFileReader(bytes.NewReader(buf), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("read arrow: %v", err)
	}
	defer r.Close()

	if r.Schema().NumFields() != 1 {
		t.Fatalf("want 1 field, got %d", r.Schema().NumFields())
	}
	got := r.Schema().Field(0).Type
	if got.ID() != arrow.STRUCT {
		t.Fatalf("p must be STRUCT, got %v", got)
	}
	st := got.(*arrow.StructType)
	if st.NumFields() != 3 {
		t.Errorf("node struct must have 3 fields (id, labels, properties), got %d", st.NumFields())
	}
	if st.Field(0).Name != "id" || st.Field(0).Type.ID() != arrow.STRUCT {
		t.Errorf("field[0] = %v %v, want id STRUCT", st.Field(0).Name, st.Field(0).Type)
	}
	if st.Field(1).Name != "labels" || st.Field(1).Type.ID() != arrow.LIST {
		t.Errorf("field[1] = %v %v, want labels LIST", st.Field(1).Name, st.Field(1).Type)
	}
	if st.Field(2).Name != "properties" || st.Field(2).Type.ID() != arrow.STRING {
		t.Errorf("field[2] = %v %v, want properties STRING", st.Field(2).Name, st.Field(2).Type)
	}
}

func TestArrow_NodeColumnRoundTripsValues(t *testing.T) {
	res := &router.Result{
		Columns: []string{"p"},
		Rows: []map[string]any{
			{"p": fakeNode{
				ID:         fakeInternalID{TableID: 1, Offset: 10},
				Label:      "Person",
				Properties: map[string]any{"name": "Alice"},
			}},
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
	rec, err := r.RecordBatch(0)
	if err != nil {
		t.Fatalf("record: %v", err)
	}
	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
	// Inspect via the typed array APIs — GetOneForMarshal returns
	// pre-marshaled JSON tokens for nested types, which is the
	// shape consumers see but a pain to assert on. Reach through
	// the StructArray's children instead.
	st := rec.Column(0).(*array.Struct)
	idCol := st.Field(0).(*array.Struct)
	tableID := idCol.Field(0).(*array.Int64).Value(0)
	offset := idCol.Field(1).(*array.Int64).Value(0)
	if tableID != 1 || offset != 10 {
		t.Errorf("id = (%d, %d), want (1, 10)", tableID, offset)
	}

	labelsCol := st.Field(1).(*array.List)
	labelValues := labelsCol.ListValues().(*array.String)
	start, end := labelsCol.ValueOffsets(0)
	if end-start != 1 {
		t.Errorf("labels list length = %d, want 1", end-start)
	}
	if got := labelValues.Value(int(start)); got != "Person" {
		t.Errorf("label = %q, want Person", got)
	}

	propsCol := st.Field(2).(*array.String)
	props := propsCol.Value(0)
	if props == "" {
		t.Errorf("properties JSON is empty")
	}
	if want := `"name":"Alice"`; !bytes.Contains([]byte(props), []byte(want)) {
		t.Errorf("properties JSON %q does not contain %q", props, want)
	}
}

func TestArrow_RelationshipColumnHasStructSchema(t *testing.T) {
	res := &router.Result{
		Columns: []string{"r"},
		Rows: []map[string]any{
			{"r": fakeRelationship{
				ID:            fakeInternalID{TableID: 2, Offset: 7},
				SourceID:      fakeInternalID{TableID: 1, Offset: 1},
				DestinationID: fakeInternalID{TableID: 1, Offset: 2},
				Label:         "KNOWS",
				Properties:    map[string]any{"since": int64(2020)},
			}},
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

	got := r.Schema().Field(0).Type
	if got.ID() != arrow.STRUCT {
		t.Fatalf("r must be STRUCT, got %v", got)
	}
	st := got.(*arrow.StructType)
	wantFields := []struct {
		name string
		id   arrow.Type
	}{
		{"id", arrow.STRUCT},
		{"start_id", arrow.STRUCT},
		{"end_id", arrow.STRUCT},
		{"label", arrow.STRING},
		{"properties", arrow.STRING},
	}
	if st.NumFields() != len(wantFields) {
		t.Fatalf("relationship struct fields = %d, want %d", st.NumFields(), len(wantFields))
	}
	for i, want := range wantFields {
		if st.Field(i).Name != want.name || st.Field(i).Type.ID() != want.id {
			t.Errorf("field[%d] = %s %v, want %s %v",
				i, st.Field(i).Name, st.Field(i).Type, want.name, want.id)
		}
	}
}

// Mixing a node value with a scalar in the same column is still the
// JSON fallback — proves the heterogeneous-column rule still kicks
// in even with the new strongly-typed kinds.
func TestArrow_HeterogeneousNodeAndScalarFallsBackToJSON(t *testing.T) {
	res := &router.Result{
		Columns: []string{"x"},
		Rows: []map[string]any{
			{"x": fakeNode{
				ID:         fakeInternalID{TableID: 1, Offset: 1},
				Label:      "Person",
				Properties: map[string]any{},
			}},
			{"x": "scalar string"},
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
	if got := r.Schema().Field(0).Type.ID(); got != arrow.STRING {
		t.Errorf("heterogeneous column must fall back to STRING (JSON), got %v", got)
	}
}
