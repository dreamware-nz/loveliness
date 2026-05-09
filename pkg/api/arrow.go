package api

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/johnjansen/loveliness/pkg/router"
)

// arrowColumnKind classifies a column's effective Arrow type after
// inspecting every row. NODE and RELATIONSHIP get strongly-typed
// Arrow structs (see arrow_node.go). LIST is strongly typed when
// every element across every row reduces to the same scalar kind;
// otherwise the column falls back to the JSON-encoded utf8 path.
// MAP currently still falls back to JSON until its dedicated slice
// lands.
type arrowColumnKind int

const (
	arrowKindNull arrowColumnKind = iota
	arrowKindBool
	arrowKindInt64
	arrowKindFloat64
	arrowKindString
	arrowKindNode
	arrowKindRelationship
	arrowKindList
	arrowKindJSON
)

// columnSpec is the resolved schema description for a result column.
// `kind` is the top-level Arrow type; `listElem` is only meaningful
// when kind == arrowKindList and carries the unified element kind
// across every row's list. We never nest deeper — a list-of-list
// degrades to the JSON fallback so the schema stays predictable.
type columnSpec struct {
	kind     arrowColumnKind
	listElem arrowColumnKind
}

func (s columnSpec) arrowType() arrow.DataType {
	switch s.kind {
	case arrowKindBool:
		return arrow.FixedWidthTypes.Boolean
	case arrowKindInt64:
		return arrow.PrimitiveTypes.Int64
	case arrowKindFloat64:
		return arrow.PrimitiveTypes.Float64
	case arrowKindString, arrowKindJSON:
		return arrow.BinaryTypes.String
	case arrowKindNode:
		return arrowNodeType
	case arrowKindRelationship:
		return arrowRelationshipType
	case arrowKindList:
		return arrow.ListOf(listElementType(s.listElem))
	default:
		return arrow.Null
	}
}

// listElementType returns the Arrow type for a list column's element
// kind. When all observed lists were empty / null, kind is Null and
// we default to utf8 — list<null> is technically legal but no
// downstream consumer reliably handles it.
func listElementType(k arrowColumnKind) arrow.DataType {
	switch k {
	case arrowKindBool:
		return arrow.FixedWidthTypes.Boolean
	case arrowKindInt64:
		return arrow.PrimitiveTypes.Int64
	case arrowKindFloat64:
		return arrow.PrimitiveTypes.Float64
	case arrowKindString, arrowKindNull:
		return arrow.BinaryTypes.String
	}
	// Heterogeneous element kinds shouldn't reach this path because
	// classifyColumn collapses such columns to the JSON fallback
	// before we land here. The default keeps the schema honest.
	return arrow.BinaryTypes.String
}

// classifyColumn walks every row's value for the column and resolves
// it to a columnSpec. Promotion rules within a column:
//   - int + float    → float64
//   - any other mix  → JSON utf8 fallback
//   - list elements  → unified the same way; mixed-element lists
//     degrade the entire column to JSON utf8
func classifyColumn(rows []map[string]any, col string) columnSpec {
	spec := columnSpec{kind: arrowKindNull}
	for _, r := range rows {
		v, ok := r[col]
		if !ok || v == nil {
			continue
		}
		next := specOf(v)
		spec = mergeSpecs(spec, next)
		if spec.kind == arrowKindJSON {
			return spec
		}
	}
	return spec
}

func kindOf(v any) arrowColumnKind {
	switch v.(type) {
	case bool:
		return arrowKindBool
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return arrowKindInt64
	case float32, float64:
		return arrowKindFloat64
	case string:
		return arrowKindString
	}
	if _, ok := extractNode(v); ok {
		return arrowKindNode
	}
	if _, ok := extractRelationship(v); ok {
		return arrowKindRelationship
	}
	return arrowKindJSON
}

// specOf classifies a single cell value into a columnSpec. Lists
// recurse: the slice's element kind is unified across all elements
// in this row. Cross-row unification happens in mergeSpecs.
func specOf(v any) columnSpec {
	if elems, ok := asListElements(v); ok {
		elemKind := arrowKindNull
		for _, e := range elems {
			if e == nil {
				continue
			}
			ek := kindOf(e)
			elemKind = mergeKinds(elemKind, ek)
			if elemKind == arrowKindJSON {
				// A list with heterogeneous / complex elements is
				// not expressible as Arrow `list<T>`; degrade the
				// whole column to JSON utf8.
				return columnSpec{kind: arrowKindJSON}
			}
		}
		return columnSpec{kind: arrowKindList, listElem: elemKind}
	}
	return columnSpec{kind: kindOf(v)}
}

// asListElements detects values that should land as Arrow lists.
// `[]any` is the common shape for Cypher LIST values (every store
// binding we currently target produces it). Strings are excluded
// from the slice path even though Go strings are byte-sliceable.
func asListElements(v any) ([]any, bool) {
	if s, ok := v.([]any); ok {
		return s, true
	}
	return nil, false
}

func mergeKinds(a, b arrowColumnKind) arrowColumnKind {
	if a == arrowKindNull {
		return b
	}
	if b == arrowKindNull {
		return a
	}
	if a == b {
		return a
	}
	// int64 + float64 → float64 is safe; everything else mixed → JSON.
	if (a == arrowKindInt64 && b == arrowKindFloat64) || (a == arrowKindFloat64 && b == arrowKindInt64) {
		return arrowKindFloat64
	}
	return arrowKindJSON
}

// mergeSpecs unifies two cell specs into a column spec. Lists merge
// elementwise; mixing a list with a non-list collapses the column to
// JSON utf8 (a column can't be "sometimes a scalar, sometimes a
// list" in Arrow without a union type).
func mergeSpecs(a, b columnSpec) columnSpec {
	if a.kind == arrowKindNull {
		return b
	}
	if b.kind == arrowKindNull {
		return a
	}
	if a.kind == arrowKindList && b.kind == arrowKindList {
		merged := mergeKinds(a.listElem, b.listElem)
		if merged == arrowKindJSON {
			return columnSpec{kind: arrowKindJSON}
		}
		return columnSpec{kind: arrowKindList, listElem: merged}
	}
	if a.kind == arrowKindList || b.kind == arrowKindList {
		return columnSpec{kind: arrowKindJSON}
	}
	return columnSpec{kind: mergeKinds(a.kind, b.kind)}
}

// buildArrowRecord turns the router.Result into a schema and a single
// record batch, shared by the file and stream encoders. Caller owns
// the returned record and must Release() it.
func buildArrowRecord(result *router.Result, mem memory.Allocator) (*arrow.Schema, arrow.RecordBatch, error) {
	cols := result.Columns
	rows := result.Rows

	specs := make([]columnSpec, len(cols))
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		specs[i] = classifyColumn(rows, c)
		fields[i] = arrow.Field{
			Name:     c,
			Type:     specs[i].arrowType(),
			Nullable: true,
		}
	}

	md := arrow.NewMetadata(
		schemaMetadataKeys(result),
		schemaMetadataValues(result),
	)
	schema := arrow.NewSchema(fields, &md)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	for _, row := range rows {
		for i, c := range cols {
			if err := appendCell(b.Field(i), specs[i], row[c]); err != nil {
				return nil, nil, fmt.Errorf("column %q: %w", c, err)
			}
		}
	}

	return schema, b.NewRecordBatch(), nil
}

// encodeResultAsArrow serializes a router.Result as an Arrow IPC
// "file" payload (random-access, length-prefixed) — the buffered
// variant. Use this for `application/vnd.apache.arrow.file`.
//
// Errors and Partial flags ride along as schema-level metadata so
// clients can surface them without needing a separate channel.
func encodeResultAsArrow(result *router.Result) ([]byte, error) {
	mem := memory.NewGoAllocator()

	schema, rec, err := buildArrowRecord(result, mem)
	if err != nil {
		return nil, err
	}
	defer rec.Release()

	var buf bytes.Buffer
	w, err := ipc.NewFileWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err != nil {
		return nil, fmt.Errorf("arrow file writer: %w", err)
	}
	if err := w.Write(rec); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("arrow write record: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("arrow close: %w", err)
	}
	return buf.Bytes(), nil
}

// encodeResultAsArrowStream serializes a router.Result as an Arrow
// IPC "stream" payload — schema message + record batches +
// end-of-stream marker, no random-access footer. Use this for
// `application/vnd.apache.arrow.stream`. Stream format is what
// DuckDB-WASM's `read_arrow` and pyarrow's `open_stream` expect.
func encodeResultAsArrowStream(result *router.Result) ([]byte, error) {
	mem := memory.NewGoAllocator()

	schema, rec, err := buildArrowRecord(result, mem)
	if err != nil {
		return nil, err
	}
	defer rec.Release()

	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("arrow stream write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("arrow stream close: %w", err)
	}
	return buf.Bytes(), nil
}

// arrowMappingVersion is the version of the Cypher→Arrow type
// mapping documented in docs/arrow-mapping.md. Bumped on a breaking
// change to a documented row; new rows alone don't bump it.
const arrowMappingVersion = "1"

func schemaMetadataKeys(result *router.Result) []string {
	keys := []string{"loveliness.arrow_mapping_version", "loveliness.partial"}
	if len(result.Errors) > 0 {
		keys = append(keys, "loveliness.errors")
	}
	return keys
}

func schemaMetadataValues(result *router.Result) []string {
	values := []string{arrowMappingVersion, boolStr(result.Partial)}
	if len(result.Errors) > 0 {
		errsJSON, _ := json.Marshal(result.Errors)
		values = append(values, string(errsJSON))
	}
	return values
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// appendCell writes one cell into the matching column builder.
// nil values append a null mask entry for any kind.
func appendCell(b array.Builder, spec columnSpec, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	if spec.kind == arrowKindList {
		return appendListCell(b.(*array.ListBuilder), spec.listElem, v)
	}
	return appendScalarCell(b, spec.kind, v)
}

// appendScalarCell handles the non-list cases. Factored out so the
// list path can reuse the same scalar-coercion logic when filling
// element children.
func appendScalarCell(b array.Builder, kind arrowColumnKind, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch kind {
	case arrowKindBool:
		bb, ok := v.(bool)
		if !ok {
			b.AppendNull()
			return nil
		}
		b.(*array.BooleanBuilder).Append(bb)
	case arrowKindInt64:
		n, ok := toInt64(v)
		if !ok {
			b.AppendNull()
			return nil
		}
		b.(*array.Int64Builder).Append(n)
	case arrowKindFloat64:
		f, ok := toFloat64(v)
		if !ok {
			b.AppendNull()
			return nil
		}
		b.(*array.Float64Builder).Append(f)
	case arrowKindString:
		s, ok := v.(string)
		if !ok {
			b.AppendNull()
			return nil
		}
		b.(*array.StringBuilder).Append(s)
	case arrowKindNode:
		n, ok := extractNode(v)
		if !ok {
			b.AppendNull()
			return nil
		}
		appendNode(b.(*array.StructBuilder), n)
	case arrowKindRelationship:
		r, ok := extractRelationship(v)
		if !ok {
			b.AppendNull()
			return nil
		}
		appendRelationship(b.(*array.StructBuilder), r)
	case arrowKindJSON:
		// Heterogeneous / complex values: serialize each cell as JSON
		// so clients can still read them. This is documented as the
		// fallback until proper struct/list types land.
		raw, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("json fallback: %w", err)
		}
		b.(*array.StringBuilder).Append(string(raw))
	case arrowKindNull:
		b.AppendNull()
	}
	return nil
}

// appendListCell pushes one list value into a ListBuilder. A non-list
// value lands as a null entry — classifyColumn already collapses
// list+scalar columns to JSON, so we never see a real mix here, but
// we stay defensive.
func appendListCell(lb *array.ListBuilder, elemKind arrowColumnKind, v any) error {
	elems, ok := asListElements(v)
	if !ok {
		lb.AppendNull()
		return nil
	}
	lb.Append(true)
	valB := lb.ValueBuilder()
	for _, e := range elems {
		if err := appendScalarCell(valB, elemKind, e); err != nil {
			return err
		}
	}
	return nil
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return int64(x), true
	}
	return 0, false
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	}
	return 0, false
}
