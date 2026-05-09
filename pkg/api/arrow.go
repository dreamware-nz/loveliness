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
// inspecting every row. Heterogeneous columns and complex types
// (lists, maps, nodes) fall back to JSON-encoded utf8 — that keeps
// the wire format honest while we ship the simple cases first;
// proper struct/list types are a follow-up slice.
type arrowColumnKind int

const (
	arrowKindNull arrowColumnKind = iota
	arrowKindBool
	arrowKindInt64
	arrowKindFloat64
	arrowKindString
	arrowKindJSON
)

func (k arrowColumnKind) arrowType() arrow.DataType {
	switch k {
	case arrowKindBool:
		return arrow.FixedWidthTypes.Boolean
	case arrowKindInt64:
		return arrow.PrimitiveTypes.Int64
	case arrowKindFloat64:
		return arrow.PrimitiveTypes.Float64
	case arrowKindString, arrowKindJSON:
		return arrow.BinaryTypes.String
	default:
		return arrow.Null
	}
}

// classifyColumn walks every row's value for the column and picks
// the narrowest Arrow kind that fits. Numeric promotion rules:
// int64 + float64 → float64; bool + anything-else → JSON utf8.
func classifyColumn(rows []map[string]any, col string) arrowColumnKind {
	kind := arrowKindNull
	for _, r := range rows {
		v, ok := r[col]
		if !ok || v == nil {
			continue
		}
		next := kindOf(v)
		kind = mergeKinds(kind, next)
		if kind == arrowKindJSON {
			return kind
		}
	}
	return kind
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
	default:
		return arrowKindJSON
	}
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

// encodeResultAsArrow serializes a router.Result as an Arrow IPC
// "file" payload (random-access, length-prefixed). This is the
// buffered variant from the spec — streaming is a follow-up.
//
// Errors and Partial flags ride along as schema-level metadata so
// clients can surface them without needing a separate channel.
func encodeResultAsArrow(result *router.Result) ([]byte, error) {
	mem := memory.NewGoAllocator()

	cols := result.Columns
	rows := result.Rows

	kinds := make([]arrowColumnKind, len(cols))
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		kinds[i] = classifyColumn(rows, c)
		fields[i] = arrow.Field{
			Name:     c,
			Type:     kinds[i].arrowType(),
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
			if err := appendCell(b.Field(i), kinds[i], row[c]); err != nil {
				return nil, fmt.Errorf("column %q: %w", c, err)
			}
		}
	}

	rec := b.NewRecordBatch()
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

func schemaMetadataKeys(result *router.Result) []string {
	keys := []string{"loveliness.partial"}
	if len(result.Errors) > 0 {
		keys = append(keys, "loveliness.errors")
	}
	return keys
}

func schemaMetadataValues(result *router.Result) []string {
	values := []string{boolStr(result.Partial)}
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
func appendCell(b array.Builder, kind arrowColumnKind, v any) error {
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
