package api

import (
	"encoding/json"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// Node and Relationship values flow through router.Result.Rows as the
// concrete structs the underlying store binding produces — today
// `lbug.Node` and `lbug.Relationship` from the LadybugDB Go binding,
// tomorrow potentially others. We deliberately do not import the
// binding here; instead, we duck-type by reflection on the structs'
// exported field set. This keeps pkg/api decoupled from the CGo
// binding for unit testing and future store backends.
//
// Node shape: ID (struct{TableID, Offset uint64}), Label string, Properties map[string]any
// Rel shape:  ID + SourceID + DestinationID (same struct), Label string, Properties map[string]any

// internalIDValue is the flattened ID we land in Arrow. Two int64s
// faithfully represent the LadybugDB 128-bit identifier.
type internalIDValue struct {
	tableID int64
	offset  int64
}

// nodeValue is the duck-typed projection of a node from any store.
type nodeValue struct {
	id        internalIDValue
	labels    []string
	propsJSON string
}

// relationshipValue is the duck-typed projection of a relationship.
type relationshipValue struct {
	id        internalIDValue
	startID   internalIDValue
	endID     internalIDValue
	label     string
	propsJSON string
}

// extractInternalID accepts anything that exposes uint TableID and
// Offset fields (lbug.InternalID's shape) and returns the int64
// pair. We accept both signed and unsigned underlying integer kinds
// so the helper also works for fixtures that use int64.
func extractInternalID(v any) (internalIDValue, bool) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return internalIDValue{}, false
	}
	tID := rv.FieldByName("TableID")
	off := rv.FieldByName("Offset")
	if !tID.IsValid() || !off.IsValid() {
		return internalIDValue{}, false
	}
	t, tOk := readIntField(tID)
	o, oOk := readIntField(off)
	if !tOk || !oOk {
		return internalIDValue{}, false
	}
	return internalIDValue{tableID: t, offset: o}, true
}

func readIntField(v reflect.Value) (int64, bool) {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		// Cast to int64. Kuzu IDs in practice never overflow int64;
		// if a deployment ever produces uint64 > 2^63 we would silently
		// alias — accepted because LadybugDB does not currently allocate
		// IDs in that range and a wrap-around is the visible signal.
		return int64(v.Uint()), true
	}
	return 0, false
}

// extractNode duck-types a value as a node. A struct with ID, Label,
// and Properties matching the expected kinds qualifies — but not if
// SourceID / DestinationID are also present, which marks it as a
// Relationship instead.
func extractNode(v any) (nodeValue, bool) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nodeValue{}, false
	}
	if rv.FieldByName("SourceID").IsValid() && rv.FieldByName("DestinationID").IsValid() {
		return nodeValue{}, false
	}
	fID := rv.FieldByName("ID")
	fLabel := rv.FieldByName("Label")
	fProps := rv.FieldByName("Properties")
	if !fID.IsValid() || !fLabel.IsValid() || !fProps.IsValid() {
		return nodeValue{}, false
	}
	if fLabel.Kind() != reflect.String {
		return nodeValue{}, false
	}
	id, ok := extractInternalID(fID.Interface())
	if !ok {
		return nodeValue{}, false
	}
	props, ok := fProps.Interface().(map[string]any)
	if !ok {
		return nodeValue{}, false
	}
	pj, err := json.Marshal(props)
	if err != nil {
		return nodeValue{}, false
	}
	var labels []string
	if lab := fLabel.String(); lab != "" {
		labels = []string{lab}
	}
	return nodeValue{id: id, labels: labels, propsJSON: string(pj)}, true
}

// extractRelationship duck-types a value as a relationship.
func extractRelationship(v any) (relationshipValue, bool) {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return relationshipValue{}, false
	}
	fID := rv.FieldByName("ID")
	fSrc := rv.FieldByName("SourceID")
	fDst := rv.FieldByName("DestinationID")
	fLabel := rv.FieldByName("Label")
	fProps := rv.FieldByName("Properties")
	if !fID.IsValid() || !fSrc.IsValid() || !fDst.IsValid() ||
		!fLabel.IsValid() || !fProps.IsValid() {
		return relationshipValue{}, false
	}
	if fLabel.Kind() != reflect.String {
		return relationshipValue{}, false
	}
	id, idOk := extractInternalID(fID.Interface())
	src, srcOk := extractInternalID(fSrc.Interface())
	dst, dstOk := extractInternalID(fDst.Interface())
	if !idOk || !srcOk || !dstOk {
		return relationshipValue{}, false
	}
	props, ok := fProps.Interface().(map[string]any)
	if !ok {
		return relationshipValue{}, false
	}
	pj, err := json.Marshal(props)
	if err != nil {
		return relationshipValue{}, false
	}
	return relationshipValue{
		id: id, startID: src, endID: dst,
		label:     fLabel.String(),
		propsJSON: string(pj),
	}, true
}

// arrowInternalIDType is the struct type used wherever a Kuzu-style
// 128-bit ID lands in Arrow. Two int64s faithfully represent the
// (TableID, Offset) pair without lossy compression to a single int64.
var arrowInternalIDType = arrow.StructOf(
	arrow.Field{Name: "table_id", Type: arrow.PrimitiveTypes.Int64},
	arrow.Field{Name: "offset", Type: arrow.PrimitiveTypes.Int64},
)

var arrowNodeType = arrow.StructOf(
	arrow.Field{Name: "id", Type: arrowInternalIDType},
	arrow.Field{Name: "labels", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	// properties is JSON-encoded utf8 for v1 of the mapping. A
	// follow-up slice promotes this to map<utf8, utf8>; clients can
	// detect the upgrade by checking loveliness.arrow_mapping_version.
	arrow.Field{Name: "properties", Type: arrow.BinaryTypes.String},
)

var arrowRelationshipType = arrow.StructOf(
	arrow.Field{Name: "id", Type: arrowInternalIDType},
	arrow.Field{Name: "start_id", Type: arrowInternalIDType},
	arrow.Field{Name: "end_id", Type: arrowInternalIDType},
	arrow.Field{Name: "label", Type: arrow.BinaryTypes.String},
	arrow.Field{Name: "properties", Type: arrow.BinaryTypes.String},
)

// appendInternalID writes one (table_id, offset) pair into the given
// StructBuilder. The caller must have already called Append(true)
// on the parent (or this builder, when used at the top level).
func appendInternalID(b *array.StructBuilder, id internalIDValue) {
	b.Append(true)
	b.FieldBuilder(0).(*array.Int64Builder).Append(id.tableID)
	b.FieldBuilder(1).(*array.Int64Builder).Append(id.offset)
}

// appendNode writes one node into the StructBuilder for arrowNodeType.
func appendNode(b *array.StructBuilder, n nodeValue) {
	b.Append(true)
	appendInternalID(b.FieldBuilder(0).(*array.StructBuilder), n.id)

	labelsB := b.FieldBuilder(1).(*array.ListBuilder)
	labelsB.Append(true)
	valB := labelsB.ValueBuilder().(*array.StringBuilder)
	for _, lab := range n.labels {
		valB.Append(lab)
	}

	b.FieldBuilder(2).(*array.StringBuilder).Append(n.propsJSON)
}

// appendRelationship writes one relationship into the StructBuilder
// for arrowRelationshipType.
func appendRelationship(b *array.StructBuilder, r relationshipValue) {
	b.Append(true)
	appendInternalID(b.FieldBuilder(0).(*array.StructBuilder), r.id)
	appendInternalID(b.FieldBuilder(1).(*array.StructBuilder), r.startID)
	appendInternalID(b.FieldBuilder(2).(*array.StructBuilder), r.endID)
	b.FieldBuilder(3).(*array.StringBuilder).Append(r.label)
	b.FieldBuilder(4).(*array.StringBuilder).Append(r.propsJSON)
}
