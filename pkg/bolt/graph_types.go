package bolt

import (
	"hash/fnv"
	"strings"
)

// PackNode writes a Bolt Node struct.
//
// Bolt Node structure (tag 0x4E):
//
//	fields[0]: id (int64)
//	fields[1]: labels ([]string)
//	fields[2]: properties (map[string]any)
//	fields[3]: element_id (string) — Bolt v5+, we include for compatibility
func PackNode(p *Packer, id int64, labels []string, props map[string]any) {
	p.PackStructHeader(4, tagNode)
	p.PackInt(id)
	p.PackListHeader(len(labels))
	for _, l := range labels {
		p.PackString(l)
	}
	packProps(p, props)
	p.PackString(intToElementID(id))
}

// PackRelationship writes a Bolt Relationship struct.
//
// Bolt Relationship structure (tag 0x52):
//
//	fields[0]: id (int64)
//	fields[1]: startNodeId (int64)
//	fields[2]: endNodeId (int64)
//	fields[3]: type (string)
//	fields[4]: properties (map[string]any)
//	fields[5]: element_id (string)
//	fields[6]: start_element_id (string)
//	fields[7]: end_element_id (string)
func PackRelationship(p *Packer, id, startID, endID int64, relType string, props map[string]any) {
	p.PackStructHeader(8, tagRelationship)
	p.PackInt(id)
	p.PackInt(startID)
	p.PackInt(endID)
	p.PackString(relType)
	packProps(p, props)
	p.PackString(intToElementID(id))
	p.PackString(intToElementID(startID))
	p.PackString(intToElementID(endID))
}

func packProps(p *Packer, props map[string]any) {
	if props == nil {
		p.PackMapHeader(0)
		return
	}
	clean := make(map[string]any, len(props))
	for k, v := range props {
		// Strip "n." or "r." prefixes from column names.
		key := k
		if dot := strings.IndexByte(k, '.'); dot != -1 && dot < 3 {
			key = k[dot+1:]
		}
		if strings.HasPrefix(key, "_") {
			continue
		}
		clean[key] = v
	}
	p.PackMapHeader(len(clean))
	for k, v := range clean {
		p.PackString(k)
		p.PackValue(v)
	}
}

func intToElementID(id int64) string {
	return "4:" + itoa(id)
}

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := false
	if i < 0 {
		neg = true
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// SyntheticNodeID generates a stable int64 ID from a string key.
func SyntheticNodeID(label, key string) int64 {
	h := fnv.New64a()
	h.Write([]byte(label))
	h.Write([]byte{0})
	h.Write([]byte(key))
	return int64(h.Sum64() & 0x7FFFFFFFFFFFFFFF)
}

// SyntheticRelID generates a stable int64 ID for a relationship.
func SyntheticRelID(relType, fromKey, toKey string) int64 {
	h := fnv.New64a()
	h.Write([]byte(relType))
	h.Write([]byte{0})
	h.Write([]byte(fromKey))
	h.Write([]byte{0})
	h.Write([]byte(toKey))
	return int64(h.Sum64() & 0x7FFFFFFFFFFFFFFF)
}

// RowToFields converts a result row into an ordered field list, wrapping
// values as Bolt graph types (Node, Relationship) when detected.
//
// Detection heuristics:
//   - A column with no dot (e.g. "p" not "p.name") whose value is a map
//     with a "_label" or "_labels" key → Node
//   - A column whose value is a map with "_src" and "_dst" keys → Relationship
//   - Otherwise returned as-is (scalar, map, list)
func RowToFields(columns []string, row map[string]any) []any {
	fields := make([]any, len(columns))
	for i, col := range columns {
		val := row[col]
		fields[i] = maybeWrapGraphType(col, val)
	}
	return fields
}

// maybeWrapGraphType checks if a value looks like a node or relationship
// and returns a packable graph type wrapper. Otherwise returns the value as-is.
func maybeWrapGraphType(col string, val any) any {
	m, ok := val.(map[string]any)
	if !ok {
		return val
	}

	// Detect Node: map with "_label" key, or bare column name (no dot).
	if label, ok := m["_label"]; ok {
		return wrapNode(label.(string), m)
	}
	if labels, ok := m["_labels"]; ok {
		if ls, ok := labels.([]any); ok && len(ls) > 0 {
			if l, ok := ls[0].(string); ok {
				return wrapNode(l, m)
			}
		}
	}

	// Detect Relationship: map with "_src" and "_dst" and "_type".
	if _, hasSrc := m["_src"]; hasSrc {
		if _, hasDst := m["_dst"]; hasDst {
			return wrapRelationship(m)
		}
	}

	// Bare variable name (no dot) with a map value that has an "_id" field
	// could be a node — LadybugDB returns nodes as property maps.
	if !strings.Contains(col, ".") && len(m) > 0 {
		if id, ok := m["_id"]; ok {
			_ = id // has internal ID
			return wrapNodeFromProps(col, m)
		}
	}

	return val
}

// nodeWrapper is a sentinel type that packRecord can detect.
type nodeWrapper struct {
	ID    int64
	Label string
	Props map[string]any
}

type relWrapper struct {
	ID      int64
	StartID int64
	EndID   int64
	Type    string
	Props   map[string]any
}

func wrapNode(label string, m map[string]any) *nodeWrapper {
	props := make(map[string]any, len(m))
	var pk string
	for k, v := range m {
		if strings.HasPrefix(k, "_") {
			if k == "_id" || k == "_key" {
				if s, ok := v.(string); ok {
					pk = s
				}
			}
			continue
		}
		props[k] = v
	}
	// Use the first string property as PK fallback if no _id/_key.
	if pk == "" {
		for _, v := range props {
			if s, ok := v.(string); ok {
				pk = s
				break
			}
		}
	}
	return &nodeWrapper{
		ID:    SyntheticNodeID(label, pk),
		Label: label,
		Props: props,
	}
}

func wrapNodeFromProps(col string, m map[string]any) *nodeWrapper {
	props := make(map[string]any, len(m))
	var pk string
	for k, v := range m {
		if strings.HasPrefix(k, "_") {
			if k == "_id" || k == "_key" {
				if s, ok := v.(string); ok {
					pk = s
				}
			}
			continue
		}
		props[k] = v
	}
	if pk == "" {
		for _, v := range props {
			if s, ok := v.(string); ok {
				pk = s
				break
			}
		}
	}
	label := strings.ToUpper(col[:1]) + col[1:]
	return &nodeWrapper{
		ID:    SyntheticNodeID(label, pk),
		Label: label,
		Props: props,
	}
}

func wrapRelationship(m map[string]any) *relWrapper {
	props := make(map[string]any, len(m))
	var relType, src, dst string
	for k, v := range m {
		switch k {
		case "_type":
			relType, _ = v.(string)
		case "_src":
			src, _ = v.(string)
		case "_dst":
			dst, _ = v.(string)
		default:
			if !strings.HasPrefix(k, "_") {
				props[k] = v
			}
		}
	}
	return &relWrapper{
		ID:      SyntheticRelID(relType, src, dst),
		StartID: SyntheticNodeID("", src),
		EndID:   SyntheticNodeID("", dst),
		Type:    relType,
		Props:   props,
	}
}

// DetectNodeReturn checks if a column name suggests a full node return.
func DetectNodeReturn(col string) bool {
	return !strings.Contains(col, ".")
}
