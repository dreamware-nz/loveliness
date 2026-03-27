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
	// element_id (string representation of id).
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
	// Filter out internal properties.
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

// intToElementID converts a numeric ID to a string element ID.
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
// Used when LadybugDB doesn't expose internal IDs.
func SyntheticNodeID(label, key string) int64 {
	h := fnv.New64a()
	h.Write([]byte(label))
	h.Write([]byte{0})
	h.Write([]byte(key))
	return int64(h.Sum64() & 0x7FFFFFFFFFFFFFFF) // keep positive
}

// RowToFields converts a LadybugDB result row (map[string]any with column
// names as keys) into an ordered field list matching the column order.
// If a column looks like a full node (has label + properties), it's packed
// as a Bolt Node struct.
func RowToFields(columns []string, row map[string]any) []any {
	fields := make([]any, len(columns))
	for i, col := range columns {
		fields[i] = row[col]
	}
	return fields
}

// DetectNodeReturn checks if a column name suggests a full node return
// (e.g., "p" from "RETURN p" where p is a node variable).
func DetectNodeReturn(col string) bool {
	// If the column has no dot (not p.name, just p), it might be a node.
	return !strings.Contains(col, ".")
}
