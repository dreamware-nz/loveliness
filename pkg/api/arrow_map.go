package api

import "reflect"

// mapEntry is the normalized form of a Cypher MAP entry, decoupled
// from any specific store binding. The encoder stays oblivious to
// the lbug.MapItem struct (or any future binding) and only relies
// on a duck-typed shape: a struct with exported `Key` and `Value`
// fields, both of type `any`.
type mapEntry struct {
	key   any
	value any
}

// asMapEntries detects a Cypher MAP value by shape: a slice whose
// elements are structs with exported `Key` and `Value` fields. This
// matches `[]lbug.MapItem` from the LadybugDB binding and any
// equivalent fixture used in tests, without forcing pkg/api to
// depend on the binding directly.
//
// `map[string]any` is intentionally NOT recognized here — Cypher MAPs
// arrive on the wire as ordered key/value sequences, and we don't
// want to silently reorder values when a downstream consumer relies
// on insertion order. If a caller wants Arrow `map<utf8, V>` from a
// Go map, they should normalize to a `[]MapItem` shape first.
func asMapEntries(v any) ([]mapEntry, bool) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, false
	}
	if rv.Kind() != reflect.Slice {
		return nil, false
	}
	// `[]any` is the runtime shape used elsewhere in the encoder for
	// LIST values. We only treat it as a map when every element is
	// itself a {Key, Value} struct — otherwise it's a list and the
	// list path takes over.
	out := make([]mapEntry, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		ev := rv.Index(i)
		// Unwrap interface{} to reach the concrete element struct.
		if ev.Kind() == reflect.Interface {
			ev = ev.Elem()
		}
		if !ev.IsValid() || ev.Kind() != reflect.Struct {
			return nil, false
		}
		k := ev.FieldByName("Key")
		val := ev.FieldByName("Value")
		if !k.IsValid() || !val.IsValid() {
			return nil, false
		}
		out = append(out, mapEntry{key: k.Interface(), value: val.Interface()})
	}
	if rv.Len() == 0 {
		// An empty slice is ambiguous (list-of-? vs map-of-?). We
		// treat it as a list to keep behavior backwards-compatible
		// with existing list-detection callers — empty MAP literals
		// are vanishingly rare on the wire and the JSON envelope
		// already serializes them identically.
		return nil, false
	}
	return out, true
}
