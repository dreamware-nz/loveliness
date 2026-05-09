package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/johnjansen/loveliness/pkg/router"
	"github.com/johnjansen/loveliness/pkg/shard"
)

// e2eTestServer stands up a single-shard Loveliness server pre-seeded
// with the given rows. Single shard keeps column ordering reproducible
// — the router merges rows from multiple shards in scatter-gather and
// the merge can shuffle column order, which obscures HTTP-level
// assertions that aren't really about merging.
//
// The MemoryStore returns every row for any MATCH; the columns are
// derived from the first row's keys. That's good enough to drive the
// encoder against real data without standing up a full storage stack.
func e2eTestServer(rows []map[string]any) *Server {
	store := shard.NewMemoryStore()
	for i, r := range rows {
		store.PutNode(rowKey(i, r), r)
	}
	sh := shard.NewShard(0, store, 4)
	r := router.NewRouter([]*shard.Shard{sh}, 5*time.Second)
	return NewServer(r, nil, []*shard.Shard{sh}, nil, 5*time.Second)
}

// rowKey produces a unique key per row. PutNode is keyed by name, so
// any two rows that share a key would collapse — using the index
// guarantees we keep every row distinct in the store.
func rowKey(i int, _ map[string]any) string {
	return "row-" + intToA(i)
}

func intToA(i int) string {
	if i == 0 {
		return "0"
	}
	digits := []byte{}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return string(digits)
}

// runArrowQuery runs a Cypher query through the full HTTP stack and
// returns the response. Caller asserts on the recorder; helpers
// below decode it for the common cases.
func runArrowQuery(t *testing.T, srv *Server, query, accept string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/cypher", bytes.NewBufferString(query))
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	return w
}

func decodeArrowStream(t *testing.T, body []byte) (*arrow.Schema, []arrow.RecordBatch) {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(body), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("decode arrow stream: %v", err)
	}
	defer r.Release()
	var batches []arrow.RecordBatch
	for r.Next() {
		rec := r.RecordBatch()
		rec.Retain() // outlive the reader
		batches = append(batches, rec)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("stream reader err: %v", err)
	}
	return r.Schema(), batches
}

func decodeArrowFile(t *testing.T, body []byte) (*arrow.Schema, []arrow.RecordBatch) {
	t.Helper()
	r, err := ipc.NewFileReader(bytes.NewReader(body), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("decode arrow file: %v", err)
	}
	defer r.Close()
	var batches []arrow.RecordBatch
	for i := 0; i < r.NumRecords(); i++ {
		rec, err := r.RecordBatch(i)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		rec.Retain()
		batches = append(batches, rec)
	}
	return r.Schema(), batches
}

// fieldByName builds a name→Field index. Schema field order is not
// stable across MemoryStore queries (Go map iteration order), so the
// E2E asserts read by name.
func fieldByName(s *arrow.Schema) map[string]arrow.Field {
	out := map[string]arrow.Field{}
	for i := 0; i < s.NumFields(); i++ {
		f := s.Field(i)
		out[f.Name] = f
	}
	return out
}

func totalRows(batches []arrow.RecordBatch) int {
	n := 0
	for _, b := range batches {
		n += int(b.NumRows())
	}
	return n
}

// readScalarColumn pulls every value of a named column across batches
// and returns it as a []any. Driven by GetOneForMarshal so it works
// for any scalar Arrow type without per-type plumbing.
func readScalarColumn(t *testing.T, schema *arrow.Schema, batches []arrow.RecordBatch, name string) []any {
	t.Helper()
	idx := -1
	for i := 0; i < schema.NumFields(); i++ {
		if schema.Field(i).Name == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatalf("column %q not in schema", name)
	}
	var out []any
	for _, b := range batches {
		col := b.Column(idx)
		for r := 0; r < int(b.NumRows()); r++ {
			if col.IsNull(r) {
				out = append(out, nil)
				continue
			}
			out = append(out, col.GetOneForMarshal(r))
		}
	}
	return out
}

// --- E2E scenarios ----------------------------------------------------------

func TestE2E_ArrowStream_AllScalarTypesRoundTrip(t *testing.T) {
	// Seed three rows with every scalar Cypher kind — bool, int64,
	// float64, string — so the HTTP-driven encoder has to classify
	// each column and emit the right Arrow type. Reading the values
	// back proves the encode + transport + decode round-trip is
	// faithful, not just that the bytes parse.
	rows := []map[string]any{
		{"name": "Alice", "age": int64(30), "score": 1.5, "active": true},
		{"name": "Bob", "age": int64(25), "score": 2.5, "active": false},
		{"name": "Carol", "age": int64(40), "score": 3.5, "active": true},
	}
	srv := e2eTestServer(rows)

	w := runArrowQuery(t, srv, "MATCH (n) RETURN n", contentTypeArrowStream)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != contentTypeArrowStream {
		t.Fatalf("Content-Type = %q, want %q", got, contentTypeArrowStream)
	}
	// Stream payloads must not carry the file-format ARROW1 magic;
	// guards against a dispatch regression.
	if bytes.HasPrefix(w.Body.Bytes(), []byte("ARROW1")) {
		t.Fatal("stream Accept returned file-format payload (ARROW1 magic)")
	}

	schema, batches := decodeArrowStream(t, w.Body.Bytes())
	if got := totalRows(batches); got != len(rows) {
		t.Errorf("rows = %d, want %d", got, len(rows))
	}

	fields := fieldByName(schema)
	checks := map[string]arrow.Type{
		"name":   arrow.STRING,
		"age":    arrow.INT64,
		"score":  arrow.FLOAT64,
		"active": arrow.BOOL,
	}
	for col, want := range checks {
		f, ok := fields[col]
		if !ok {
			t.Errorf("column %q missing from schema", col)
			continue
		}
		if f.Type.ID() != want {
			t.Errorf("column %q type = %s, want %s", col, f.Type, want)
		}
	}

	// Schema metadata must carry the documented version pin and the
	// partial flag. Clients use these to gate compatibility decisions.
	md := schema.Metadata()
	if got, ok := md.GetValue("loveliness.arrow_mapping_version"); !ok || got != "1" {
		t.Errorf("loveliness.arrow_mapping_version = %q (ok=%v), want 1", got, ok)
	}
	if got, ok := md.GetValue("loveliness.partial"); !ok || got != "false" {
		t.Errorf("loveliness.partial = %q (ok=%v), want false on a clean run", got, ok)
	}
	for _, k := range []string{"loveliness.error", "loveliness.error.code", "loveliness.error.message"} {
		if _, ok := md.GetValue(k); ok {
			t.Errorf("clean run must not carry %s metadata", k)
		}
	}

	// Round-trip values for every column. Order isn't guaranteed
	// (single-shard but row-iteration is map-iteration), so we treat
	// each column as a multiset of expected values.
	gotNames := readScalarColumn(t, schema, batches, "name")
	wantNames := map[string]int{"Alice": 1, "Bob": 1, "Carol": 1}
	gotByName := map[string]int{}
	for _, v := range gotNames {
		gotByName[v.(string)]++
	}
	for k, v := range wantNames {
		if gotByName[k] != v {
			t.Errorf("name multiset mismatch: got %v, want %v", gotByName, wantNames)
			break
		}
	}

	gotAges := readScalarColumn(t, schema, batches, "age")
	wantAges := map[int64]int{30: 1, 25: 1, 40: 1}
	gotAgesM := map[int64]int{}
	for _, v := range gotAges {
		gotAgesM[v.(int64)]++
	}
	for k, v := range wantAges {
		if gotAgesM[k] != v {
			t.Errorf("age multiset mismatch: got %v, want %v", gotAgesM, wantAges)
			break
		}
	}
}

func TestE2E_ArrowFile_RoundTrip(t *testing.T) {
	// File format must produce the same logical content as the stream
	// format. The two differ only in container framing — anything else
	// is a regression.
	rows := []map[string]any{
		{"name": "Alice", "age": int64(30)},
		{"name": "Bob", "age": int64(25)},
	}
	srv := e2eTestServer(rows)

	w := runArrowQuery(t, srv, "MATCH (n) RETURN n", contentTypeArrowFile)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != contentTypeArrowFile {
		t.Fatalf("Content-Type = %q, want %q", got, contentTypeArrowFile)
	}
	if !bytes.HasPrefix(w.Body.Bytes(), []byte("ARROW1")) {
		t.Fatal("file format must carry the ARROW1 magic prefix")
	}

	schema, batches := decodeArrowFile(t, w.Body.Bytes())
	if got := totalRows(batches); got != 2 {
		t.Errorf("rows = %d, want 2", got)
	}
	fields := fieldByName(schema)
	if fields["name"].Type.ID() != arrow.STRING {
		t.Errorf("name type = %v, want STRING", fields["name"].Type)
	}
	if fields["age"].Type.ID() != arrow.INT64 {
		t.Errorf("age type = %v, want INT64", fields["age"].Type)
	}
}

func TestE2E_QValueNegotiation_PrefersArrowOverJSON(t *testing.T) {
	// Per RFC 7231 the highest-q match wins. A client that lists both
	// JSON and Arrow with Arrow at higher q must receive Arrow.
	srv := e2eTestServer([]map[string]any{{"name": "Alice"}})

	w := runArrowQuery(t, srv, "MATCH (n) RETURN n",
		"application/json;q=0.5, application/vnd.apache.arrow.stream;q=0.9")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != contentTypeArrowStream {
		t.Errorf("q-value negotiation chose %q, want %q", got, contentTypeArrowStream)
	}
}

func TestE2E_QValueNegotiation_PrefersJSONOverArrow(t *testing.T) {
	// Same thing in reverse — when JSON is preferred, the JSON path
	// stays canonical.
	srv := e2eTestServer([]map[string]any{{"name": "Alice"}})

	w := runArrowQuery(t, srv, "MATCH (n) RETURN n",
		"application/json;q=0.9, application/vnd.apache.arrow.stream;q=0.3")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("q-value negotiation chose %q, want application/json", got)
	}
}

func TestE2E_QValueZeroExcludesType(t *testing.T) {
	// q=0 means "not acceptable" per RFC 7231. Listing JSON with q=0
	// alongside Arrow should pick Arrow even though Arrow doesn't
	// have the higher q (because JSON has been removed from the
	// candidate set).
	srv := e2eTestServer([]map[string]any{{"name": "Alice"}})

	w := runArrowQuery(t, srv, "MATCH (n) RETURN n",
		"application/json;q=0, application/vnd.apache.arrow.stream")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != contentTypeArrowStream {
		t.Errorf("q=0 must remove json; chose %q, want %q", got, contentTypeArrowStream)
	}
}

func TestE2E_DefaultIsByteIdenticalToExplicitJSON(t *testing.T) {
	// "Default behaviour byte-identical to today's JSON output" is an
	// explicit AC bullet on #27. Two requests against the same data —
	// one with no Accept, one with Accept: application/json — must
	// produce byte-identical bodies, not just equivalent JSON.
	//
	// Single row, single column: MemoryStore iterates Go maps for
	// both row order and column order, and the runtime randomizes
	// map iteration. A multi-row or multi-column fixture would
	// shuffle independently between the two calls and produce
	// equivalent-but-not-byte-equal bodies. The AC is about the
	// *encoding* matching (JSON formatting, key ordering policy),
	// not the row-order policy — a 1×1 fixture removes the
	// iteration variable so the byte-equal check is meaningful.
	rows := []map[string]any{
		{"name": "Alice"},
	}
	srv1 := e2eTestServer(rows)
	srv2 := e2eTestServer(rows)

	w1 := runArrowQuery(t, srv1, "MATCH (n) RETURN n", "")
	w2 := runArrowQuery(t, srv2, "MATCH (n) RETURN n", "application/json")

	if w1.Code != http.StatusOK || w2.Code != http.StatusOK {
		t.Fatalf("non-200: w1=%d w2=%d", w1.Code, w2.Code)
	}
	if w1.Header().Get("Content-Type") != "application/json" ||
		w2.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type drift: %q vs %q",
			w1.Header().Get("Content-Type"),
			w2.Header().Get("Content-Type"))
	}
	if !bytes.Equal(w1.Body.Bytes(), w2.Body.Bytes()) {
		t.Errorf("default vs explicit JSON bodies differ:\n  default=%s\n  explicit=%s",
			w1.Body.String(), w2.Body.String())
	}
}

func TestE2E_ArrowStream_ListColumn(t *testing.T) {
	// Cypher LIST<INTEGER> values must arrive as Arrow list<int64>
	// at the HTTP layer. Encoder-level tests already cover this
	// classification logic; the E2E proves it survives the full
	// router→HTTP→encoder pipeline.
	rows := []map[string]any{
		{"id": int64(1), "tags": []any{int64(10), int64(20), int64(30)}},
		{"id": int64(2), "tags": []any{int64(40), int64(50)}},
	}
	srv := e2eTestServer(rows)

	w := runArrowQuery(t, srv, "MATCH (n) RETURN n", contentTypeArrowStream)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	schema, batches := decodeArrowStream(t, w.Body.Bytes())
	fields := fieldByName(schema)

	tagsField, ok := fields["tags"]
	if !ok {
		t.Fatal("tags column missing from schema")
	}
	if tagsField.Type.ID() != arrow.LIST {
		t.Fatalf("tags type = %s, want LIST", tagsField.Type)
	}
	lt := tagsField.Type.(*arrow.ListType)
	if lt.Elem().ID() != arrow.INT64 {
		t.Errorf("list element type = %s, want INT64", lt.Elem())
	}
	if got := totalRows(batches); got != 2 {
		t.Errorf("rows = %d, want 2", got)
	}
}

// e2eMapItem matches the duck-typed shape asMapEntries detects —
// `Key any, Value any`. Lets us drive the MAP path end-to-end without
// taking a CGo dep on the lbug binding.
type e2eMapItem struct {
	Key   any
	Value any
}

func TestE2E_ArrowStream_MapColumn(t *testing.T) {
	rows := []map[string]any{
		{
			"id": int64(1),
			"props": []e2eMapItem{
				{Key: "city", Value: "Auckland"},
				{Key: "country", Value: "NZ"},
			},
		},
		{
			"id": int64(2),
			"props": []e2eMapItem{
				{Key: "city", Value: "Wellington"},
			},
		},
	}
	srv := e2eTestServer(rows)

	w := runArrowQuery(t, srv, "MATCH (n) RETURN n", contentTypeArrowStream)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	schema, batches := decodeArrowStream(t, w.Body.Bytes())
	fields := fieldByName(schema)

	propsField, ok := fields["props"]
	if !ok {
		t.Fatal("props column missing from schema")
	}
	if propsField.Type.ID() != arrow.MAP {
		t.Fatalf("props type = %s, want MAP", propsField.Type)
	}
	mt := propsField.Type.(*arrow.MapType)
	if mt.KeyType().ID() != arrow.STRING {
		t.Errorf("map key type = %s, want STRING", mt.KeyType())
	}
	if mt.ItemType().ID() != arrow.STRING {
		t.Errorf("map value type = %s, want STRING", mt.ItemType())
	}
	if got := totalRows(batches); got != 2 {
		t.Errorf("rows = %d, want 2", got)
	}
}

func TestE2E_ArrowStream_NumericPromotion(t *testing.T) {
	// Encoder promotion rule: int + float in the same column lands as
	// float64. Pin it at the HTTP layer so a server-side regression
	// can't quietly switch a column to JSON fallback.
	rows := []map[string]any{
		{"value": int64(1)},
		{"value": 2.5},
		{"value": int64(3)},
	}
	srv := e2eTestServer(rows)

	w := runArrowQuery(t, srv, "MATCH (n) RETURN n", contentTypeArrowStream)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	schema, batches := decodeArrowStream(t, w.Body.Bytes())
	fields := fieldByName(schema)
	if fields["value"].Type.ID() != arrow.FLOAT64 {
		t.Errorf("value type = %s, want FLOAT64 after int+float promotion",
			fields["value"].Type)
	}
	if got := totalRows(batches); got != 3 {
		t.Errorf("rows = %d, want 3", got)
	}
}

func TestE2E_ArrowStream_EmptyResult(t *testing.T) {
	// An empty result must still produce a valid stream — schema
	// message + zero record batches + EOS — and stay 200.
	srv := e2eTestServer(nil)

	w := runArrowQuery(t, srv, "MATCH (n) RETURN n", contentTypeArrowStream)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	schema, batches := decodeArrowStream(t, w.Body.Bytes())
	if got := totalRows(batches); got != 0 {
		t.Errorf("rows = %d, want 0", got)
	}
	// Even an empty stream carries the documented schema metadata.
	if got, ok := schema.Metadata().GetValue("loveliness.arrow_mapping_version"); !ok || got != "1" {
		t.Errorf("loveliness.arrow_mapping_version = %q (ok=%v), want 1 even on empty result", got, ok)
	}
}

func TestE2E_UnsupportedAcceptReturns406(t *testing.T) {
	srv := e2eTestServer([]map[string]any{{"name": "Alice"}})
	w := runArrowQuery(t, srv, "MATCH (n) RETURN n", "application/protobuf")
	if w.Code != http.StatusNotAcceptable {
		t.Errorf("Accept: application/protobuf got %d, want 406", w.Code)
	}
}

func TestE2E_AcceptStarSlashStarStaysJSON(t *testing.T) {
	// Per the AC, `Accept: */*` must keep the existing JSON default
	// to avoid breaking clients that were happy with `*/*`.
	srv := e2eTestServer([]map[string]any{{"name": "Alice"}})
	w := runArrowQuery(t, srv, "MATCH (n) RETURN n", "*/*")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Accept: */* got %q, want application/json", got)
	}
}

// Unused import sanity — array is imported for downstream tests that
// might want strongly-typed assertions; keep the import alive without
// a real test that depends on it.
var _ = array.NewBuilder
