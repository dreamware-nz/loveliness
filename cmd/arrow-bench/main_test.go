package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// fakeCypherServer answers /cypher with a JSON or Arrow stream
// payload depending on the Accept header — same negotiation contract
// that the real server honors. Letting the bench tool talk to a
// httptest server is what makes it unit-testable without standing
// up a live cluster.
func fakeCypherServer(t *testing.T, rowCount int) *httptest.Server {
	t.Helper()
	rows := make([]map[string]any, rowCount)
	for i := range rows {
		rows[i] = map[string]any{"name": "person-" + strings.Repeat("x", i%5+1), "age": int64(20 + i%50)}
	}
	jsonBody, err := json.Marshal(map[string]any{
		"columns": []string{"name", "age"},
		"rows":    rows,
	})
	if err != nil {
		t.Fatalf("marshal json fixture: %v", err)
	}

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	rb := array.NewRecordBuilder(mem, schema)
	for _, r := range rows {
		rb.Field(0).(*array.StringBuilder).Append(r["name"].(string))
		rb.Field(1).(*array.Int64Builder).Append(r["age"].(int64))
	}
	rec := rb.NewRecordBatch()
	rb.Release()
	defer rec.Release()

	var arrowBuf bytes.Buffer
	w := ipc.NewWriter(&arrowBuf, ipc.WithSchema(schema), ipc.WithAllocator(mem))
	if err := w.Write(rec); err != nil {
		t.Fatalf("write arrow: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close arrow: %v", err)
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Accept") {
		case contentTypeArrowStream:
			w.Header().Set("Content-Type", contentTypeArrowStream)
			_, _ = w.Write(arrowBuf.Bytes())
		default:
			w.Header().Set("Content-Type", contentTypeJSON)
			_, _ = w.Write(jsonBody)
		}
	}))
}

func TestRun_ReportsBytesAndRows(t *testing.T) {
	srv := fakeCypherServer(t, 100)
	defer srv.Close()

	out, err := run(srv.URL, "MATCH (p:Person) RETURN p", 3, 5*time.Second)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out.Rows != 100 {
		t.Errorf("rows = %d, want 100", out.Rows)
	}
	if out.JSON.Bytes == 0 || out.Arrow.Bytes == 0 {
		t.Errorf("bytes must be populated for both formats: %+v", out)
	}
	if out.Iters != 3 {
		t.Errorf("iters = %d, want 3", out.Iters)
	}
}

// At 100 rows with this fixture, Arrow should produce a smaller
// payload than JSON. The fixture is too small to guarantee a
// dramatic ratio, but Arrow's tabular layout should be at least
// somewhat narrower than JSON's repeated field names.
func TestRun_ArrowSmallerThanJSON(t *testing.T) {
	srv := fakeCypherServer(t, 1000)
	defer srv.Close()

	out, err := run(srv.URL, "MATCH (p:Person) RETURN p", 1, 5*time.Second)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if out.Ratio.Bytes >= 1.0 {
		t.Errorf("expected Arrow payload < JSON at 1000 rows; got ratio = %.3f (json=%d arrow=%d)",
			out.Ratio.Bytes, out.JSON.Bytes, out.Arrow.Bytes)
	}
}

func TestRun_ItersZeroErrors(t *testing.T) {
	srv := fakeCypherServer(t, 1)
	defer srv.Close()
	if _, err := run(srv.URL, "MATCH (p) RETURN p", 0, 5*time.Second); err == nil {
		t.Error("expected error for iters=0")
	}
}

func TestRun_ReturnsErrorOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer srv.Close()

	if _, err := run(srv.URL, "MATCH (p) RETURN p", 1, 5*time.Second); err == nil {
		t.Error("expected error when server returns 500")
	}
}

// parseJSONRowCount must tolerate the real wire shape including
// Partial / Errors fields, not just the bare {columns, rows}.
func TestParseJSONRowCount_HandlesFullEnvelope(t *testing.T) {
	body := []byte(`{"columns":["x"],"rows":[{"x":1},{"x":2},{"x":3}],"partial":false,"errors":[]}`)
	n, err := parseJSONRowCount(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if n != 3 {
		t.Errorf("rows = %d, want 3", n)
	}
}
