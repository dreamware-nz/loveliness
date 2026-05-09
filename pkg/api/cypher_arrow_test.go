package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestCypher_AcceptArrowFile_ReturnsArrowBuffer(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher",
		bytes.NewBufferString("MATCH (p:Person) RETURN p"))
	req.Header.Set("Accept", contentTypeArrowFile)
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != contentTypeArrowFile {
		t.Errorf("expected Content-Type %q, got %q", contentTypeArrowFile, got)
	}
	r, err := ipc.NewFileReader(bytes.NewReader(w.Body.Bytes()),
		ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("response is not a valid Arrow file: %v", err)
	}
	r.Close()
}

func TestCypher_AcceptArrowStream_ReturnsStreamBuffer(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher",
		bytes.NewBufferString("MATCH (p:Person) RETURN p"))
	req.Header.Set("Accept", contentTypeArrowStream)
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != contentTypeArrowStream {
		t.Errorf("expected Content-Type %q, got %q", contentTypeArrowStream, got)
	}
	// Stream payload must NOT carry the "ARROW1" file-format magic;
	// guards against the dispatch silently downgrading to the file
	// encoder under the stream content type.
	if bytes.HasPrefix(w.Body.Bytes(), []byte("ARROW1")) {
		t.Fatal("stream Accept returned file-format payload (ARROW1 magic present)")
	}
	r, err := ipc.NewReader(bytes.NewReader(w.Body.Bytes()),
		ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		t.Fatalf("response is not a valid Arrow stream: %v", err)
	}
	r.Release()
}

func TestCypher_AcceptJSON_StaysJSON(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher",
		bytes.NewBufferString("MATCH (p:Person) RETURN p"))
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("expected JSON content-type, got %q", got)
	}
}

func TestCypher_AcceptUnsupported_Returns406(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher",
		bytes.NewBufferString("MATCH (p:Person) RETURN p"))
	req.Header.Set("Accept", "application/xml")
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusNotAcceptable {
		t.Errorf("expected 406, got %d: %s", w.Code, w.Body.String())
	}
}

func TestCypher_NoAcceptHeader_StaysJSON(t *testing.T) {
	srv := setupTestServer()
	req := httptest.NewRequest("POST", "/cypher",
		bytes.NewBufferString("MATCH (p:Person) RETURN p"))
	// no Accept header at all
	w := httptest.NewRecorder()

	srv.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("default content-type must remain JSON, got %q", got)
	}
}
