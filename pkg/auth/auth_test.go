package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTokenAuth_Disabled(t *testing.T) {
	a := New("")
	if a.Enabled() {
		t.Error("expected disabled with empty token")
	}
	if !a.ValidateToken("anything") {
		t.Error("disabled auth should accept any token")
	}
}

func TestTokenAuth_ValidToken(t *testing.T) {
	a := New("secret-token-123")
	if !a.Enabled() {
		t.Error("expected enabled")
	}
	if !a.ValidateToken("secret-token-123") {
		t.Error("expected valid token to pass")
	}
}

func TestTokenAuth_InvalidToken(t *testing.T) {
	a := New("secret-token-123")
	if a.ValidateToken("wrong-token") {
		t.Error("expected invalid token to fail")
	}
	if a.ValidateToken("") {
		t.Error("expected empty token to fail")
	}
}

func TestMiddleware_NoToken_Returns401(t *testing.T) {
	a := New("my-secret")
	handler := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/cypher", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
	if w.Header().Get("WWW-Authenticate") == "" {
		t.Error("expected WWW-Authenticate header")
	}
}

func TestMiddleware_WrongToken_Returns401(t *testing.T) {
	a := New("my-secret")
	handler := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/cypher", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

func TestMiddleware_ValidToken_Passes(t *testing.T) {
	a := New("my-secret")
	handler := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest("GET", "/cypher", nil)
	req.Header.Set("Authorization", "Bearer my-secret")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestMiddleware_Disabled_PassesAll(t *testing.T) {
	a := New("")
	handler := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/cypher", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with disabled auth, got %d", w.Code)
	}
}
