package auth

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// TokenAuth validates requests against a shared API token.
// Empty token disables authentication (dev mode).
type TokenAuth struct {
	token []byte
}

// New creates a TokenAuth. Empty token means auth is disabled.
func New(token string) *TokenAuth {
	return &TokenAuth{token: []byte(token)}
}

// Enabled returns true if a token is configured.
func (a *TokenAuth) Enabled() bool {
	return len(a.token) > 0
}

// ValidateToken checks whether the provided token matches.
func (a *TokenAuth) ValidateToken(provided string) bool {
	if !a.Enabled() {
		return true
	}
	return subtle.ConstantTimeCompare(a.token, []byte(provided)) == 1
}

// Middleware returns an HTTP middleware that requires a valid Bearer token.
// Unauthenticated requests get 401.
func (a *TokenAuth) Middleware(next http.Handler) http.Handler {
	if !a.Enabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := extractBearerToken(r)
		if !a.ValidateToken(token) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="loveliness"`)
			http.Error(w, `{"error":{"code":"UNAUTHORIZED","message":"invalid or missing token"}}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func extractBearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(h, "Bearer ") {
		return h[7:]
	}
	return ""
}
