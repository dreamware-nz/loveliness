package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// JoinToken is a single-use, time-limited token for cluster join authorization.
type JoinToken struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

// TokenStore manages join tokens.
type TokenStore struct {
	mu     sync.Mutex
	tokens map[string]*JoinToken
}

// NewTokenStore creates a new token store.
func NewTokenStore() *TokenStore {
	return &TokenStore{
		tokens: make(map[string]*JoinToken),
	}
}

// Generate creates a new join token valid for the given duration.
func (s *TokenStore) Generate(ttl time.Duration) (*JoinToken, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	t := &JoinToken{
		Token:     hex.EncodeToString(b),
		ExpiresAt: time.Now().Add(ttl),
	}

	s.mu.Lock()
	s.tokens[t.Token] = t
	s.mu.Unlock()

	return t, nil
}

// Validate checks and consumes a join token. Returns true if valid.
// Tokens are single-use — a valid token is deleted after validation.
func (s *TokenStore) Validate(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, ok := s.tokens[token]
	if !ok {
		return false
	}

	// Always delete — expired or consumed.
	delete(s.tokens, token)

	return time.Now().Before(t.ExpiresAt)
}

// Cleanup removes expired tokens.
func (s *TokenStore) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for k, t := range s.tokens {
		if now.After(t.ExpiresAt) {
			delete(s.tokens, k)
		}
	}
}

// Count returns the number of active (unexpired) tokens.
func (s *TokenStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	now := time.Now()
	for _, t := range s.tokens {
		if now.Before(t.ExpiresAt) {
			count++
		}
	}
	return count
}
