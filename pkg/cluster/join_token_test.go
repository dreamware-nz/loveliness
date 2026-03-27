package cluster

import (
	"testing"
	"time"
)

func TestTokenStore_GenerateAndValidate(t *testing.T) {
	s := NewTokenStore()
	tok, err := s.Generate(5 * time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if tok.Token == "" {
		t.Error("expected non-empty token")
	}
	if len(tok.Token) != 64 { // 32 bytes = 64 hex chars
		t.Errorf("expected 64-char token, got %d", len(tok.Token))
	}
	if !s.Validate(tok.Token) {
		t.Error("expected valid token")
	}
}

func TestTokenStore_SingleUse(t *testing.T) {
	s := NewTokenStore()
	tok, _ := s.Generate(5 * time.Minute)

	if !s.Validate(tok.Token) {
		t.Error("first use should succeed")
	}
	if s.Validate(tok.Token) {
		t.Error("second use should fail — tokens are single-use")
	}
}

func TestTokenStore_Expired(t *testing.T) {
	s := NewTokenStore()
	tok, _ := s.Generate(1 * time.Millisecond)

	time.Sleep(5 * time.Millisecond)

	if s.Validate(tok.Token) {
		t.Error("expired token should be rejected")
	}
}

func TestTokenStore_InvalidToken(t *testing.T) {
	s := NewTokenStore()
	if s.Validate("nonexistent") {
		t.Error("unknown token should be rejected")
	}
}

func TestTokenStore_Cleanup(t *testing.T) {
	s := NewTokenStore()
	s.Generate(1 * time.Millisecond)
	s.Generate(1 * time.Millisecond)
	s.Generate(5 * time.Minute)

	time.Sleep(5 * time.Millisecond)
	s.Cleanup()

	if s.Count() != 1 {
		t.Errorf("expected 1 active token after cleanup, got %d", s.Count())
	}
}

func TestTokenStore_Count(t *testing.T) {
	s := NewTokenStore()
	s.Generate(5 * time.Minute)
	s.Generate(5 * time.Minute)

	if s.Count() != 2 {
		t.Errorf("expected 2, got %d", s.Count())
	}
}
