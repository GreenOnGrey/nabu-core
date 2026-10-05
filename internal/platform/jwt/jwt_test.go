package jwt

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

func TestIssueVerify(t *testing.T) {
	s := NewSigner("https://nabu-api.example.org/", []byte("0123456789abcdef0123456789abcdef"))
	tok := s.Issue(Claims{Subject: "client:hammurapi", Audience: AudClient, Scope: []string{"delegate"}}, time.Minute)
	c, err := s.Verify(tok, AudClient)
	if err != nil || c.Subject != "client:hammurapi" || !c.HasScope("delegate") || c.Issuer != "https://nabu-api.example.org" {
		t.Fatalf("verify: %+v %v", c, err)
	}
	if _, err := s.Verify(tok, AudWorkspace); !errors.Is(err, ErrAudience) {
		t.Fatalf("audience: %v", err)
	}
	other := NewSigner("https://nabu-api.example.org", []byte("another-key-another-key-another-k"))
	if _, err := other.Verify(tok, AudClient); !errors.Is(err, ErrSignature) {
		t.Fatalf("foreign key: %v", err)
	}
	s.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, err := s.Verify(tok, AudClient); !errors.Is(err, ErrExpired) {
		t.Fatalf("expiry: %v", err)
	}
}

func TestJWKSVerifies(t *testing.T) {
	s := NewSigner("https://n", []byte("k"))
	keys := s.JWKS()["keys"].([]map[string]string)
	x, _ := base64.RawURLEncoding.DecodeString(keys[0]["x"])
	tok := s.Issue(Claims{Audience: "hammurapi", Email: "a@b.c"}, time.Minute)
	if _, err := VerifyWith(tok, ed25519.PublicKey(x), "https://n", "hammurapi", time.Now()); err != nil {
		t.Fatal(err)
	}
}
