// Package jwt issues and verifies the tokens of Nabu: EdDSA (Ed25519) JWTs.
//
// One key signs every token: client tokens (/oauth/token, 15 minutes), tokens
// of workspaces and sandboxes, tokens of the built-in MCP and the tokens Nabu
// presents to products when it acts on behalf of a user (FTR.NAB.CMN-0001
// arch §7, FTR.HMR.CMN-0006 tech §3.6). The audience separates them; products
// check signatures with the public keys of /.well-known/jwks.json.
package jwt

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Audiences of Nabu's own tokens.
const (
	AudClient    = "nabu:client"    // service client access token
	AudRunEvents = "nabu:run"       // reading the events of one run
	AudWorkspace = "nabu:workspace" // a workspace connecting to the relay
	AudCall      = "nabu:call"      // the agent calling a workspace through the relay
	AudSandbox   = "nabu:sandbox"   // a sandbox syncing its files through the api
	AudMCP       = "nabu:mcp"       // the built-in MCP of a session
	AudAgent     = "nabu:agent"     // the worker calling the agent pod of one owner
)

// Claims are the claims Nabu uses.
type Claims struct {
	Issuer    string   `json:"iss"`
	Subject   string   `json:"sub"`
	Audience  string   `json:"aud"`
	ExpiresAt int64    `json:"exp"`
	IssuedAt  int64    `json:"iat"`
	ID        string   `json:"jti,omitempty"`
	Scope     []string `json:"scope,omitempty"`
	// Workspace is the workspace of a workspace or call token.
	Workspace string `json:"ws,omitempty"`
	// Kind is sandbox or external for workspace tokens.
	Kind string `json:"kind,omitempty"`
	// User, Conversation, Channel, Run, Task scope MCP and sandbox tokens.
	User         string `json:"uid,omitempty"`
	Conversation string `json:"conv,omitempty"`
	Channel      string `json:"ch,omitempty"`
	Run          string `json:"run,omitempty"`
	Task         string `json:"task,omitempty"`
	Email        string `json:"email,omitempty"`
	// Generation is the start of the agent pod an agent token is for.
	Generation int64 `json:"gen,omitempty"`
}

// HasScope reports whether s is in the scope.
func (c Claims) HasScope(s string) bool {
	for _, x := range c.Scope {
		if x == s {
			return true
		}
	}
	return false
}

// Signer issues and verifies tokens.
type Signer struct {
	issuer string
	priv   ed25519.PrivateKey
	pub    ed25519.PublicKey
	kid    string
	now    func() time.Time
}

// NewSigner derives the signing key from the secrets key: one secret to keep,
// the same key on every pod. Rotating SECRETS_KEY rotates the signing key.
func NewSigner(issuer string, secretsKey []byte) *Signer {
	seed := sha256.Sum256(append([]byte("nabu-jwt-ed25519:"), secretsKey...))
	priv := ed25519.NewKeyFromSeed(seed[:])
	pub := priv.Public().(ed25519.PublicKey)
	h := sha256.Sum256(pub)
	return &Signer{issuer: strings.TrimRight(issuer, "/"), priv: priv, pub: pub,
		kid: base64.RawURLEncoding.EncodeToString(h[:8]), now: time.Now}
}

// Issuer is the iss of the tokens (the public API URL).
func (s *Signer) Issuer() string { return s.issuer }

// PublicKey is the verification key in the form agent pods get in their
// environment (FTR.NAB.CMN-0004 tech §3.1); it is not a secret.
func (s *Signer) PublicKey() string { return b64(s.pub) }

// ParsePublicKey reads the key of PublicKey.
func ParsePublicKey(v string) (ed25519.PublicKey, error) {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(v))
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil, errors.New("not an Ed25519 public key")
	}
	return ed25519.PublicKey(b), nil
}

// Issue signs claims; Issuer, IssuedAt, ExpiresAt and ID are filled in.
func (s *Signer) Issue(c Claims, ttl time.Duration) string {
	now := s.now()
	c.Issuer = s.issuer
	c.IssuedAt = now.Unix()
	c.ExpiresAt = now.Add(ttl).Unix()
	if c.ID == "" {
		c.ID = uuid.NewString()
	}
	head, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "JWT", "kid": s.kid})
	body, _ := json.Marshal(c)
	unsigned := b64(head) + "." + b64(body)
	return unsigned + "." + b64(ed25519.Sign(s.priv, []byte(unsigned)))
}

// Errors of Verify.
var (
	ErrMalformed = errors.New("malformed token")
	ErrSignature = errors.New("invalid token signature")
	ErrExpired   = errors.New("token expired")
	ErrAudience  = errors.New("token audience mismatch")
)

// Verify checks the signature, issuer, audience and expiry.
func (s *Signer) Verify(tok, audience string) (Claims, error) {
	return VerifyWith(tok, s.pub, s.issuer, audience, s.now())
}

// VerifyWith verifies a token with a known public key.
func VerifyWith(tok string, pub ed25519.PublicKey, issuer, audience string, now time.Time) (Claims, error) {
	var c Claims
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return c, ErrMalformed
	}
	var head struct {
		Alg string `json:"alg"`
	}
	hb, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || json.Unmarshal(hb, &head) != nil || head.Alg != "EdDSA" {
		return c, ErrMalformed
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return c, ErrMalformed
	}
	if !ed25519.Verify(pub, []byte(parts[0]+"."+parts[1]), sig) {
		return c, ErrSignature
	}
	bb, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(bb, &c) != nil {
		return c, ErrMalformed
	}
	if issuer != "" && c.Issuer != issuer {
		return c, fmt.Errorf("%w: issuer", ErrSignature)
	}
	if c.Audience != audience {
		return c, ErrAudience
	}
	if now.Unix() >= c.ExpiresAt {
		return c, ErrExpired
	}
	return c, nil
}

// JWKS is the public key set served at /.well-known/jwks.json.
func (s *Signer) JWKS() map[string]any {
	return map[string]any{"keys": []map[string]string{{
		"kty": "OKP", "crv": "Ed25519", "use": "sig", "alg": "EdDSA", "kid": s.kid, "x": b64(s.pub),
	}}}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }
