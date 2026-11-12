package engine

import (
	"encoding/json"
	"strings"
	"testing"
)

// FTR.HMR.DEV-0004 qa MSK-02: a secret in the arguments of a tool call is masked, the JSON stays JSON.
func TestMaskArgs(t *testing.T) {
	secret := "npm_0123456789abcdef"
	got := maskArgs(json.RawMessage(`{"command":"curl -H 'Authorization: Bearer `+secret+`' https://registry"}`), []string{secret})
	if strings.Contains(string(got), secret) || !json.Valid(got) || !strings.Contains(string(got), "***") {
		t.Fatalf("masked: %s", got)
	}
	if got := maskArgs(nil, []string{secret}); got != nil {
		t.Fatalf("empty: %s", got)
	}
}
