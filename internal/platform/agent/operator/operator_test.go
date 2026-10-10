package operator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
)

var testKey = []byte("0123456789abcdef0123456789abcdef")

func newOp(t *testing.T, cfg Config) *httptest.Server {
	t.Helper()
	cfg.Runtime = pi.Runtime{Command: []string{"/bin/false"}}
	cfg.WorkDir = t.TempDir()
	op, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(op.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, method, url, token string, body any) (int, string) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, url, &buf)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var e struct {
		Error string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&e)
	return resp.StatusCode, e.Error
}

func session(kind agent.SessionKind) agent.SessionRequest {
	return agent.SessionRequest{Kind: kind, Secrets: agent.Secrets{LLMKey: "sk"},
		Model: agent.ModelSpec{Provider: "nabu-x", API: "openai-completions", BaseURL: "http://llm", ModelID: "m", Models: []agent.ModelDef{{ID: "m"}}}}
}

// TOK-01, TOK-03: a pod of an owner admits only the tokens signed for this
// owner and this start of the pod.
func TestOwnerTokens(t *testing.T) {
	signer := jwt.NewSigner("https://nabu.test", testKey)
	pub, err := jwt.ParsePublicKey(signer.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	srv := newOp(t, Config{Mode: ModeOwner, Owner: "owner-a", Generation: 7, PublicKey: pub, ServiceToken: "svc"})
	tok := func(c jwt.Claims, ttl time.Duration) string { return signer.Issue(c, ttl) }
	good := tok(jwt.Claims{Audience: jwt.AudAgent, Subject: "owner-a", Generation: 7}, time.Minute)
	if code, _ := call(t, http.MethodGet, srv.URL+"/v1/status", good, nil); code != http.StatusOK {
		t.Fatal(code)
	}
	other := jwt.NewSigner("https://nabu.test", []byte("another-key-another-key-another!"))
	for name, bad := range map[string]string{
		"another owner":      tok(jwt.Claims{Audience: jwt.AudAgent, Subject: "owner-b", Generation: 7}, time.Minute),
		"another generation": tok(jwt.Claims{Audience: jwt.AudAgent, Subject: "owner-a", Generation: 6}, time.Minute),
		"expired":            tok(jwt.Claims{Audience: jwt.AudAgent, Subject: "owner-a", Generation: 7}, -time.Minute),
		"another audience":   tok(jwt.Claims{Audience: jwt.AudMCP, Subject: "owner-a", Generation: 7}, time.Minute),
		"another key":        other.Issue(jwt.Claims{Audience: jwt.AudAgent, Subject: "owner-a", Generation: 7}, time.Minute),
		"the pool's token":   "svc",
		"none":               "",
	} {
		if code, _ := call(t, http.MethodGet, srv.URL+"/v1/status", bad, nil); code != http.StatusUnauthorized {
			t.Fatalf("%s: %d", name, code)
		}
		if code, _ := call(t, http.MethodPost, srv.URL+"/v1/sessions/x/prompt", bad, agent.PromptRequest{Text: "hi"}); code != http.StatusUnauthorized {
			t.Fatalf("%s: prompt %d", name, code)
		}
	}
	// POD-05: no runs of service agents and no checks in a pod of an owner
	if code, e := call(t, http.MethodPost, srv.URL+"/v1/sessions", good, session(agent.KindRun)); code != http.StatusBadRequest || e != "invalid_request" {
		t.Fatal(code, e)
	}
	if code, _ := call(t, http.MethodPost, srv.URL+"/v1/checks/llm", good, map[string]any{}); code != http.StatusNotFound {
		t.Fatal(code)
	}
}

// POD-05: the pool runs service agents only.
func TestPoolRejectsConversations(t *testing.T) {
	srv := newOp(t, Config{Mode: ModePool, ServiceToken: "svc"})
	if code, e := call(t, http.MethodPost, srv.URL+"/v1/sessions", "svc", session(agent.KindChat)); code != http.StatusBadRequest || e != "invalid_request" {
		t.Fatal(code, e)
	}
	if code, _ := call(t, http.MethodGet, srv.URL+"/v1/status", "svc", nil); code != http.StatusNotFound {
		t.Fatal(code)
	}
}

func TestModeConfig(t *testing.T) {
	rt := pi.Runtime{Command: []string{"/bin/false"}}
	if _, err := New(Config{Runtime: rt, WorkDir: t.TempDir(), Mode: ModeOwner, ServiceToken: "svc"}); err == nil {
		t.Fatal("the owner mode needs the owner, the generation and the key")
	}
	if _, err := New(Config{Runtime: rt, WorkDir: t.TempDir(), Mode: "other", ServiceToken: "svc"}); err == nil {
		t.Fatal("an unknown mode")
	}
	if _, err := New(Config{Runtime: rt, WorkDir: t.TempDir(), Mode: ModePool}); err == nil {
		t.Fatal("the pool needs the service token")
	}
}
