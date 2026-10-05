package relay

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/workspace"
)

type memStore struct {
	mu sync.Mutex
	m  map[string]string
}

func (s *memStore) Register(_ context.Context, id, _, pod string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[id] = pod
	return nil
}
func (s *memStore) Unregister(_ context.Context, id, pod string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m[id] == pod {
		delete(s.m, id)
	}
	return nil
}
func (s *memStore) Lookup(_ context.Context, id string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.m[id]
	return p, ok, nil
}
func (s *memStore) Ping(context.Context, string, string) error { return nil }
func (s *memStore) Touch(context.Context, string)              {}

func setup(t *testing.T) (*jwt.Signer, *httptest.Server, string) {
	t.Helper()
	signer := jwt.NewSigner("https://nabu", []byte("test-key"))
	srv := NewServer(Config{Pod: "self", WaitConnect: 5 * time.Second, Reconnect: 2 * time.Second}, signer, &memStore{m: map[string]string{}})
	r := chi.NewRouter()
	srv.Routes(r)
	hs := httptest.NewServer(r)
	t.Cleanup(hs.Close)
	return signer, hs, "ws" + strings.TrimPrefix(hs.URL, "http") + "/v1/workspaces/connect"
}

func connectWorkspace(t *testing.T, signer *jwt.Signer, wsURL, id string) (string, context.CancelFunc) {
	t.Helper()
	root := t.TempDir()
	ws := &workspace.Workspace{Root: root, Token: "local", Env: workspace.CommandEnv(root)}
	ctx, cancel := context.WithCancel(context.Background())
	connected := make(chan struct{}, 1)
	c := &Client{URL: wsURL, Token: signer.Issue(jwt.Claims{Audience: jwt.AudWorkspace, Workspace: id, Kind: "external"}, time.Hour),
		WorkspaceID: id, Kind: "external", Handler: ws.Handler(), HandlerToken: "local",
		OnConnected: func() { connected <- struct{}{} }}
	go func() { _ = c.Run(ctx) }()
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		t.Fatal("workspace did not connect")
	}
	time.Sleep(50 * time.Millisecond)
	return root, cancel
}

func post(t *testing.T, url, tok string, body any) (*http.Response, []byte) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp, out
}

// RLY-01, RLY-03: a workspace connected with its token serves calls; exec streams.
func TestCallThroughRelay(t *testing.T) {
	signer, hs, wsURL := setup(t)
	_, cancel := connectWorkspace(t, signer, wsURL, "run-1")
	defer cancel()
	call := signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: "run-1"}, time.Hour)
	base := hs.URL + "/internal/v1/workspaces/run-1/v1/"
	resp, body := post(t, base+"fs/write", call, map[string]string{"path": "a.txt", "content": base64.StdEncoding.EncodeToString([]byte("hello"))})
	if resp.StatusCode/100 != 2 {
		t.Fatalf("write: %d %s", resp.StatusCode, body)
	}
	resp, body = post(t, base+"fs/read", call, map[string]string{"path": "a.txt"})
	var rd struct{ Data string }
	_ = json.Unmarshal(body, &rd)
	if got, _ := base64.StdEncoding.DecodeString(rd.Data); resp.StatusCode != 200 || string(got) != "hello" {
		t.Fatalf("read: %d %s", resp.StatusCode, body)
	}
	resp, body = post(t, base+"exec", call, map[string]any{"command": "printf 'x%.0s' $(seq 1 3000)", "cwd": ""})
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(nil, 1<<20)
	var out strings.Builder
	exit := -1
	for sc.Scan() {
		var rec map[string]any
		_ = json.Unmarshal(sc.Bytes(), &rec)
		if d, ok := rec["data"].(string); ok {
			b, _ := base64.StdEncoding.DecodeString(d)
			out.Write(b)
		}
		if c, ok := rec["exitCode"].(float64); ok {
			exit = int(c)
		}
	}
	if resp.StatusCode != 200 || exit != 0 || len(out.String()) != 3000 {
		t.Fatalf("exec: %d exit=%d len=%d", resp.StatusCode, exit, len(out.String()))
	}
}

// RLY-04: results larger than a frame arrive whole.
func TestLargeResult(t *testing.T) {
	signer, hs, wsURL := setup(t)
	_, cancel := connectWorkspace(t, signer, wsURL, "run-2")
	defer cancel()
	call := signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: "run-2"}, time.Hour)
	big := bytes.Repeat([]byte("0123456789"), 300000) // 3 MB
	base := hs.URL + "/internal/v1/workspaces/run-2/v1/"
	post(t, base+"fs/write", call, map[string]string{"path": "big", "content": base64.StdEncoding.EncodeToString(big)})
	_, body := post(t, base+"fs/read", call, map[string]string{"path": "big"})
	var rd struct{ Data string }
	if err := json.Unmarshal(body, &rd); err != nil {
		t.Fatal(err)
	}
	if got, _ := base64.StdEncoding.DecodeString(rd.Data); !bytes.Equal(got, big) {
		t.Fatalf("got %d bytes", len(got))
	}
}

// RLY-02: wrong tokens are refused.
func TestTokens(t *testing.T) {
	signer, hs, _ := setup(t)
	req, _ := http.NewRequest(http.MethodGet, hs.URL+"/v1/workspaces/connect", nil)
	req.Header.Set("Authorization", "Bearer "+signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: "x"}, time.Hour))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("connect with a call token: %d", resp.StatusCode)
	}
	other := signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: "other"}, time.Hour)
	if r, _ := post(t, hs.URL+"/internal/v1/workspaces/x/v1/fs/read", other, map[string]string{}); r.StatusCode != http.StatusUnauthorized {
		t.Fatalf("call with another workspace's token: %d", r.StatusCode)
	}
}

// RLY-07 (relay part): a workspace that never connects fails the call.
func TestNotConnected(t *testing.T) {
	signer, hs, _ := setup(t)
	call := signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: "ghost"}, time.Hour)
	start := time.Now()
	r, body := post(t, hs.URL+"/internal/v1/workspaces/ghost/v1/fs/stat", call, map[string]string{"path": "."})
	if r.StatusCode != http.StatusConflict || !strings.Contains(string(body), "workspace_not_connected") || time.Since(start) < 4*time.Second {
		t.Fatalf("%d %s", r.StatusCode, body)
	}
}
