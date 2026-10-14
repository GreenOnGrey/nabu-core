//go:build integration

package itest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/comm"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
	"github.com/GreenOnGrey/nabu-core/internal/space"
)

// presignMem is the in-memory store with links: what the proxy gives the
// server is the key behind a fake host.
type presignMem struct{ *mem }

func (p presignMem) PresignGet(_ context.Context, key string, ttl time.Duration) (string, error) {
	return "http://s3.test/" + key + "?ttl=" + ttl.String(), nil
}

// fakeMCP is an MCP server with fixed tools that records the arguments of calls.
type fakeMCP struct {
	mu    sync.Mutex
	calls []map[string]any
	tools []map[string]any
}

func (f *fakeMCP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		} `json:"params"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	w.Header().Set("Content-Type", "application/json")
	var result any
	switch req.Method {
	case "tools/list":
		result = map[string]any{"tools": f.tools}
	case "tools/call":
		f.mu.Lock()
		f.calls = append(f.calls, map[string]any{"name": req.Params.Name, "args": req.Params.Arguments})
		f.mu.Unlock()
		result = map[string]any{"content": []map[string]string{{"type": "text", "text": "done"}}, "isError": req.Params.Name == "fails"}
	default:
		result = map[string]any{}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
}

func stringProp(desc string) map[string]any {
	return map[string]any{"type": "string", "description": desc}
}

// FTR.NAB.CMN-0006: CAT-03, CAT-05, CAT-06, CAT-09, CAT-13, CAT-14
func TestCatalogTools(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	cat := &catalog.Service{Pool: pool, Box: box, Signer: jwt.NewSigner("https://n", key), InternalURL: "http://api:8081"}
	tools := []agent.MCPTool{{Name: "read", ReadOnly: true}, {Name: "write"}}
	cat.CheckMCP = func(context.Context, agent.MCPCheckRequest) (*agent.MCPCheckResponse, error) {
		return &agent.MCPCheckResponse{OK: true, Tools: tools}, nil
	}
	mode := "platform"
	it, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "tools-ro", Source: &catalog.Source{Kind: "url", URL: "https://t/mcp"}, Mode: &mode})
	if err != nil || it.AllowedTools != nil || !it.ReadOnly || it.FileMaxBytes != catalog.DefaultFileMaxBytes {
		t.Fatalf("a new item has no list yet: %+v %v", it, err)
	}
	// the first check gives the list by the old rule (read-only: only reading tools)
	if it, err = cat.Check(ctx, it.ID, "", uuid.Nil); err != nil || strings.Join(it.AllowedTools, ",") != "read" || !it.ReadOnly {
		t.Fatalf("first check: %+v %v", it, err)
	}
	// CAT-03: a new tool of the server is not allowed by a later check
	tools = append(tools, agent.MCPTool{Name: "new", ReadOnly: true})
	if it, err = cat.Check(ctx, it.ID, "", uuid.Nil); err != nil || strings.Join(it.AllowedTools, ",") != "read" || len(it.Tools) != 3 ||
		strings.Join(it.NewTools, ",") != "new" {
		t.Fatalf("new tool: %+v %v", it, err)
	}
	// still new at the next check; saving the list clears the mark
	if it, err = cat.Check(ctx, it.ID, "", uuid.Nil); err != nil || strings.Join(it.NewTools, ",") != "new" {
		t.Fatalf("new tool kept: %+v %v", it, err)
	}
	// CAT-09
	if _, err := cat.Save(ctx, &it.ID, catalog.Input{AllowedTools: &[]string{"ghost"}}); !hasCode(err, "catalog_tool_unknown") {
		t.Fatalf("unknown tool: %v", err)
	}
	if it, err = cat.Save(ctx, &it.ID, catalog.Input{AllowedTools: &[]string{"read", "write"}}); err != nil || it.ReadOnly || len(it.NewTools) != 0 {
		t.Fatalf("a writing tool: %+v %v", it, err)
	}

	// CAT-05, CAT-06: the migration fills the list of checked items by read_only
	raw, _ := json.Marshal([]agent.MCPTool{{Name: "a", ReadOnly: true}, {Name: "b"}})
	var ro, rw uuid.UUID
	for _, q := range []struct {
		name string
		ro   bool
		id   *uuid.UUID
	}{{"mig-ro", true, &ro}, {"mig-rw", false, &rw}} {
		if err := pool.QueryRow(ctx, `INSERT INTO catalog_items (type, name, source, mode, read_only, tools) VALUES ('mcp',$1,'{"kind":"url","url":"https://m/mcp"}','platform',$2,$3) RETURNING id`,
			q.name, q.ro, raw).Scan(q.id); err != nil {
			t.Fatal(err)
		}
	}
	mig, err := os.ReadFile("../../migrations/0005_catalog_tools.sql")
	if err != nil {
		t.Fatal(err)
	}
	update := regexp.MustCompile(`(?s)UPDATE catalog_items SET allowed_tools.*?;`).Find(mig)
	if update == nil {
		t.Fatal("no UPDATE in the migration")
	}
	if _, err := pool.Exec(ctx, strings.Replace(string(update), "WHERE type = 'mcp'", "WHERE name LIKE 'mig-%' AND type = 'mcp'", 1)); err != nil {
		t.Fatal(err)
	}
	if a, _ := cat.Get(ctx, ro); strings.Join(a.AllowedTools, ",") != "a" {
		t.Fatalf("read-only item after the migration: %v", a.AllowedTools)
	}
	if b, _ := cat.Get(ctx, rw); strings.Join(b.AllowedTools, ",") != "a,b" {
		t.Fatalf("item after the migration: %v", b.AllowedTools)
	}

	// CAT-13, CAT-14: a personal OAuth item is checked with the access of the administrator
	var sent map[string]string
	cat.CheckMCP = func(_ context.Context, req agent.MCPCheckRequest) (*agent.MCPCheckResponse, error) {
		sent = req.Headers
		if req.Headers["Authorization"] == "" {
			return &agent.MCPCheckResponse{Error: "401"}, nil
		}
		return &agent.MCPCheckResponse{OK: true, Tools: tools}, nil
	}
	pmode := "personal"
	gi, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "gmail-like", Source: &catalog.Source{Kind: "url", URL: "https://g/mcp"}, Mode: &pmode,
		PersonalAuth: &catalog.PersonalAuth{Kind: "token"}, Published: ptr(true)})
	if err != nil {
		t.Fatal(err)
	}
	admin := newUser(t, "tools-admin@x.org")
	if _, err := cat.Check(ctx, gi.ID, "tools-admin@x.org", admin); !hasCode(err, "catalog_check_needs_connection") {
		t.Fatalf("not connected: %v", err)
	}
	if _, err := cat.Connect(ctx, admin, gi.ID, "admin-token"); err != nil {
		t.Fatal(err)
	}
	if gi, err = cat.Check(ctx, gi.ID, "tools-admin@x.org", admin); err != nil || sent["Authorization"] != "Bearer admin-token" || len(gi.Tools) != 3 {
		t.Fatalf("connected: %+v %v %v", gi, err, sent)
	}
}

func hasCode(err error, code string) bool {
	e, ok := apperr.As(err)
	return ok && e.Code == code
}

// FTR.NAB.CMN-0006: CAT-01, CAT-02, FIL-02, FIL-04, FIL-05, FIL-09, FIL-10, FIL-16, DOC-05 (a file over the limit of the item)
func TestMCPProxyFiles(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	signer := jwt.NewSigner("https://n", key)
	srv := &fakeMCP{tools: []map[string]any{
		{"name": "convert", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"source": stringProp("URL or local path")}}},
		{"name": "export", "annotations": map[string]any{"readOnlyHint": true}},
		{"name": "list_cached", "annotations": map[string]any{"readOnlyHint": true}},
	}}
	mcpSrv := httptest.NewServer(srv)
	defer mcpSrv.Close()

	s3 := presignMem{&mem{m: map[string][]byte{}}}
	sp := &space.Service{Pool: pool, S3: s3, Signer: signer, Quota: 1 << 20, MaxFile: 1 << 20}
	snaps := &space.Snapshots{Space: sp}
	cat := &catalog.Service{Pool: pool, Box: box, Signer: signer, InternalURL: "http://api:8081", Files: snaps.Snapshot}
	cat.CheckMCP = func(context.Context, agent.MCPCheckRequest) (*agent.MCPCheckResponse, error) {
		return &agent.MCPCheckResponse{OK: true, Tools: []agent.MCPTool{
			{Name: "convert", StringArgs: []string{"source"}}, {Name: "export", ReadOnly: true}, {Name: "list_cached", ReadOnly: true}}}, nil
	}
	mode := "platform"
	it, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "docling-it", Source: &catalog.Source{Kind: "url", URL: mcpSrv.URL}, Mode: &mode, Published: ptr(true)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.Check(ctx, it.ID, "", uuid.Nil); err != nil {
		t.Fatal(err)
	}
	limit := int64(2048)
	if it, err = cat.Save(ctx, &it.ID, catalog.Input{AllowedTools: &[]string{"convert", "export"},
		FileArgs: &[]catalog.FileArg{{Tool: "convert", Arg: "source"}}, FileMaxBytes: &limit}); err != nil {
		t.Fatal(err)
	}
	uid := newUser(t, "files@x.org")
	other := newUser(t, "files-other@x.org")
	if _, err := cat.Connect(ctx, uid, it.ID, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Put(ctx, uid, "docs/a.pdf", strings.NewReader("%PDF-a"), 6, "agent"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE spaces SET quota_bytes = 1048576 WHERE user_id = $1`, uid); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Put(ctx, uid, "big.pdf", strings.NewReader(strings.Repeat("x", 4096)), 4096, "agent"); err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Put(ctx, other, "secret.pdf", strings.NewReader("%PDF-b"), 6, "agent"); err != nil {
		t.Fatal(err)
	}

	r := chi.NewRouter()
	cat.ProxyRoutes(r, func(context.Context, uuid.UUID) (string, bool) { return "files@x.org", true }, nil)
	api := httptest.NewServer(r)
	defer api.Close()
	tok := cat.ProxyToken(uid, it.ID, "web", "", time.Hour)
	rpc := func(method string, params any) map[string]any {
		b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		req, _ := http.NewRequest(http.MethodPost, api.URL+"/internal/v1/mcp-proxy/"+it.ID.String(), bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s: %s", method, raw)
		}
		return out
	}
	resultText := func(m map[string]any) (string, bool) {
		res, _ := m["result"].(map[string]any)
		content, _ := res["content"].([]any)
		if len(content) == 0 {
			return "", false
		}
		isErr, _ := res["isError"].(bool)
		return content[0].(map[string]any)["text"].(string), isErr
	}

	// CAT-01, FIL-09
	list := rpc("tools/list", map[string]any{})
	b, _ := json.Marshal(list)
	if !strings.Contains(string(b), `"convert"`) || !strings.Contains(string(b), `"export"`) || strings.Contains(string(b), "list_cached") ||
		!strings.Contains(string(b), "Path to a file in the user's Nabu space") {
		t.Fatalf("tools/list: %s", b)
	}
	// CAT-02
	if text, isErr := resultText(rpc("tools/call", map[string]any{"name": "list_cached", "arguments": map[string]any{}})); !isErr || !strings.Contains(text, "not allowed") {
		t.Fatalf("denied: %q", text)
	}
	// FIL-02: the sandbox sleeps — a copy of the file of the space
	if _, isErr := resultText(rpc("tools/call", map[string]any{"name": "convert", "arguments": map[string]any{"source": "docs/a.pdf"}})); isErr {
		t.Fatal("convert failed")
	}
	srv.mu.Lock()
	n := len(srv.calls)
	got, _ := srv.calls[n-1]["args"].(map[string]any)["source"].(string)
	srv.mu.Unlock()
	m := regexp.MustCompile(`^http://s3\.test/(mcp-files/[0-9a-f-]{36}/a\.pdf)\?ttl=5m0s$`).FindStringSubmatch(got)
	if m == nil || string(s3.m[m[1]]) != "%PDF-a" {
		t.Fatalf("the server got %q", got)
	}
	// FIL-04, FIL-05, FIL-16, FIL-14: refused before the server is called
	for _, v := range []string{"https://x/a.pdf", "../" + other.String() + "/secret.pdf", "secret.pdf", "missing.pdf", "big.pdf"} {
		if text, isErr := resultText(rpc("tools/call", map[string]any{"name": "convert", "arguments": map[string]any{"source": v}})); !isErr {
			t.Errorf("%s accepted: %q", v, text)
		}
	}
	srv.mu.Lock()
	if len(srv.calls) != n {
		t.Errorf("refused calls reached the server: %d", len(srv.calls)-n)
	}
	srv.mu.Unlock()
	// FIL-10: a confirmed call gets the copy too
	if text, isErr, err := cat.CallTool(ctx, uid, it.ID, "convert", json.RawMessage(`{"source":"docs/a.pdf"}`)); err != nil || isErr || text != "done" {
		t.Fatalf("confirmed call: %q %v %v", text, isErr, err)
	}
	srv.mu.Lock()
	got, _ = srv.calls[len(srv.calls)-1]["args"].(map[string]any)["source"].(string)
	srv.mu.Unlock()
	if !strings.HasPrefix(got, "http://s3.test/mcp-files/") {
		t.Fatalf("confirmed call got %q", got)
	}
}

// fakeRelay answers the operations of a sandbox: fs/stat and fs/read of files.
type fakeRelay struct {
	files map[string]string // path → content; "" with dirs — a directory
	dirs  map[string]bool
	delay time.Duration
}

func (f *fakeRelay) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	time.Sleep(f.delay)
	var req struct {
		Path string `json:"path"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	w.Header().Set("Content-Type", "application/json")
	content, ok := f.files[req.Path]
	switch {
	case strings.HasSuffix(r.URL.Path, "/fs/stat"):
		_ = json.NewEncoder(w).Encode(map[string]any{"exists": ok || f.dirs[req.Path], "isDir": f.dirs[req.Path], "size": len(content)})
	case strings.HasSuffix(r.URL.Path, "/fs/read") && ok:
		_ = json.NewEncoder(w).Encode(map[string]string{"data": base64.StdEncoding.EncodeToString([]byte(content))})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// FTR.NAB.CMN-0006: FIL-01 (the current version from a running sandbox), FIL-03
// (a sandbox that does not answer — the copy from S3), FIL-06 (a directory),
// FIL-07 (the link lives 5 minutes), FIL-08 (copies older than an hour are
// removed), FIL-15 (the name of the copy), FIL-14 (the answer is read no
// further than the limit).
func TestSpaceSnapshots(t *testing.T) {
	ctx := context.Background()
	signer := jwt.NewSigner("https://n", key)
	s3 := presignMem{&mem{m: map[string][]byte{}}}
	sp := &space.Service{Pool: pool, S3: s3, Signer: signer, Quota: 1 << 20, MaxFile: 1 << 20}
	uid := newUser(t, "snap@x.org")
	if _, err := sp.Put(ctx, uid, "docs/a.pdf", strings.NewReader("old"), 3, "agent"); err != nil {
		t.Fatal(err)
	}
	relay := &fakeRelay{files: map[string]string{"docs/a.pdf": "new version", "big.bin": strings.Repeat("x", 4096)},
		dirs: map[string]bool{"docs": true}}
	srv := httptest.NewServer(relay)
	defer srv.Close()
	if _, err := pool.Exec(ctx, `INSERT INTO workspace_connections (workspace_id, kind, relay_pod) VALUES ($1, 'sandbox', 'p')
		ON CONFLICT (workspace_id) DO UPDATE SET last_ping_at = now()`, space.WorkspaceID(uid)); err != nil {
		t.Fatal(err)
	}
	snaps := &space.Snapshots{Space: sp, Relay: &space.RelayStore{Pool: pool}, RelayURL: srv.URL, Wait: 300 * time.Millisecond}
	copyOf := func(u string) string {
		m := regexp.MustCompile(`^http://s3\.test/(mcp-files/[0-9a-f-]{36}/[^?]+)\?ttl=5m0s$`).FindStringSubmatch(u)
		if m == nil {
			t.Fatalf("FIL-07, FIL-15: link %q", u)
		}
		return string(s3.m[m[1]])
	}
	u, err := snaps.Snapshot(ctx, uid, "docs/a.pdf", 1<<20)
	if err != nil || copyOf(u) != "new version" {
		t.Fatalf("FIL-01: %q %v", u, err)
	}
	if _, err := snaps.Snapshot(ctx, uid, "docs", 1<<20); !errors.Is(err, catalog.ErrFileNotFound) {
		t.Fatalf("FIL-06: a directory: %v", err)
	}
	if _, err := snaps.Snapshot(ctx, uid, "big.bin", 1024); !errors.Is(err, catalog.ErrFileTooLarge) {
		t.Fatalf("FIL-14: %v", err)
	}
	relay.delay = time.Second
	if u, err = snaps.Snapshot(ctx, uid, "docs/a.pdf", 1<<20); err != nil || copyOf(u) != "old" {
		t.Fatalf("FIL-03: %q %v", u, err)
	}
	// FIL-08
	list := &listedMem{presignMem: s3, at: map[string]time.Time{}}
	for k := range s3.m {
		list.at[k] = time.Now()
	}
	list.at["mcp-files/old/x.pdf"] = time.Now().Add(-2 * time.Hour)
	s3.m["mcp-files/old/x.pdf"] = []byte("x")
	sweeper := &space.Snapshots{Space: &space.Service{Pool: pool, S3: list}}
	if n, err := sweeper.Sweep(ctx); err != nil || n != 1 || s3.m["mcp-files/old/x.pdf"] != nil {
		t.Fatalf("FIL-08: %d %v", n, err)
	}
	if _, ok := s3.m[strings.TrimPrefix(regexp.MustCompile(`mcp-files/[^?]+`).FindString(u), "")]; !ok {
		t.Fatal("FIL-08: a fresh copy was removed")
	}
}

// listedMem lists the objects of the in-memory store with their times.
type listedMem struct {
	presignMem
	at map[string]time.Time
}

func (l *listedMem) List(_ context.Context, prefix string) ([]storage.Object, error) {
	var out []storage.Object
	for k := range l.m {
		if strings.HasPrefix(k, prefix) {
			out = append(out, storage.Object{Key: k, Size: int64(len(l.m[k])), LastModified: l.at[k]})
		}
	}
	return out, nil
}

// FTR.NAB.CMN-0006: CAT-11 (a changing allowed tool in a mail topic waits for
// the user), OBS-03 (tools/list is not counted), DOC-07 (a tool listing the
// cache of other users is refused), GML-09 (an adapter needs its tools among
// the allowed ones).
func TestMCPProxyHoldAndMetrics(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	signer := jwt.NewSigner("https://n", key)
	srv := &fakeMCP{tools: []map[string]any{{"name": "convert"}, {"name": "export", "annotations": map[string]any{"readOnlyHint": true}},
		{"name": "list_cached_documents", "annotations": map[string]any{"readOnlyHint": true}}}}
	mcpSrv := httptest.NewServer(srv)
	defer mcpSrv.Close()
	cat := &catalog.Service{Pool: pool, Box: box, Signer: signer, InternalURL: "http://api:8081"}
	cat.CheckMCP = func(context.Context, agent.MCPCheckRequest) (*agent.MCPCheckResponse, error) {
		return &agent.MCPCheckResponse{OK: true, Tools: []agent.MCPTool{{Name: "convert"}, {Name: "export", ReadOnly: true},
			{Name: "list_cached_documents", ReadOnly: true}}}, nil
	}
	mode := "platform"
	it, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "hold-it", Source: &catalog.Source{Kind: "url", URL: mcpSrv.URL}, Mode: &mode, Published: ptr(true)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.Check(ctx, it.ID, "", uuid.Nil); err != nil {
		t.Fatal(err)
	}
	if it, err = cat.Save(ctx, &it.ID, catalog.Input{AllowedTools: &[]string{"convert", "export"}}); err != nil {
		t.Fatal(err)
	}
	uid := newUser(t, "hold@x.org")
	if _, err := cat.Connect(ctx, uid, it.ID, ""); err != nil {
		t.Fatal(err)
	}
	var held []string
	r := chi.NewRouter()
	cat.ProxyRoutes(r, func(context.Context, uuid.UUID) (string, bool) { return "hold@x.org", true },
		func(_ context.Context, _, _, _ uuid.UUID, _, tool string, _ json.RawMessage) (string, bool) {
			held = append(held, tool)
			return "waits for the user", true
		})
	api := httptest.NewServer(r)
	defer api.Close()
	tok := cat.ProxyToken(uid, it.ID, "email", uuid.NewString(), time.Hour)
	rpc := func(method string, params any) string {
		b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		req, _ := http.NewRequest(http.MethodPost, api.URL+"/internal/v1/mcp-proxy/"+it.ID.String(), bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return string(raw)
	}
	calls := func() float64 { return proxyCalls(t, "hold-it", "") }
	before := calls()
	rpc("tools/list", map[string]any{})
	if calls() != before {
		t.Fatal("OBS-03: tools/list is counted")
	}
	if out := rpc("tools/call", map[string]any{"name": "convert", "arguments": map[string]any{}}); !strings.Contains(out, "waits for the user") ||
		strings.Join(held, ",") != "convert" {
		t.Fatalf("CAT-11: %s %v", out, held)
	}
	rpc("tools/call", map[string]any{"name": "export", "arguments": map[string]any{}})
	if len(held) != 1 {
		t.Fatal("CAT-11: a reading tool waits for the user")
	}
	if out := rpc("tools/call", map[string]any{"name": "list_cached_documents", "arguments": map[string]any{}}); !strings.Contains(out, "not allowed") {
		t.Fatalf("DOC-07: %s", out)
	}
	if proxyCalls(t, "hold-it", "list_cached_documents", "denied") != 1 {
		t.Fatal("OBS-01: denied is counted")
	}

	// GML-09: an item marked as a source of communications needs the tools of
	// its adapter among the allowed ones
	pmode := "personal"
	gi, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "gmail-src", Source: &catalog.Source{Kind: "url", URL: mcpSrv.URL},
		Mode: &pmode, PersonalAuth: &catalog.PersonalAuth{Kind: "token"}, Published: ptr(true)})
	if err != nil {
		t.Fatal(err)
	}
	gmailTools, _ := json.Marshal([]agent.MCPTool{{Name: "search_threads", ReadOnly: true}, {Name: "get_thread", ReadOnly: true}})
	if _, err := pool.Exec(ctx, `UPDATE catalog_items SET tools = $2, allowed_tools = '["search_threads"]' WHERE id = $1`, gi.ID, gmailTools); err != nil {
		t.Fatal(err)
	}
	cs := &comm.Service{Pool: pool}
	adapter, yes := "gmail", true
	if err := cs.SetSource(ctx, gi.ID, &adapter, &yes); !hasCode(err, "comm_source_tools_missing") {
		t.Fatalf("GML-09: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE catalog_items SET allowed_tools = '["search_threads","get_thread"]' WHERE id = $1`, gi.ID); err != nil {
		t.Fatal(err)
	}
	if err := cs.SetSource(ctx, gi.ID, &adapter, &yes); err != nil {
		t.Fatalf("GML-09: allowed: %v", err)
	}

	// YML-13: a mailbox over imap-mcp is a source through the adapter "mail";
	// the source is named by the item
	title := "Яндекс Почта"
	yi, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "yandex-mail-src", Title: &title, Source: &catalog.Source{Kind: "url", URL: mcpSrv.URL},
		Mode: &pmode, PersonalAuth: &catalog.PersonalAuth{Kind: "oauth", AuthorizeURL: "https://oauth.yandex.ru/authorize",
			TokenURL: "https://oauth.yandex.ru/token", ClientID: "c"}, Published: ptr(true)})
	if err != nil {
		t.Fatal(err)
	}
	mailTools, _ := json.Marshal([]agent.MCPTool{{Name: "list_folders", ReadOnly: true}, {Name: "search_messages", ReadOnly: true},
		{Name: "read_message", ReadOnly: true}, {Name: "read_thread", ReadOnly: true}})
	if _, err := pool.Exec(ctx, `UPDATE catalog_items SET tools = $2 WHERE id = $1`, yi.ID, mailTools); err != nil {
		t.Fatal(err)
	}
	mail := "mail"
	if err := cs.SetSource(ctx, yi.ID, &mail, &yes); err != nil {
		t.Fatalf("YML-13: %v", err)
	}
	srcs, err := cs.Sources(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, src := range srcs {
		if src.ItemID == yi.ID {
			found = src.Source == "mail" && src.Title == "Яндекс Почта"
		}
	}
	if !found {
		t.Fatalf("YML-13: sources %+v", srcs)
	}
}

// proxyCalls sums nabu_mcp_proxy_calls_total of an item; tool and result
// narrow it when given.
func proxyCalls(t *testing.T, item, tool string, result ...string) float64 {
	t.Helper()
	families, err := metrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	sum := 0.0
	for _, f := range families {
		if f.GetName() != "nabu_mcp_proxy_calls_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["item"] != item || (tool != "" && labels["tool"] != tool) || (len(result) > 0 && labels["result"] != result[0]) {
				continue
			}
			sum += m.GetCounter().GetValue()
		}
	}
	return sum
}
