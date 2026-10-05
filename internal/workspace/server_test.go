package workspace

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func wsServer(t *testing.T) (*Workspace, *httptest.Server, string) {
	t.Helper()
	root := t.TempDir()
	ws := &Workspace{Root: root, Token: "ws-token", Env: CommandEnv(root)}
	srv := httptest.NewServer(ws.Handler())
	t.Cleanup(srv.Close)
	return ws, srv, root
}

func call(t *testing.T, srv *httptest.Server, path, tok string, body any) (*http.Response, []byte) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out bytes.Buffer
	_, _ = out.ReadFrom(resp.Body)
	return resp, out.Bytes()
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func TestWorkspaceFiles(t *testing.T) {
	_, srv, root := wsServer(t)
	if r, _ := call(t, srv, "/v1/fs/write", "wrong", map[string]string{"path": "a.txt", "content": b64("x")}); r.StatusCode != 401 {
		t.Fatalf("wrong token: %d", r.StatusCode)
	}
	if r, b := call(t, srv, "/v1/fs/write", "ws-token", map[string]string{"path": "src/pkg/main.go", "content": b64("package main\n// TODO fix\nfunc main() {}\n")}); r.StatusCode != 204 {
		t.Fatalf("write %d %s", r.StatusCode, b)
	}
	if got, _ := os.ReadFile(filepath.Join(root, "src", "pkg", "main.go")); !strings.Contains(string(got), "TODO") {
		t.Fatal("file not written")
	}
	_, b := call(t, srv, "/v1/fs/read", "ws-token", map[string]string{"path": "/src/pkg/main.go"})
	var rd struct{ Data string }
	_ = json.Unmarshal(b, &rd)
	if data, _ := base64.StdEncoding.DecodeString(rd.Data); !strings.HasPrefix(string(data), "package main") {
		t.Fatalf("read %s", b)
	}
	_, b = call(t, srv, "/v1/fs/stat", "ws-token", map[string]string{"path": "src"})
	if !strings.Contains(string(b), `"isDir":true`) {
		t.Fatalf("stat %s", b)
	}
	_, b = call(t, srv, "/v1/fs/glob", "ws-token", map[string]any{"pattern": "**/*.go", "path": ""})
	if !strings.Contains(string(b), "src/pkg/main.go") {
		t.Fatalf("glob %s", b)
	}
	_, b = call(t, srv, "/v1/grep", "ws-token", map[string]any{"pattern": "todo", "path": "", "ignoreCase": true, "context": 1})
	if !strings.Contains(string(b), `src/pkg/main.go:2: // TODO fix`) || !strings.Contains(string(b), `src/pkg/main.go-1- package main`) {
		t.Fatalf("grep %s", b)
	}
	if r, _ := call(t, srv, "/v1/fs/read", "ws-token", map[string]string{"path": "missing.txt"}); r.StatusCode != 404 {
		t.Fatalf("missing file: %d", r.StatusCode)
	}
}

// RUN-04: paths outside the working copy, also through a symlink.
func TestWorkspacePathEscape(t *testing.T) {
	ws, srv, root := wsServer(t)
	for _, p := range []string{"../../etc/passwd", "src/../../x", "/../outside"} {
		if r, b := call(t, srv, "/v1/fs/read", "ws-token", map[string]string{"path": p}); r.StatusCode != 403 || !strings.Contains(string(b), "path_outside_workspace") {
			t.Fatalf("%s: %d %s", p, r.StatusCode, b)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("no symlinks here: %v", err)
	}
	for _, p := range []string{"link/secret", "link/new/deep/file"} {
		if _, err := ws.Resolve(p); err == nil {
			t.Fatalf("symlink escape accepted: %s", p)
		}
		if r, _ := call(t, srv, "/v1/fs/write", "ws-token", map[string]string{"path": p, "content": b64("x")}); r.StatusCode != 403 {
			t.Fatalf("write through link: %d", r.StatusCode)
		}
	}
}

func execLines(t *testing.T, srv *httptest.Server, body map[string]any) (string, map[string]any) {
	t.Helper()
	bodyJSON, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/exec", bytes.NewReader(bodyJSON))
	req.Header.Set("Authorization", "Bearer ws-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out strings.Builder
	var final map[string]any
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var rec map[string]any
		_ = json.Unmarshal(sc.Bytes(), &rec)
		if d, ok := rec["data"].(string); ok {
			b, _ := base64.StdEncoding.DecodeString(d)
			out.Write(b)
		}
		if _, ok := rec["exitCode"]; ok {
			final = rec
		}
	}
	return out.String(), final
}

// RUN-05 and the clean environment of commands: the runner's task token
// never reaches the agent's shell.
func TestWorkspaceExec(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bash commands are tested on Linux")
	}
	_, srv, _ := wsServer(t)
	t.Setenv("HAMMURAPI_TASK_TOKEN", "task-secret")
	out, fin := execLines(t, srv, map[string]any{"command": "echo hello; env; exit 3", "cwd": ""})
	if !strings.Contains(out, "hello") || fin["exitCode"].(float64) != 3 {
		t.Fatalf("%q %v", out, fin)
	}
	if strings.Contains(out, "task-secret") {
		t.Fatal("the runner environment leaked into the command")
	}
	out, fin = execLines(t, srv, map[string]any{"command": "echo start; sleep 5", "cwd": "", "timeoutSec": 1})
	if fin["timedOut"] != true || !strings.Contains(out, "start") {
		t.Fatalf("timeout: %q %v", out, fin)
	}
	out, _ = execLines(t, srv, map[string]any{"command": "head -c 200000 /dev/zero | tr '\\0' a", "cwd": ""})
	if len(out) != 200000 {
		t.Fatalf("streamed %d bytes", len(out))
	}
}

func TestGlobToRegexp(t *testing.T) {
	cases := map[string][]string{
		"**/*.go":           {"main.go", "a/b/c.go"},
		"*.md":              {"README.md", "docs/a.md"},
		"src/**/*.{ts,tsx}": {"src/a.ts", "src/x/y.tsx"},
	}
	for g, ok := range cases {
		re, err := globToRegexp(g)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range ok {
			if !re.MatchString(p) {
				t.Errorf("%s should match %s", g, p)
			}
		}
	}
	re, _ := globToRegexp("src/*.go")
	if re.MatchString("src/a/b.go") || re.MatchString("main.go") {
		t.Error("src/*.go is not recursive")
	}
}
