package workspace

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

// Workspace serves the working copy of a task to the agent operator
// (FTR.HMR.CMN-0004 arch §4.2): file operations, search and shell commands, all
// confined to the checkout. The hammurapi-workspace extension of Pi calls it
// in place of the built-in tools, so the model works with Pi's own read,
// write, edit, bash, grep, find and ls.
type Workspace struct {
	Root  string
	Token string
	// Env is the environment of shell commands: never the runner's own
	// (it holds the task token).
	Env []string

	run    sync.Mutex // one bash at a time; the others wait
	mu     sync.Mutex
	cancel context.CancelFunc
}

// Bounds of shell commands (tech spec §6).
const (
	bashDefaultTimeout = 300 * time.Second
	bashMaxTimeout     = 1800 * time.Second
	bashMaxOutput      = 16 << 20 // streamed; Pi keeps the tail and the full log on its side
	readMaxBytes       = 64 << 20
	grepMaxFiles       = 20000
)

// ErrOutside is a path outside the working copy (RUN-04).
var ErrOutside = errors.New("path_outside_workspace")

// Resolve maps a path relative to the checkout (or "/"-rooted inside it) to
// an absolute path, refusing ".." escapes and symlinks that lead outside —
// also for files that do not exist yet.
func (w *Workspace) Resolve(p string) (string, error) {
	root, err := filepath.Abs(w.Root)
	if err != nil {
		return "", err
	}
	p = strings.TrimPrefix(filepath.ToSlash(p), "/")
	clean := filepath.Clean(filepath.Join(root, filepath.FromSlash(p)))
	if !inside(root, clean) {
		return "", ErrOutside
	}
	rr, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	if !inside(rr, realPath(clean)) {
		return "", ErrOutside
	}
	return clean, nil
}

func inside(root, p string) bool {
	rel, err := filepath.Rel(root, p)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// realPath resolves symlinks in the longest existing prefix of p.
func realPath(p string) string {
	rest := ""
	for cur := p; ; {
		if real, err := filepath.EvalSymlinks(cur); err == nil {
			return filepath.Join(real, rest)
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return p
		}
		rest = filepath.Join(filepath.Base(cur), rest)
		cur = parent
	}
}

// Handler serves /v1.
func (w *Workspace) Handler() http.Handler {
	mux := http.NewServeMux()
	for route, fn := range map[string]func(http.ResponseWriter, *http.Request) error{
		"/v1/fs/read":    w.read,
		"/v1/fs/access":  w.access,
		"/v1/fs/stat":    w.stat,
		"/v1/fs/readdir": w.readdir,
		"/v1/fs/write":   w.write,
		"/v1/fs/mkdir":   w.mkdir,
		"/v1/fs/glob":    w.glob,
		"/v1/grep":       w.grep,
		"/v1/exec":       w.exec,
		"/v1/abort":      w.abort,
	} {
		fn := fn
		mux.HandleFunc("POST "+route, func(rw http.ResponseWriter, r *http.Request) {
			if subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")), []byte(w.Token)) != 1 {
				wsErr(rw, http.StatusUnauthorized, "unauthorized", "invalid workspace token")
				return
			}
			if err := fn(rw, r); err != nil {
				switch {
				case errors.Is(err, ErrOutside):
					wsErr(rw, http.StatusForbidden, "path_outside_workspace", "the path is outside the working copy")
				case errors.Is(err, fs.ErrNotExist):
					wsErr(rw, http.StatusNotFound, "not_found", err.Error())
				case errors.Is(err, fs.ErrPermission):
					wsErr(rw, http.StatusForbidden, "permission_denied", err.Error())
				default:
					wsErr(rw, http.StatusUnprocessableEntity, "failed", err.Error())
				}
			}
		})
	}
	return mux
}

// Serve listens on addr until ctx ends.
func (w *Workspace) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{Handler: w.Handler(), ReadHeaderTimeout: 15 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	err := srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func wsErr(w http.ResponseWriter, code int, errCode, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": errCode, "message": msg})
}

func wsJSON(w http.ResponseWriter, v any) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(v)
}

type pathReq struct {
	Path    string `json:"path"`
	Write   bool   `json:"write,omitempty"`
	Content string `json:"content,omitempty"` // base64
}

func decodeReq(r *http.Request, v any) error {
	return json.NewDecoder(io.LimitReader(r.Body, readMaxBytes+(1<<20))).Decode(v)
}

func (w *Workspace) resolveReq(r *http.Request) (pathReq, string, error) {
	var req pathReq
	if err := decodeReq(r, &req); err != nil {
		return req, "", err
	}
	p, err := w.Resolve(req.Path)
	return req, p, err
}

func (w *Workspace) read(rw http.ResponseWriter, r *http.Request) error {
	_, p, err := w.resolveReq(r)
	if err != nil {
		return err
	}
	st, err := os.Stat(p)
	if err != nil {
		return err
	}
	if st.IsDir() {
		return fmt.Errorf("%s is a directory", r.URL.Path)
	}
	if st.Size() > readMaxBytes {
		return fmt.Errorf("the file is larger than %d MB", readMaxBytes>>20)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	return wsJSON(rw, map[string]string{"data": base64.StdEncoding.EncodeToString(b)})
}

func (w *Workspace) access(rw http.ResponseWriter, r *http.Request) error {
	req, p, err := w.resolveReq(r)
	if err != nil {
		return err
	}
	flag := os.O_RDONLY
	if req.Write {
		flag = os.O_RDWR
	}
	f, err := os.OpenFile(p, flag, 0)
	if err != nil {
		return err
	}
	_ = f.Close()
	rw.WriteHeader(http.StatusNoContent)
	return nil
}

func (w *Workspace) stat(rw http.ResponseWriter, r *http.Request) error {
	_, p, err := w.resolveReq(r)
	if err != nil {
		return err
	}
	st, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return wsJSON(rw, map[string]any{"exists": false})
	}
	if err != nil {
		return err
	}
	return wsJSON(rw, map[string]any{"exists": true, "isDir": st.IsDir(), "size": st.Size()})
}

func (w *Workspace) readdir(rw http.ResponseWriter, r *http.Request) error {
	_, p, err := w.resolveReq(r)
	if err != nil {
		return err
	}
	ents, err := os.ReadDir(p)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return wsJSON(rw, map[string]any{"entries": names})
}

func (w *Workspace) write(rw http.ResponseWriter, r *http.Request) error {
	req, p, err := w.resolveReq(r)
	if err != nil {
		return err
	}
	b, err := base64.StdEncoding.DecodeString(req.Content)
	if err != nil {
		return errors.New("content must be base64")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(p, b, 0o644); err != nil {
		return err
	}
	rw.WriteHeader(http.StatusNoContent)
	return nil
}

func (w *Workspace) mkdir(rw http.ResponseWriter, r *http.Request) error {
	_, p, err := w.resolveReq(r)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(p, 0o755); err != nil {
		return err
	}
	rw.WriteHeader(http.StatusNoContent)
	return nil
}

// skipDirs are never searched (the agent can still read them explicitly).
var searchSkipDirs = map[string]bool{".git": true, "node_modules": true}

// walk visits the files under dir with their slash paths relative to dir.
func walk(dir string, visit func(rel, abs string) bool) error {
	return filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if p != dir && searchSkipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		if !visit(filepath.ToSlash(rel), p) {
			return filepath.SkipAll
		}
		return nil
	})
}

// globToRegexp supports **, *, ? and {a,b} as fd and ripgrep do.
func globToRegexp(glob string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	if !strings.Contains(glob, "/") {
		b.WriteString("(?:.*/)?") // a bare pattern matches at any depth
	}
	for i := 0; i < len(glob); i++ {
		c := glob[i]
		switch c {
		case '*':
			if i+1 < len(glob) && glob[i+1] == '*' {
				b.WriteString(".*")
				i++
				if i+1 < len(glob) && glob[i+1] == '/' {
					i++
					b.WriteString("/?")
				}
			} else {
				b.WriteString("[^/]*")
			}
		case '?':
			b.WriteString("[^/]")
		case '{':
			b.WriteString("(?:")
		case '}':
			b.WriteString(")")
		case ',':
			b.WriteString("|")
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}

func (w *Workspace) glob(rw http.ResponseWriter, r *http.Request) error {
	var req struct {
		Pattern string   `json:"pattern"`
		Path    string   `json:"path"`
		Ignore  []string `json:"ignore"`
		Limit   int      `json:"limit"`
	}
	if err := decodeReq(r, &req); err != nil {
		return err
	}
	dir, err := w.Resolve(req.Path)
	if err != nil {
		return err
	}
	re, err := globToRegexp(req.Pattern)
	if err != nil {
		return fmt.Errorf("invalid pattern: %w", err)
	}
	var ignores []*regexp.Regexp
	for _, ig := range req.Ignore {
		if x, err := globToRegexp(ig); err == nil {
			ignores = append(ignores, x)
		}
	}
	limit := req.Limit
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
	out := []string{}
	err = walk(dir, func(rel, _ string) bool {
		for _, ig := range ignores {
			if ig.MatchString(rel) {
				return true
			}
		}
		if re.MatchString(rel) {
			out = append(out, rel)
		}
		return len(out) < limit
	})
	if err != nil {
		return err
	}
	sort.Strings(out)
	return wsJSON(rw, map[string]any{"paths": out})
}

// grep searches like Pi's grep tool and returns its output lines
// ("path:line: text" for matches, "path-line- text" for context).
func (w *Workspace) grep(rw http.ResponseWriter, r *http.Request) error {
	var req struct {
		Pattern    string `json:"pattern"`
		Path       string `json:"path"`
		Glob       string `json:"glob"`
		IgnoreCase bool   `json:"ignoreCase"`
		Literal    bool   `json:"literal"`
		Context    int    `json:"context"`
		Limit      int    `json:"limit"`
	}
	if err := decodeReq(r, &req); err != nil {
		return err
	}
	root, err := w.Resolve(req.Path)
	if err != nil {
		return err
	}
	st, err := os.Stat(root)
	if err != nil {
		return err
	}
	pat := req.Pattern
	if req.Literal {
		pat = regexp.QuoteMeta(pat)
	}
	if req.IgnoreCase {
		pat = "(?i)" + pat
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		return fmt.Errorf("invalid pattern: %w", err)
	}
	var gl *regexp.Regexp
	if req.Glob != "" {
		if gl, err = globToRegexp(req.Glob); err != nil {
			return fmt.Errorf("invalid glob: %w", err)
		}
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	var lines []string
	matches, files := 0, 0
	limited, truncatedLines := false, false
	search := func(rel, abs string) bool {
		files++
		if files > grepMaxFiles {
			return false
		}
		if gl != nil && !gl.MatchString(rel) {
			return true
		}
		b, err := os.ReadFile(abs)
		if err != nil || isBinary(b) {
			return true
		}
		text := strings.ReplaceAll(strings.ReplaceAll(string(b), "\r\n", "\n"), "\r", "\n")
		all := strings.Split(text, "\n")
		for i, l := range all {
			if !re.MatchString(l) {
				continue
			}
			matches++
			from, to := max(0, i-req.Context), min(len(all)-1, i+req.Context)
			for j := from; j <= to; j++ {
				sep := "-"
				if j == i {
					sep = ":"
				}
				line := all[j]
				if len(line) > 500 {
					line, truncatedLines = line[:500]+"…", true
				}
				lines = append(lines, fmt.Sprintf("%s%s%d%s %s", rel, sep, j+1, sep, line))
			}
			if matches >= limit {
				limited = true
				return false
			}
		}
		return true
	}
	if st.IsDir() {
		if err := walk(root, search); err != nil {
			return err
		}
	} else {
		search(path.Base(filepath.ToSlash(root)), root)
	}
	if lines == nil {
		lines = []string{}
	}
	return wsJSON(rw, map[string]any{"lines": lines, "matchCount": matches, "matchLimitReached": limited, "linesTruncated": truncatedLines})
}

func isBinary(b []byte) bool {
	n := len(b)
	if n > 8000 {
		n = 8000
	}
	for _, c := range b[:n] {
		if c == 0 {
			return true
		}
	}
	return false
}

// exec runs a shell command in the working copy, streaming its output as
// NDJSON: {"data": base64}… then {"exitCode": n, "timedOut": bool}.
func (w *Workspace) exec(rw http.ResponseWriter, r *http.Request) error {
	var req struct {
		Command    string `json:"command"`
		Cwd        string `json:"cwd"`
		TimeoutSec int    `json:"timeoutSec"`
	}
	if err := decodeReq(r, &req); err != nil {
		return err
	}
	cwd, err := w.Resolve(req.Cwd)
	if err != nil {
		return err
	}
	timeout := bashDefaultTimeout
	if req.TimeoutSec > 0 {
		timeout = time.Duration(req.TimeoutSec) * time.Second
	}
	if timeout > bashMaxTimeout {
		timeout = bashMaxTimeout
	}
	w.run.Lock() // one active bash per session; the others wait
	defer w.run.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	w.mu.Lock()
	w.cancel = cancel
	w.mu.Unlock()
	shell, flag := "bash", "-c"
	if runtime.GOOS == "windows" {
		shell, flag = "cmd", "/C"
	}
	cmd := exec.CommandContext(ctx, shell, flag, req.Command)
	cmd.Dir = cwd
	cmd.Env = w.Env
	cmd.WaitDelay = 5 * time.Second
	pr, pw := io.Pipe()
	cmd.Stdout, cmd.Stderr = pw, pw
	rw.Header().Set("Content-Type", "application/x-ndjson")
	rw.WriteHeader(http.StatusOK)
	fl, _ := rw.(http.Flusher)
	enc := json.NewEncoder(rw)
	if err := cmd.Start(); err != nil {
		_ = enc.Encode(map[string]any{"data": base64.StdEncoding.EncodeToString([]byte(err.Error() + "\n")), "exitCode": 127})
		return nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		br := bufio.NewReaderSize(pr, 32<<10)
		buf := make([]byte, 32<<10)
		sent := 0
		for {
			n, err := br.Read(buf)
			if n > 0 && sent < bashMaxOutput {
				_ = enc.Encode(map[string]string{"data": base64.StdEncoding.EncodeToString(buf[:n])})
				sent += n
				if fl != nil {
					fl.Flush()
				}
			}
			if err != nil {
				return
			}
		}
	}()
	werr := cmd.Wait()
	_ = pw.Close()
	<-done
	code := 0
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
	}
	var ee *exec.ExitError
	if werr != nil && !errors.As(werr, &ee) && code == 0 {
		code = 1
	}
	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	aborted := errors.Is(ctx.Err(), context.Canceled)
	_ = enc.Encode(map[string]any{"exitCode": code, "timedOut": timedOut, "aborted": aborted, "timeoutSec": int(timeout.Seconds())})
	return nil
}

func (w *Workspace) abort(rw http.ResponseWriter, _ *http.Request) error {
	w.mu.Lock()
	if w.cancel != nil {
		w.cancel()
	}
	w.mu.Unlock()
	rw.WriteHeader(http.StatusNoContent)
	return nil
}

// CommandEnv is the environment of the agent's commands: a clean set, not
// the runner's (which holds the task token).
func CommandEnv(root string) []string {
	env := []string{"HOME=" + root, "CI=true", "LANG=C.UTF-8"}
	for _, k := range []string{"PATH", "GOPATH", "GOCACHE", "GOMODCACHE", "JAVA_HOME", "NODE_PATH", "TMPDIR", "SystemRoot", "ComSpec", "TEMP", "TMP"} {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	return env
}
