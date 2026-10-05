// Package sandbox is the `nabu sandbox` mode: the personal space of one user
// (FTR.NAB.CMN-0001 arch §5.2, tech §7). It downloads the files of the space
// through the internal API, serves the agent's tools over the relay channel it
// opens itself, uploads changes after a quiet period and does a final upload
// before it stops. It has no S3 credentials and no inbound ports.
package sandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/relay"
	"github.com/GreenOnGrey/nabu-core/internal/workspace"
)

// Config configures a sandbox.
type Config struct {
	UserID         string
	Root           string // /work
	SandboxToken   string // sync with the api
	WorkspaceToken string // relay channel
	RelayURL       string
	APIURL         string
	Exclude        []string
	// Ephemeral sandboxes (service runs) keep nothing: no restore, no sync.
	Ephemeral   bool
	WorkspaceID string
	Quiet       time.Duration // 5 s
	Batch       int           // 50 files
	PullEvery   time.Duration
}

type entry struct {
	size  int64
	mtime time.Time
	sha   string
}

// Sandbox syncs the working copy.
type Sandbox struct {
	cfg  Config
	http *http.Client

	mu        sync.Mutex
	known     map[string]entry // what the space has, as of the last sync
	lastLocal time.Time        // last time a local change was seen
}

// New creates a sandbox.
func New(cfg Config) *Sandbox {
	if cfg.Quiet <= 0 {
		cfg.Quiet = 5 * time.Second
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 50
	}
	if cfg.PullEvery <= 0 {
		cfg.PullEvery = 15 * time.Second
	}
	return &Sandbox{cfg: cfg, http: &http.Client{Timeout: 5 * time.Minute}, known: map[string]entry{}}
}

// Excluded reports whether a path is not synced (SPC-06): directory patterns
// end with "/", others match the base name.
func Excluded(rel string, patterns []string) bool {
	rel = filepath.ToSlash(rel)
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if dir, ok := strings.CutSuffix(p, "/"); ok {
			if rel == dir || strings.HasPrefix(rel, dir+"/") || strings.Contains(rel, "/"+dir+"/") {
				return true
			}
			continue
		}
		if ok, _ := path.Match(p, path.Base(rel)); ok {
			return true
		}
	}
	return false
}

type remoteFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func (s *Sandbox) api(ctx context.Context, method, op, p string, body io.Reader, hdr map[string]string) (*http.Response, error) {
	u := s.cfg.APIURL + "/internal/v1/spaces/" + s.cfg.UserID + "/" + op
	if p != "" {
		u += "?path=" + url.QueryEscape(p)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.SandboxToken)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("%s %s: %d %s", method, op, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return resp, nil
}

func (s *Sandbox) manifest(ctx context.Context) ([]remoteFile, error) {
	resp, err := s.api(ctx, http.MethodGet, "manifest", "", nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var m []remoteFile
	return m, json.NewDecoder(resp.Body).Decode(&m)
}

func (s *Sandbox) download(ctx context.Context, f remoteFile) error {
	resp, err := s.api(ctx, http.MethodGet, "objects", f.Path, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	dst := filepath.Join(s.cfg.Root, filepath.FromSlash(f.Path))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp := dst + ".nabu-download"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	st, err := os.Stat(dst)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.known[f.Path] = entry{size: st.Size(), mtime: st.ModTime(), sha: f.SHA256}
	s.mu.Unlock()
	return nil
}

// Restore downloads every file of the manifest (SPC-02).
func (s *Sandbox) Restore(ctx context.Context) error {
	m, err := s.manifest(ctx)
	if err != nil {
		return err
	}
	for _, f := range m {
		if Excluded(f.Path, s.cfg.Exclude) {
			continue
		}
		if err := s.download(ctx, f); err != nil {
			return fmt.Errorf("restore %s: %w", f.Path, err)
		}
	}
	slog.Info("space restored", "files", len(m))
	return nil
}

// scan lists the local files with size and modification time.
func (s *Sandbox) scan() (map[string]entry, error) {
	out := map[string]entry{}
	err := filepath.WalkDir(s.cfg.Root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(s.cfg.Root, p)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if Excluded(rel, s.cfg.Exclude) || strings.HasSuffix(rel, ".nabu-download") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		out[rel] = entry{size: info.Size(), mtime: info.ModTime()}
		return nil
	})
	return out, err
}

type change struct {
	path    string
	deleted bool
}

func (s *Sandbox) changes(local map[string]entry) []change {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []change
	for p, e := range local {
		k, ok := s.known[p]
		if !ok || k.size != e.size || !k.mtime.Equal(e.mtime) {
			out = append(out, change{path: p})
		}
	}
	for p := range s.known {
		if _, ok := local[p]; !ok {
			out = append(out, change{path: p, deleted: true})
		}
	}
	return out
}

func fileSHA(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// upload sends up to Batch changes; it returns how many were sent.
func (s *Sandbox) upload(ctx context.Context, cs []change) (int, error) {
	n := 0
	for _, c := range cs {
		if n >= s.cfg.Batch {
			break
		}
		n++
		if c.deleted {
			resp, err := s.api(ctx, http.MethodDelete, "objects", c.path, nil, nil)
			if err != nil {
				return n, err
			}
			resp.Body.Close()
			s.mu.Lock()
			delete(s.known, c.path)
			s.mu.Unlock()
			continue
		}
		abs := filepath.Join(s.cfg.Root, filepath.FromSlash(c.path))
		st, err := os.Stat(abs)
		if err != nil {
			continue // removed meanwhile; the next scan sees it
		}
		sum, err := fileSHA(abs)
		if err != nil {
			continue
		}
		s.mu.Lock()
		k, ok := s.known[c.path]
		s.mu.Unlock()
		if ok && k.sha == sum {
			s.mu.Lock()
			s.known[c.path] = entry{size: st.Size(), mtime: st.ModTime(), sha: sum}
			s.mu.Unlock()
			continue
		}
		f, err := os.Open(abs)
		if err != nil {
			continue
		}
		resp, err := s.api(ctx, http.MethodPut, "objects", c.path, f, map[string]string{"X-Sha256": sum})
		f.Close()
		if err != nil {
			if strings.Contains(err.Error(), "space_quota_exceeded") {
				slog.Warn("space quota exceeded, file not uploaded", "path", c.path)
				s.mu.Lock()
				s.known[c.path] = entry{size: st.Size(), mtime: st.ModTime(), sha: sum} // do not retry until it changes
				s.mu.Unlock()
				continue
			}
			return n, err
		}
		resp.Body.Close()
		s.mu.Lock()
		s.known[c.path] = entry{size: st.Size(), mtime: st.ModTime(), sha: sum}
		s.mu.Unlock()
	}
	return n, nil
}

// Flush uploads every pending change (before stopping: tech §7).
func (s *Sandbox) Flush(ctx context.Context) error {
	for {
		local, err := s.scan()
		if err != nil {
			return err
		}
		cs := s.changes(local)
		if len(cs) == 0 {
			return nil
		}
		if _, err := s.upload(ctx, cs); err != nil {
			return err
		}
	}
}

// pull applies changes the user made on the site (SPC-09): files whose remote
// hash differs from what was synced, and files deleted remotely, unless the
// local copy changed since.
func (s *Sandbox) pull(ctx context.Context, local map[string]entry) error {
	m, err := s.manifest(ctx)
	if err != nil {
		return err
	}
	remote := map[string]remoteFile{}
	for _, f := range m {
		remote[f.Path] = f
	}
	for _, f := range m {
		if Excluded(f.Path, s.cfg.Exclude) {
			continue
		}
		s.mu.Lock()
		k, ok := s.known[f.Path]
		s.mu.Unlock()
		l, hasLocal := local[f.Path]
		if ok && k.sha == f.SHA256 {
			continue
		}
		if hasLocal && ok && (l.size != k.size || !l.mtime.Equal(k.mtime)) {
			continue // changed on both sides: the agent's copy wins and is uploaded
		}
		if err := s.download(ctx, f); err != nil {
			slog.Warn("pull", "path", f.Path, "err", err)
		}
	}
	s.mu.Lock()
	var gone []string
	for p, k := range s.known {
		if _, ok := remote[p]; !ok {
			if l, ok := local[p]; ok && l.size == k.size && l.mtime.Equal(k.mtime) {
				gone = append(gone, p)
			}
		}
	}
	for _, p := range gone {
		delete(s.known, p)
	}
	s.mu.Unlock()
	for _, p := range gone {
		_ = os.Remove(filepath.Join(s.cfg.Root, filepath.FromSlash(p)))
	}
	return nil
}

// Sync uploads changes after the quiet period and pulls remote changes until ctx ends.
func (s *Sandbox) Sync(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var pending []change
	lastPull := time.Now()
	var prevSig string
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		local, err := s.scan()
		if err != nil {
			continue
		}
		cs := s.changes(local)
		sig := signature(cs, local)
		if sig != prevSig {
			prevSig = sig
			s.lastLocal = time.Now()
		}
		pending = cs
		if len(pending) > 0 && time.Since(s.lastLocal) >= s.cfg.Quiet {
			if _, err := s.upload(ctx, pending); err != nil {
				slog.Warn("upload", "err", err)
			}
			continue
		}
		if len(pending) == 0 && time.Since(lastPull) >= s.cfg.PullEvery {
			lastPull = time.Now()
			if err := s.pull(ctx, local); err != nil {
				slog.Warn("pull", "err", err)
			}
		}
	}
}

func signature(cs []change, local map[string]entry) string {
	var b bytes.Buffer
	for _, c := range cs {
		e := local[c.path]
		fmt.Fprintf(&b, "%s|%v|%d|%d;", c.path, c.deleted, e.size, e.mtime.UnixNano())
	}
	return b.String()
}

// Run restores the space, connects to the relay and syncs until ctx ends,
// then does the final upload (60 seconds).
func Run(ctx context.Context, cfg Config) error {
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return err
	}
	s := New(cfg)
	if !cfg.Ephemeral {
		if err := s.Restore(ctx); err != nil {
			return err
		}
	}
	ws := &workspace.Workspace{Root: cfg.Root, Token: cfg.WorkspaceToken, Env: workspace.CommandEnv(cfg.Root)}
	wsID := cfg.WorkspaceID
	if wsID == "" {
		wsID = "sandbox-" + cfg.UserID
	}
	client := &relay.Client{URL: cfg.RelayURL, Token: cfg.WorkspaceToken, WorkspaceID: wsID, Kind: "sandbox",
		Handler: ws.Handler(), HandlerToken: cfg.WorkspaceToken}
	if cfg.Ephemeral {
		return client.Run(ctx)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = client.Run(ctx) }()
	go func() { defer wg.Done(); s.Sync(ctx) }()
	<-ctx.Done()
	wg.Wait()
	fctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := s.Flush(fctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("final upload failed", "err", err)
		return err
	}
	slog.Info("final upload done")
	return nil
}
