// Package space is the personal space of an agent (FTR.NAB.CMN-0001 R7, R31;
// arch §5.2; tech §3.4, §7): the files live in S3 under spaces/<userId>/ and
// are listed in space_files; a sandbox pod holds the working copy while the
// agent works and sleeps after idle. The sandbox has no S3 credentials: it
// syncs through the internal API with a token scoped to its user.
package space

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
)

// WorkspaceID is the relay id of a user's sandbox.
func WorkspaceID(uid uuid.UUID) string { return "sandbox-" + uid.String() }

// UserOfWorkspace returns the user of a sandbox workspace id.
func UserOfWorkspace(id string) (uuid.UUID, bool) {
	s, ok := strings.CutPrefix(id, "sandbox-")
	if !ok {
		return uuid.Nil, false
	}
	u, err := uuid.Parse(s)
	return u, err == nil
}

// ErrQuota is space_quota_exceeded (SPC-05).
func ErrQuota() error {
	return apperr.New(http.StatusInsufficientStorage, "space_quota_exceeded", "the space quota is exceeded")
}

// Info is GET /space.
type Info struct {
	State      string     `json:"state"`
	UsedBytes  int64      `json:"usedBytes"`
	QuotaBytes int64      `json:"quotaBytes"`
	Files      int        `json:"files"`
	LastActive *time.Time `json:"lastActivityAt"`
}

// File is a file of the space.
type File struct {
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	SHA256     string    `json:"sha256"`
	ModifiedAt time.Time `json:"modifiedAt"`
	ModifiedBy string    `json:"modifiedBy"`
}

// Service is the space store.
type Service struct {
	Pool    *pgxpool.Pool
	S3      storage.Storage
	Signer  *jwt.Signer
	Events  events.Publisher
	Quota   int64
	MaxFile int64
	// Enabled reports whether personal sandboxes run in this deployment.
	Enabled bool
}

// Key is the S3 key of a file.
func Key(uid uuid.UUID, p string) string { return "spaces/" + uid.String() + "/" + p }

// CleanPath validates a path inside the space: relative, no "..", no
// control characters.
func CleanPath(p string) (string, error) {
	p = strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))
	p = strings.TrimPrefix(p, "/")
	c := path.Clean(p)
	if p == "" || c == "." || c == ".." || strings.HasPrefix(c, "../") || len(c) > 1024 {
		return "", apperr.BadRequest("invalid_path", "invalid path")
	}
	for _, r := range c {
		if r < 0x20 {
			return "", apperr.BadRequest("invalid_path", "invalid path")
		}
	}
	return c, nil
}

// Info returns the state of the space.
func (s *Service) Info(ctx context.Context, uid uuid.UUID) (Info, error) {
	var i Info
	err := s.Pool.QueryRow(ctx, `SELECT s.state, s.used_bytes, s.quota_bytes, s.last_activity_at,
		(SELECT count(*) FROM space_files f WHERE f.user_id = s.user_id) FROM spaces s WHERE s.user_id = $1`, uid).
		Scan(&i.State, &i.UsedBytes, &i.QuotaBytes, &i.LastActive, &i.Files)
	if postgres.IsNoRows(err) {
		if _, err := s.Pool.Exec(ctx, `INSERT INTO spaces (user_id, quota_bytes) VALUES ($1,$2) ON CONFLICT DO NOTHING`, uid, s.Quota); err != nil {
			return i, err
		}
		return s.Info(ctx, uid)
	}
	return i, err
}

// Files lists files under a prefix.
func (s *Service) Files(ctx context.Context, uid uuid.UUID, prefix string, page httpx.Page) (httpx.List[File], error) {
	args := []any{uid, strings.TrimPrefix(prefix, "/"), page.Limit + 1}
	cond := ""
	if page.Cursor != nil {
		cond = ` AND path > $4`
		args = append(args, page.Cursor.ID)
	}
	rows, err := s.Pool.Query(ctx, `SELECT path, size, sha256, modified_at, modified_by FROM space_files
		WHERE user_id = $1 AND path LIKE $2 || '%'`+cond+` ORDER BY path LIMIT $3`, args...)
	if err != nil {
		return httpx.List[File]{}, err
	}
	defer rows.Close()
	var out []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.Path, &f.Size, &f.SHA256, &f.ModifiedAt, &f.ModifiedBy); err != nil {
			return httpx.List[File]{}, err
		}
		out = append(out, f)
	}
	return httpx.NewList(out, page.Limit, func(f File) (time.Time, string) { return time.Time{}, f.Path }), rows.Err()
}

// Manifest lists all files of the space.
func (s *Service) Manifest(ctx context.Context, uid uuid.UUID) ([]File, error) {
	l, err := s.Files(ctx, uid, "", httpx.Page{Limit: 100000})
	return l.Items, err
}

// Put stores a file; by is agent or user. The quota counts the new size
// against the other files (SPC-05).
func (s *Service) Put(ctx context.Context, uid uuid.UUID, p string, r io.Reader, size int64, by string) (*File, error) {
	p, err := CleanPath(p)
	if err != nil {
		return nil, err
	}
	info, err := s.Info(ctx, uid)
	if err != nil {
		return nil, err
	}
	var old int64
	_ = s.Pool.QueryRow(ctx, `SELECT size FROM space_files WHERE user_id = $1 AND path = $2`, uid, p).Scan(&old)
	if size >= 0 && info.UsedBytes-old+size > info.QuotaBytes {
		return nil, ErrQuota()
	}
	h := sha256.New()
	cr := &countingReader{r: io.TeeReader(r, h)}
	limit := info.QuotaBytes - info.UsedBytes + old + 1
	if err := s.S3.Put(ctx, Key(uid, p), io.LimitReader(cr, limit), size, "application/octet-stream"); err != nil {
		return nil, err
	}
	if cr.n >= limit {
		_ = s.S3.Delete(ctx, Key(uid, p))
		return nil, ErrQuota()
	}
	f := File{Path: p, Size: cr.n, SHA256: hex.EncodeToString(h.Sum(nil)), ModifiedBy: by, ModifiedAt: time.Now()}
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO space_files (user_id, path, size, sha256, modified_by) VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (user_id, path) DO UPDATE SET size = EXCLUDED.size, sha256 = EXCLUDED.sha256, modified_by = EXCLUDED.modified_by, modified_at = now()`,
			uid, p, f.Size, f.SHA256, by); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE spaces SET used_bytes = (SELECT COALESCE(sum(size),0) FROM space_files WHERE user_id = $1) WHERE user_id = $1`, uid)
		return err
	})
	if err != nil {
		return nil, err
	}
	s.changed(ctx, uid)
	return &f, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Open reads a file.
func (s *Service) Open(ctx context.Context, uid uuid.UUID, p string) (io.ReadCloser, *File, error) {
	p, err := CleanPath(p)
	if err != nil {
		return nil, nil, err
	}
	var f File
	if err := s.Pool.QueryRow(ctx, `SELECT path, size, sha256, modified_at, modified_by FROM space_files WHERE user_id = $1 AND path = $2`, uid, p).
		Scan(&f.Path, &f.Size, &f.SHA256, &f.ModifiedAt, &f.ModifiedBy); err != nil {
		return nil, nil, apperr.NotFound("not_found", "file not found")
	}
	rc, err := s.S3.Get(ctx, Key(uid, p))
	if err != nil {
		return nil, nil, apperr.NotFound("not_found", "file not found")
	}
	return rc, &f, nil
}

// Delete removes a file (the deletion is recorded in the manifest).
func (s *Service) Delete(ctx context.Context, uid uuid.UUID, p string) error {
	p, err := CleanPath(p)
	if err != nil {
		return err
	}
	err = postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM space_files WHERE user_id = $1 AND path = $2`, uid, p); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE spaces SET used_bytes = (SELECT COALESCE(sum(size),0) FROM space_files WHERE user_id = $1) WHERE user_id = $1`, uid)
		return err
	})
	if err != nil {
		return err
	}
	_ = s.S3.Delete(ctx, Key(uid, p))
	s.changed(ctx, uid)
	return nil
}

func (s *Service) changed(ctx context.Context, uid uuid.UUID) {
	if s.Events != nil {
		i, err := s.Info(ctx, uid)
		if err == nil {
			s.Events.Publish(ctx, events.Event{Type: events.SpaceState, UserID: &uid, Data: i})
		}
	}
}

// ─── HTTP: site (tech §3.4) ─────────────────────────────────────────

// Routes mounts /space.
func (s *Service) Routes(r chi.Router) {
	r.Get("/space", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		i, err := s.Info(r.Context(), p.UserID)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"state": i.State, "usedBytes": i.UsedBytes, "quotaBytes": i.QuotaBytes, "files": i.Files,
			"lastActivityAt": i.LastActive, "enabled": s.Enabled})
		return nil
	}))
	r.Get("/space/files", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		page, err := httpx.ParsePage(r)
		if err != nil {
			return err
		}
		l, err := s.Files(r.Context(), p.UserID, r.URL.Query().Get("prefix"), page)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, l)
		return nil
	}))
	r.Put("/space/files", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		body := http.MaxBytesReader(w, r.Body, s.MaxFile)
		f, err := s.Put(r.Context(), p.UserID, r.URL.Query().Get("path"), body, r.ContentLength, "user")
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, f)
		return nil
	}))
	r.Get("/space/files/content", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		rc, f, err := s.Open(r.Context(), p.UserID, r.URL.Query().Get("path"))
		if err != nil {
			return err
		}
		defer rc.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", path.Base(f.Path)))
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = io.Copy(w, rc)
		return nil
	}))
	r.Delete("/space/files", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		if err := s.Delete(r.Context(), p.UserID, r.URL.Query().Get("path")); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
}

// ─── HTTP: sandbox sync (tech §7) ───────────────────────────────────

// SandboxToken issues the token of a user's sandbox (prefix-scoped: SPC-03).
func (s *Service) SandboxToken(uid uuid.UUID, ttl time.Duration) string {
	return s.Signer.Issue(jwt.Claims{Audience: jwt.AudSandbox, Subject: "sandbox", User: uid.String()}, ttl)
}

// InternalRoutes mounts /internal/v1/spaces/{userId}/… for sandboxes.
func (s *Service) InternalRoutes(r chi.Router) {
	r.Route("/internal/v1/spaces/{uid}", func(r chi.Router) {
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := s.Signer.Verify(httpx.Bearer(r), jwt.AudSandbox)
				if err != nil || c.User != chi.URLParam(r, "uid") {
					httpx.Error(w, r, apperr.Forbidden("forbidden", "the sandbox token does not match the space"))
					return
				}
				next.ServeHTTP(w, r)
			})
		})
		uidOf := func(r *http.Request) uuid.UUID { u, _ := uuid.Parse(chi.URLParam(r, "uid")); return u }
		r.Get("/manifest", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
			m, err := s.Manifest(r.Context(), uidOf(r))
			if err != nil {
				return err
			}
			httpx.JSON(w, 200, m)
			return nil
		}))
		r.Get("/objects", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
			rc, _, err := s.Open(r.Context(), uidOf(r), r.URL.Query().Get("path"))
			if err != nil {
				return err
			}
			defer rc.Close()
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.Copy(w, rc)
			return nil
		}))
		r.Put("/objects", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
			f, err := s.Put(r.Context(), uidOf(r), r.URL.Query().Get("path"), r.Body, r.ContentLength, "agent")
			if err != nil {
				return err
			}
			if want := r.Header.Get("X-Sha256"); want != "" && want != f.SHA256 {
				return apperr.Unprocessable("checksum_mismatch", "the content does not match X-Sha256")
			}
			httpx.JSON(w, 200, f)
			return nil
		}))
		r.Delete("/objects", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
			if err := s.Delete(r.Context(), uidOf(r), r.URL.Query().Get("path")); err != nil {
				return err
			}
			httpx.NoContent(w)
			return nil
		}))
	})
}

// Tool is space_info of the built-in MCP.
func (s *Service) Tool() mcp.Tool {
	return mcp.Tool{Name: "space_info", ReadOnly: true,
		Description: "State of the user's personal space: whether it runs, used and quota bytes, number of files. Your file and shell tools work in this space.",
		InputSchema: mcp.Schema(map[string]any{}),
		Handler: func(ctx context.Context, g mcp.Grant, _ json.RawMessage) (string, error) {
			i, err := s.Info(ctx, g.UserID)
			if err != nil {
				return "", err
			}
			if !s.Enabled {
				return "Personal spaces are not enabled in this deployment.", nil
			}
			return fmt.Sprintf("State: %s; used %d of %d bytes; %d files.", i.State, i.UsedBytes, i.QuotaBytes, i.Files), nil
		}}
}

// WorkspaceToken is the token a user's sandbox connects to the relay with.
func (s *Service) WorkspaceToken(uid uuid.UUID, ttl time.Duration) string {
	return s.Signer.Issue(jwt.Claims{Audience: jwt.AudWorkspace, Workspace: WorkspaceID(uid), Kind: "sandbox", User: uid.String()}, ttl)
}

// RelayStore is the relay's record of connections in Postgres
// (workspace_connections); for sandboxes it also keeps the space state.
type RelayStore struct {
	Pool   *pgxpool.Pool
	Events events.Publisher
	mu     sync.Mutex
	touch  map[string]time.Time
}

// Register implements relay.Store.
func (r *RelayStore) Register(ctx context.Context, id, kind, pod string) error {
	_, err := r.Pool.Exec(ctx, `INSERT INTO workspace_connections (workspace_id, kind, relay_pod) VALUES ($1,$2,$3)
		ON CONFLICT (workspace_id) DO UPDATE SET kind = EXCLUDED.kind, relay_pod = EXCLUDED.relay_pod, connected_at = now(), last_ping_at = now()`, id, kind, pod)
	if uid, ok := UserOfWorkspace(id); ok && err == nil {
		_, err = r.Pool.Exec(ctx, `UPDATE spaces SET state = 'running', state_changed_at = now(), last_activity_at = now() WHERE user_id = $1`, uid)
		r.publish(ctx, uid, "running")
	}
	return err
}

// Unregister implements relay.Store.
func (r *RelayStore) Unregister(ctx context.Context, id, pod string) error {
	tag, err := r.Pool.Exec(ctx, `DELETE FROM workspace_connections WHERE workspace_id = $1 AND relay_pod = $2`, id, pod)
	if uid, ok := UserOfWorkspace(id); ok && err == nil && tag.RowsAffected() > 0 {
		_, _ = r.Pool.Exec(ctx, `UPDATE spaces SET state = 'stopping', state_changed_at = now() WHERE user_id = $1 AND state = 'running'`, uid)
	}
	return err
}

// Lookup implements relay.Store; connections without pings for a minute are stale.
func (r *RelayStore) Lookup(ctx context.Context, id string) (string, bool, error) {
	var pod string
	err := r.Pool.QueryRow(ctx, `SELECT relay_pod FROM workspace_connections WHERE workspace_id = $1 AND last_ping_at > now() - interval '70 seconds'`, id).Scan(&pod)
	if postgres.IsNoRows(err) {
		return "", false, nil
	}
	return pod, err == nil, err
}

// Ping implements relay.Store.
func (r *RelayStore) Ping(ctx context.Context, id, pod string) error {
	_, err := r.Pool.Exec(ctx, `UPDATE workspace_connections SET last_ping_at = now() WHERE workspace_id = $1 AND relay_pod = $2`, id, pod)
	return err
}

// Touch implements relay.Store: the activity of a sandbox, at most every 30 seconds.
func (r *RelayStore) Touch(ctx context.Context, id string) {
	uid, ok := UserOfWorkspace(id)
	if !ok {
		return
	}
	r.mu.Lock()
	if r.touch == nil {
		r.touch = map[string]time.Time{}
	}
	last := r.touch[id]
	if time.Since(last) < 30*time.Second {
		r.mu.Unlock()
		return
	}
	r.touch[id] = time.Now()
	r.mu.Unlock()
	_, _ = r.Pool.Exec(ctx, `UPDATE spaces SET last_activity_at = now() WHERE user_id = $1`, uid)
}

func (r *RelayStore) publish(ctx context.Context, uid uuid.UUID, state string) {
	if r.Events != nil {
		r.Events.Publish(ctx, events.Event{Type: events.SpaceState, UserID: &uid, Data: map[string]string{"state": state}})
	}
}
