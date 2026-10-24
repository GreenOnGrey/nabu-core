package space

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
)

// Result files of catalog MCP tools (FTR.NAB.CHAT-0003 R10, tech §4.5): the
// server uploads a temporary object, the proxy moves it where the agent
// sees it — the live sandbox through relay fs/write, else S3 of the space.

// ResultPrefix keeps the uploads of servers; Sweep removes forgotten ones.
const ResultPrefix = SnapshotPrefix + "out/"

// resultTTL bounds the link to upload a result.
const resultTTL = 10 * time.Minute

var _ catalog.ResultStore = (*Snapshots)(nil)

// Exists implements catalog.ResultStore.
func (s *Snapshots) Exists(ctx context.Context, uid uuid.UUID, p string) (bool, error) {
	p, err := CleanPath(p)
	if err != nil {
		return false, catalog.ErrResultPath
	}
	if ok, err := s.existsInSandbox(ctx, uid, p); !errors.Is(err, errNoSandbox) {
		return ok, err
	}
	var n int
	err = s.Space.Pool.QueryRow(ctx, `SELECT count(*) FROM space_files WHERE user_id = $1 AND path = $2`, uid, p).Scan(&n)
	return n > 0, err
}

func (s *Snapshots) existsInSandbox(ctx context.Context, uid uuid.UUID, p string) (bool, error) {
	if s.RelayURL == "" || s.Relay == nil {
		return false, errNoSandbox
	}
	id := WorkspaceID(uid)
	if _, ok, err := s.Relay.Lookup(ctx, id); err != nil || !ok {
		return false, errNoSandbox
	}
	cctx, cancel := context.WithTimeout(ctx, s.wait())
	defer cancel()
	var st struct {
		Exists bool `json:"exists"`
	}
	if err := s.call(cctx, id, "fs/stat", p, 64<<10, &st); err != nil {
		return false, errNoSandbox
	}
	return st.Exists, nil
}

func (s *Snapshots) wait() time.Duration {
	if s.Wait > 0 {
		return s.Wait
	}
	return sandboxWait
}

// Upload implements catalog.ResultStore.
func (s *Snapshots) Upload(ctx context.Context, name string) (string, string, error) {
	pre, ok := s.Space.S3.(storage.Presigner)
	if !ok {
		return "", "", errors.New("the object store gives no links")
	}
	key := ResultPrefix + uuid.NewString() + "/" + strings.ReplaceAll(name, "/", "_")
	u, err := pre.PresignPut(ctx, key, resultTTL)
	return key, u, err
}

// Discard implements catalog.ResultStore.
func (s *Snapshots) Discard(ctx context.Context, key string) {
	_ = s.Space.S3.Delete(ctx, key)
}

// Finish implements catalog.ResultStore.
func (s *Snapshots) Finish(ctx context.Context, uid uuid.UUID, p, key string, max int64) (int64, error) {
	defer s.Discard(ctx, key)
	p, err := CleanPath(p)
	if err != nil {
		return 0, catalog.ErrResultPath
	}
	size, err := s.size(ctx, key)
	if err != nil {
		return 0, err
	}
	if size > max {
		return 0, catalog.ErrResultTooLarge
	}
	rc, err := s.Space.S3.Get(ctx, key)
	if err != nil {
		return 0, catalog.ErrResultMissing
	}
	data, err := io.ReadAll(io.LimitReader(rc, max+1))
	rc.Close()
	if err != nil {
		return 0, err
	}
	if int64(len(data)) > max {
		return 0, catalog.ErrResultTooLarge
	}
	if err := s.writeSandbox(ctx, uid, p, data); !errors.Is(err, errNoSandbox) {
		return int64(len(data)), err
	}
	if _, err := s.Space.Put(ctx, uid, p, bytes.NewReader(data), int64(len(data)), "agent"); err != nil {
		if e, ok := apperr.As(err); ok && e.Code == "space_quota_exceeded" {
			return 0, catalog.ErrResultQuota
		}
		return 0, err
	}
	return int64(len(data)), nil
}

func (s *Snapshots) size(ctx context.Context, key string) (int64, error) {
	if st, ok := s.Space.S3.(storage.Statter); ok {
		o, err := st.Stat(ctx, key)
		if errors.Is(err, storage.ErrNotFound) {
			return 0, catalog.ErrResultMissing
		}
		return o.Size, err
	}
	// a store without Stat: the object is read (Finish limits the read anyway)
	rc, err := s.Space.S3.Get(ctx, key)
	if err != nil {
		return 0, catalog.ErrResultMissing
	}
	defer rc.Close()
	return io.Copy(io.Discard, io.LimitReader(rc, maxResultRead))
}

// maxResultRead bounds the read of a result whose size the store cannot tell.
const maxResultRead = 100 << 20

// writeSandbox writes the result into the working copy of the running
// sandbox; its sync uploads it to S3 like any file the agent writes. The
// quota is checked here as Put would.
func (s *Snapshots) writeSandbox(ctx context.Context, uid uuid.UUID, p string, data []byte) error {
	if s.RelayURL == "" || s.Relay == nil {
		return errNoSandbox
	}
	id := WorkspaceID(uid)
	if _, ok, err := s.Relay.Lookup(ctx, id); err != nil || !ok {
		return errNoSandbox
	}
	var used, quota, old int64
	if err := s.Space.Pool.QueryRow(ctx, `SELECT s.used_bytes, s.quota_bytes, coalesce((SELECT size FROM space_files f WHERE f.user_id = s.user_id AND f.path = $2), 0)
		FROM spaces s WHERE s.user_id = $1`, uid, p).Scan(&used, &quota, &old); err == nil && used-old+int64(len(data)) > quota {
		return catalog.ErrResultQuota
	}
	body, _ := json.Marshal(map[string]any{"path": p, "write": true, "content": base64.StdEncoding.EncodeToString(data)})
	cctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, s.RelayURL+"/internal/v1/workspaces/"+id+"/v1/fs/write", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.Space.Signer.Issue(jwt.Claims{Audience: jwt.AudCall, Workspace: id}, time.Minute))
	c := s.HTTP
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return errNoSandbox // the sandbox went away: the file goes to S3
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return errNoSandbox
	}
	return nil
}
