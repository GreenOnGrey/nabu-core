package space

// Copies of files of a space for file arguments of catalog MCP servers
// (FTR.NAB.CMN-0006 R14, arch §4.2, tech §3.6): the server gets a link to an
// unchanging copy of the current version of the file, not to the file of the
// space, which lags behind the sandbox by its sync.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
)

const (
	// SnapshotPrefix holds the copies; they are removed after SnapshotKeep.
	SnapshotPrefix = "mcp-files/"
	SnapshotKeep   = time.Hour
	// SnapshotTTL is how long the link to a copy is valid.
	SnapshotTTL = 5 * time.Minute
	// sandboxWait bounds the read from a running sandbox; after it the copy
	// is taken from S3 (FIL-03).
	sandboxWait = 10 * time.Second
)

// Snapshots copies files of spaces for the MCP proxy.
type Snapshots struct {
	Space *Service
	Relay *RelayStore
	// RelayURL is the internal API of the relay (RELAY_INTERNAL_URL); empty —
	// files are taken from S3 only.
	RelayURL string
	HTTP     *http.Client
	// Wait bounds the read from a running sandbox; zero — sandboxWait.
	Wait time.Duration
}

// Snapshot implements catalog.FileSnapshot.
func (s *Snapshots) Snapshot(ctx context.Context, uid uuid.UUID, p string, max int64) (string, error) {
	p, err := CleanPath(p)
	if err != nil {
		return "", catalog.ErrFileInvalid
	}
	pre, ok := s.Space.S3.(storage.Presigner)
	if !ok {
		return "", errors.New("the object store gives no links")
	}
	data, err := s.read(ctx, uid, p, max)
	if err != nil {
		return "", err
	}
	key := SnapshotPrefix + uuid.NewString() + "/" + path.Base(p)
	if err := s.Space.S3.Put(ctx, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
		return "", err
	}
	return pre.PresignGet(ctx, key, SnapshotTTL)
}

// Read reads the current version of a file of the space: the working copy of
// the running sandbox, or S3 when the sandbox does not answer
// (FTR.NAB.CHAT-0001 R33: a file the agent has just written).
func (s *Snapshots) Read(ctx context.Context, uid uuid.UUID, p string, max int64) ([]byte, error) {
	p, err := CleanPath(p)
	if err != nil {
		return nil, catalog.ErrFileInvalid
	}
	return s.read(ctx, uid, p, max)
}

func (s *Snapshots) read(ctx context.Context, uid uuid.UUID, p string, max int64) ([]byte, error) {
	data, err := s.fromSandbox(ctx, uid, p, max)
	if errors.Is(err, errNoSandbox) {
		data, err = s.fromSpace(ctx, uid, p, max)
	}
	return data, err
}

var errNoSandbox = errors.New("the sandbox does not answer")

// fromSandbox reads the working copy through the relay: fs/stat, then fs/read.
func (s *Snapshots) fromSandbox(ctx context.Context, uid uuid.UUID, p string, max int64) ([]byte, error) {
	if s.RelayURL == "" || s.Relay == nil {
		return nil, errNoSandbox
	}
	id := WorkspaceID(uid)
	if _, ok, err := s.Relay.Lookup(ctx, id); err != nil || !ok {
		return nil, errNoSandbox
	}
	wait := s.Wait
	if wait == 0 {
		wait = sandboxWait
	}
	cctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	var st struct {
		Exists bool  `json:"exists"`
		IsDir  bool  `json:"isDir"`
		Size   int64 `json:"size"`
	}
	if err := s.call(cctx, id, "fs/stat", p, 64<<10, &st); err != nil {
		slog.WarnContext(ctx, "snapshot: sandbox stat", "err", err)
		return nil, errNoSandbox
	}
	if !st.Exists || st.IsDir {
		return nil, catalog.ErrFileNotFound
	}
	if st.Size > max {
		return nil, catalog.ErrFileTooLarge
	}
	var rd struct {
		Data string `json:"data"`
	}
	// fs/read answers {"data": base64}: the answer is read no further than
	// the base64 of max bytes, so a file grown since fs/stat is not read whole
	if err := s.call(cctx, id, "fs/read", p, base64.StdEncoding.EncodedLen(int(max))+1024, &rd); err != nil {
		if errors.Is(err, errAnswerTooLarge) {
			return nil, catalog.ErrFileTooLarge
		}
		slog.WarnContext(ctx, "snapshot: sandbox read", "err", err)
		return nil, errNoSandbox
	}
	b, err := base64.StdEncoding.DecodeString(rd.Data)
	if err != nil {
		return nil, errNoSandbox
	}
	if int64(len(b)) > max {
		return nil, catalog.ErrFileTooLarge
	}
	return b, nil
}

var errAnswerTooLarge = errors.New("the answer of the workspace is too large")

// call performs one operation of the workspace through the relay; the answer
// is read up to limit bytes.
func (s *Snapshots) call(ctx context.Context, id, op, p string, limit int, out any) error {
	body, _ := json.Marshal(map[string]string{"path": p})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.RelayURL+"/internal/v1/workspaces/"+id+"/v1/"+op, bytes.NewReader(body))
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
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
	if err != nil {
		return err
	}
	if len(raw) > limit {
		return errAnswerTooLarge
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: HTTP %d: %s", op, resp.StatusCode, strings.TrimSpace(string(raw[:min(len(raw), 200)])))
	}
	return json.Unmarshal(raw, out)
}

// fromSpace reads the file of the space in S3.
func (s *Snapshots) fromSpace(ctx context.Context, uid uuid.UUID, p string, max int64) ([]byte, error) {
	rc, f, err := s.Space.Open(ctx, uid, p)
	if err != nil {
		return nil, catalog.ErrFileNotFound
	}
	defer rc.Close()
	if f.Size > max {
		return nil, catalog.ErrFileTooLarge
	}
	b, err := io.ReadAll(io.LimitReader(rc, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, catalog.ErrFileTooLarge
	}
	return b, nil
}

// Sweep removes copies older than SnapshotKeep (FIL-08).
func (s *Snapshots) Sweep(ctx context.Context) (int, error) {
	objs, err := s.Space.S3.List(ctx, SnapshotPrefix)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, o := range objs {
		if time.Since(o.LastModified) < SnapshotKeep {
			continue
		}
		if err := s.Space.S3.Delete(ctx, o.Key); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
