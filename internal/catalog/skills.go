package catalog

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/operator"
)

// Skills (R14, arch §8): a skill item holds a snapshot of skill directories
// (each with SKILL.md) stored in S3 by hash — uploaded by an administrator or
// synchronized from a directory of a git repository, so the skills of
// Hammurapi in /agent/skills/ of the specification repository keep their
// change process through pull requests.

var skillDir = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// Pack builds a deterministic tar.gz of the skill directories under root and
// returns it with the skill names.
func Pack(root string) ([]byte, []string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() && skillDir.MatchString(e.Name()) {
			if _, err := os.Stat(filepath.Join(root, e.Name(), "SKILL.md")); err == nil {
				names = append(names, e.Name())
			}
		}
	}
	sort.Strings(names)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.ModTime = time.Unix(0, 0)
	tw := tar.NewWriter(gz)
	for _, n := range names {
		var files []string
		_ = filepath.WalkDir(filepath.Join(root, n), func(p string, d fs.DirEntry, err error) error {
			if err == nil && d.Type().IsRegular() {
				files = append(files, p)
			}
			return nil
		})
		sort.Strings(files)
		for _, p := range files {
			rel, _ := filepath.Rel(root, p)
			b, err := os.ReadFile(p)
			if err != nil {
				return nil, nil, err
			}
			if err := tw.WriteHeader(&tar.Header{Name: filepath.ToSlash(rel), Mode: 0o644, Size: int64(len(b)), ModTime: time.Unix(0, 0), Typeflag: tar.TypeReg}); err != nil {
				return nil, nil, err
			}
			if _, err := tw.Write(b); err != nil {
				return nil, nil, err
			}
		}
	}
	if err := tw.Close(); err != nil {
		return nil, nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, nil, err
	}
	return buf.Bytes(), names, nil
}

func (s *Service) storeSnapshot(ctx context.Context, id uuid.UUID, bundle []byte, names []string) (*Item, error) {
	sum := sha256.Sum256(bundle)
	hash := "sha256:" + hex.EncodeToString(sum[:])
	if err := s.S3.Put(ctx, "skills/"+hex.EncodeToString(sum[:])+".tar.gz", bytes.NewReader(bundle), int64(len(bundle)), "application/gzip"); err != nil {
		return nil, err
	}
	nb, _ := json.Marshal(names)
	if _, err := s.Pool.Exec(ctx, `UPDATE catalog_items SET skills_snapshot = $2, skills = $3, synced_at = now(), status = 'ok', status_reason = NULL, updated_at = now() WHERE id = $1`,
		id, hash, nb); err != nil {
		return nil, err
	}
	s.changed(ctx)
	return s.Get(ctx, id)
}

// Upload stores an uploaded tar.gz of skill directories.
func (s *Service) Upload(ctx context.Context, id uuid.UUID, r io.Reader) (*Item, error) {
	it, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if it.Type != "skill" || it.Source.Kind != "upload" {
		return nil, apperr.Unprocessable("not_upload", "the item is not an uploaded skill")
	}
	tmp, err := os.MkdirTemp("", "skills-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	if err := operator.ExtractTarGz(io.LimitReader(r, 64<<20), tmp, 64<<20, 5000); err != nil {
		return nil, apperr.Unprocessable("invalid_bundle", "a tar.gz of skill directories with SKILL.md is expected: "+err.Error())
	}
	bundle, names, err := Pack(tmp)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, apperr.Unprocessable("invalid_bundle", "no skill directories with SKILL.md")
	}
	return s.storeSnapshot(ctx, id, bundle, names)
}

func repoURL(repo string) string {
	if strings.HasPrefix(repo, "https://") || strings.HasPrefix(repo, "http://") {
		return repo
	}
	return "https://github.com/" + strings.TrimSuffix(repo, ".git") + ".git"
}

// Sync clones a git source and stores the snapshot of its skills (CAT-06).
func (s *Service) Sync(ctx context.Context, id uuid.UUID) (*Item, error) {
	it, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if it.Type != "skill" || it.Source.Kind != "git" {
		return nil, apperr.Unprocessable("not_git", "the item is not a git skill source")
	}
	tmp, err := os.MkdirTemp("", "skills-git-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)
	ref := it.Source.Ref
	if ref == "" {
		ref = "main"
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(cctx, "git", "clone", "--depth", "1", "--branch", ref, "--", repoURL(it.Source.Repo), filepath.Join(tmp, "repo"))
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + tmp, "GIT_TERMINAL_PROMPT=0"}
	if s.GitToken != "" {
		// The token goes through the environment, never argv.
		basic := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + s.GitToken))
		cmd.Env = append(cmd.Env, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=http.extraHeader", "GIT_CONFIG_VALUE_0=Authorization: Basic "+basic)
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		msg := strings.TrimSpace(string(out))
		if len(msg) > 300 {
			msg = msg[len(msg)-300:]
		}
		_, _ = s.Pool.Exec(ctx, `UPDATE catalog_items SET status = 'error', status_reason = $2 WHERE id = $1`, id, "git clone: "+msg)
		return nil, apperr.Unprocessable("sync_failed", "git clone failed: "+msg)
	}
	dir := filepath.Join(tmp, "repo", filepath.FromSlash(strings.Trim(it.Source.Path, "/")))
	if rel, err := filepath.Rel(filepath.Join(tmp, "repo"), dir); err != nil || strings.HasPrefix(rel, "..") {
		return nil, apperr.Unprocessable("invalid_source", "invalid path")
	}
	bundle, names, err := Pack(dir)
	if err != nil {
		_, _ = s.Pool.Exec(ctx, `UPDATE catalog_items SET status = 'error', status_reason = $2 WHERE id = $1`, id, "no directory "+it.Source.Path)
		return nil, apperr.Unprocessable("sync_failed", "the path is not in the repository")
	}
	return s.storeSnapshot(ctx, id, bundle, names)
}

// SyncAll synchronizes every git source (on schedule).
func (s *Service) SyncAll(ctx context.Context) {
	rows, err := s.Pool.Query(ctx, `SELECT id FROM catalog_items WHERE type = 'skill' AND source->>'kind' = 'git'`)
	if err != nil {
		return
	}
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()
	for _, id := range ids {
		if _, err := s.Sync(ctx, id); err != nil {
			slog.Warn("skills sync", "item", id, "err", err)
		}
	}
}

// HookSecret is the secret of the push webhook of a git source.
func HookSecret(key []byte, id uuid.UUID) string {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("skills-hook:" + id.String()))
	return hex.EncodeToString(m.Sum(nil))[:40]
}

// VerifyHook checks a GitHub X-Hub-Signature-256 of a push to a git source.
func VerifyHook(key []byte, id uuid.UUID, body []byte, sig string) bool {
	m := hmac.New(sha256.New, []byte(HookSecret(key, id)))
	m.Write(body)
	want := "sha256=" + hex.EncodeToString(m.Sum(nil))
	return hmac.Equal([]byte(want), []byte(sig))
}
