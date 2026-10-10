package operator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

// Bounds of an extracted skills snapshot.
const (
	maxSnapshotBytes = 64 << 20
	maxSnapshotFiles = 5000
)

var errBundleRequired = errors.New("skills bundle required")

// skillCache keeps extracted skills snapshots by hash under /work/skills.
type skillCache struct {
	dir string
	mu  sync.Mutex
}

// ensure returns the directory of the snapshot, extracting the bundle when
// the hash is new. The bundle must match the hash ("sha256:<hex>").
func (c *skillCache) ensure(hash string, bundle []byte) (string, error) {
	hexsum, ok := strings.CutPrefix(hash, "sha256:")
	if !ok || len(hexsum) != 64 || strings.Trim(hexsum, "0123456789abcdef") != "" {
		return "", fmt.Errorf("invalid skills hash %q", hash)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	dst := filepath.Join(c.dir, hexsum)
	if st, err := os.Stat(dst); err == nil && st.IsDir() {
		return dst, nil
	}
	if bundle == nil {
		return "", errBundleRequired
	}
	sum := sha256.Sum256(bundle)
	if hex.EncodeToString(sum[:]) != hexsum {
		return "", errors.New("the skills bundle does not match its hash")
	}
	if err := os.MkdirAll(c.dir, 0o700); err != nil {
		return "", err
	}
	tmp, err := os.MkdirTemp(c.dir, "tmp-")
	if err != nil {
		return "", err
	}
	if err := ExtractTarGz(bytes.NewReader(bundle), tmp, maxSnapshotBytes, maxSnapshotFiles); err != nil {
		_ = os.RemoveAll(tmp)
		return "", err
	}
	if err := os.Rename(tmp, dst); err != nil {
		_ = os.RemoveAll(tmp)
		return "", err
	}
	return dst, nil
}

// ExtractTarGz unpacks regular files and directories of a tar.gz into dst.
// Absolute paths, "..", links and special files are rejected; maxBytes and
// maxFiles bound the unpacked size.
func ExtractTarGz(r io.Reader, dst string, maxBytes int64, maxFiles int) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("skills bundle: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	var total int64
	files := 0
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("skills bundle: %w", err)
		}
		name := path.Clean(strings.TrimPrefix(h.Name, "./"))
		if name == "." {
			continue
		}
		if path.IsAbs(name) || name == ".." || strings.HasPrefix(name, "../") || strings.Contains(name, "\\") {
			return fmt.Errorf("skills bundle: unsafe path %q", h.Name)
		}
		target := filepath.Join(dst, filepath.FromSlash(name))
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o700); err != nil {
				return err
			}
		case tar.TypeReg:
			files++
			total += h.Size
			if files > maxFiles || total > maxBytes {
				return errors.New("skills bundle: too large")
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, io.LimitReader(tr, h.Size)); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("skills bundle: %q is not a regular file or directory", h.Name)
		}
	}
}

// known lists the hashes of the snapshots the cache holds.
func (c *skillCache) known() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	es, _ := os.ReadDir(c.dir)
	out := make([]string, 0, len(es))
	for _, e := range es {
		if e.IsDir() && len(e.Name()) == 64 {
			out = append(out, "sha256:"+e.Name())
		}
	}
	return out
}
