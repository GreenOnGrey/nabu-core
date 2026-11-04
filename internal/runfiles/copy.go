package runfiles

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// Entry is a file of the working copy as the sandbox unpacked it.
type Entry struct {
	Size int64       `json:"size"`
	SHA  string      `json:"sha256"`
	Mode fs.FileMode `json:"mode"`
	Link string      `json:"link,omitempty"`
}

// Manifest is the working copy by path (tech §2.5).
type Manifest map[string]Entry

// has reports a file of the copy under the directory dir.
func (m Manifest) has(dir string) bool {
	for p := range m {
		if strings.HasPrefix(p, dir+"/") {
			return true
		}
	}
	return false
}

// Extract unpacks a working copy into root and returns its manifest. Modes
// keep the permission bits only: no setuid, setgid or sticky.
func Extract(r io.Reader, root string) (Manifest, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("not gzip: %w", err)
	}
	tr := tar.NewReader(gz)
	m := Manifest{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return m, nil
		}
		if err != nil {
			return nil, fmt.Errorf("not tar: %w", err)
		}
		p, err := CleanPath(h.Name)
		if err != nil || p == "" {
			if err != nil {
				return nil, err
			}
			continue
		}
		full := filepath.Join(root, filepath.FromSlash(p))
		mode := fs.FileMode(h.Mode).Perm()
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(full, 0o755); err != nil {
				return nil, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				return nil, err
			}
			f, err := os.OpenFile(full, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode|0o200)
			if err != nil {
				return nil, err
			}
			sum := sha256.New()
			n, err := io.Copy(io.MultiWriter(f, sum), tr)
			if cerr := f.Close(); err == nil {
				err = cerr
			}
			if err != nil {
				return nil, err
			}
			m[p] = Entry{Size: n, SHA: hex.EncodeToString(sum.Sum(nil)), Mode: mode | 0o200}
		case tar.TypeSymlink:
			if strings.HasPrefix(h.Linkname, "/") {
				return nil, fmt.Errorf("symlink %q points outside", p)
			}
			if _, err := CleanPath(path.Join(path.Dir(p), h.Linkname)); err != nil {
				return nil, fmt.Errorf("symlink %q points outside", p)
			}
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				return nil, err
			}
			_ = os.Remove(full)
			if err := os.Symlink(h.Linkname, full); err != nil {
				return nil, err
			}
			m[p] = Entry{Link: h.Linkname}
		default:
			return nil, fmt.Errorf("%q: only files, directories and symlinks", p)
		}
	}
}

// Change is one changed path (changes.json).
type Change struct {
	Path string `json:"path"`
	Op   string `json:"op"` // added | modified | deleted
	Size int64  `json:"size,omitempty"`
}

// Skipped is a path left out of the changes with the reason.
type Skipped struct {
	Path   string `json:"path"`
	Reason string `json:"reason"` // too_large | symlink_outside | unreadable
}

// Changes is changes.json.
type Changes struct {
	Base    Stats     `json:"base"`
	Changes []Change  `json:"changes"`
	Skipped []Skipped `json:"skipped"`
}

// Manifest file name of the archive of changes.
const ChangesFile = "changes.json"

// MaxNewFile bounds a file the agent created (tech §2.5: too_large).
const MaxNewFile = 10 << 20

// alwaysSkipped are directories of dependencies and caches that never go
// into the changes, whatever .gitignore says (tech §2.5).
var alwaysSkipped = map[string]bool{"node_modules": true, ".venv": true, "venv": true, "__pycache__": true,
	".pytest_cache": true, ".mypy_cache": true, ".ruff_cache": true, ".pnpm-store": true, ".cache": true, ".npm": true}

// homePaths are the caches of Go in HOME=/work (GOPATH=/work/go) and the
// service directory of the sandbox: exact paths from the root, so the
// other files of a go/ directory of the repository are still changes.
// XDG directories point into /work/.cache (Dockerfile).
var homePaths = map[string]bool{"go/pkg": true, "go/bin": true, "nabu": true}

// Collect compares root with the manifest and writes the archive of changes
// to w: changes.json first, then the added and modified files.
func Collect(root string, m Manifest, w io.Writer) (*Changes, error) {
	out, files, err := diff(root, m)
	if err != nil {
		return nil, err
	}
	return out, write(root, out, files, w)
}

// HasChanges tells whether Collect would find changes (FTR.HMR.CMN-0008 R7).
func HasChanges(root string, m Manifest) (bool, error) {
	out, _, err := diff(root, m)
	if err != nil {
		return false, err
	}
	return len(out.Changes) > 0, nil
}

// diff compares root with the manifest.
func diff(root string, m Manifest) (*Changes, []string, error) {
	out := &Changes{Changes: []Change{}, Skipped: []Skipped{}}
	for _, e := range m {
		out.Base.Files++
		out.Base.Bytes += e.Size
	}
	ign := NewIgnore(root)
	seen := map[string]bool{}
	var files []string
	// skipped dirs are walked only for files of the copy: a tracked file
	// in an ignored directory is still a change.
	var walk func(dir string, untrackedOff bool) error
	walk = func(dir string, untrackedOff bool) error {
		ents, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(dir)))
		if err != nil {
			return err
		}
		for _, d := range ents {
			rel := d.Name()
			if dir != "" {
				rel = dir + "/" + d.Name()
			}
			if rel == ".git" {
				continue
			}
			info, err := os.Lstat(filepath.Join(root, filepath.FromSlash(rel)))
			if err != nil {
				out.Skipped = append(out.Skipped, Skipped{Path: rel, Reason: "unreadable"})
				continue
			}
			if info.IsDir() {
				off := untrackedOff || alwaysSkipped[d.Name()] || homePaths[rel] || ign.Ignored(rel, true)
				if off && !m.has(rel) {
					continue
				}
				if err := walk(rel, off); err != nil {
					return err
				}
				continue
			}
			_, tracked := m[rel]
			if !tracked && (untrackedOff || ign.Ignored(rel, false)) {
				continue
			}
			seen[rel] = true
			ch, skip, err := compare(root, rel, info, m)
			if err != nil {
				return err
			}
			if skip != nil {
				out.Skipped = append(out.Skipped, *skip)
				continue
			}
			if ch != nil {
				out.Changes = append(out.Changes, *ch)
				files = append(files, rel)
			}
		}
		return nil
	}
	if err := walk("", false); err != nil {
		return nil, nil, err
	}
	for p := range m {
		if !seen[p] {
			out.Changes = append(out.Changes, Change{Path: p, Op: "deleted"})
		}
	}
	sort.Slice(out.Changes, func(i, j int) bool { return out.Changes[i].Path < out.Changes[j].Path })
	return out, files, nil
}

func compare(root, rel string, info fs.FileInfo, m Manifest) (*Change, *Skipped, error) {
	full := filepath.Join(root, filepath.FromSlash(rel))
	prev, tracked := m[rel]
	if info.Mode()&fs.ModeSymlink != 0 {
		target, err := os.Readlink(full)
		if err != nil {
			return nil, &Skipped{Path: rel, Reason: "unreadable"}, nil
		}
		if _, err := CleanPath(path.Join(path.Dir(rel), target)); err != nil || strings.HasPrefix(target, "/") {
			return nil, &Skipped{Path: rel, Reason: "symlink_outside"}, nil
		}
		switch {
		case !tracked:
			return &Change{Path: rel, Op: "added"}, nil, nil
		case prev.Link != target:
			return &Change{Path: rel, Op: "modified"}, nil, nil
		}
		return nil, nil, nil
	}
	if !info.Mode().IsRegular() {
		return nil, &Skipped{Path: rel, Reason: "unreadable"}, nil
	}
	if !tracked {
		if info.Size() > MaxNewFile {
			return nil, &Skipped{Path: rel, Reason: "too_large"}, nil
		}
		return &Change{Path: rel, Op: "added", Size: info.Size()}, nil, nil
	}
	if prev.Link == "" && info.Size() == prev.Size && info.Mode().Perm() == prev.Mode {
		sum, err := fileSHA(full)
		if err != nil {
			return nil, &Skipped{Path: rel, Reason: "unreadable"}, nil
		}
		if sum == prev.SHA {
			return nil, nil, nil
		}
	}
	return &Change{Path: rel, Op: "modified", Size: info.Size()}, nil, nil
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

func write(root string, c *Changes, files []string, w io.Writer) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	b, _ := json.Marshal(c)
	if err := tw.WriteHeader(&tar.Header{Name: ChangesFile, Mode: 0o644, Size: int64(len(b)), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	if _, err := tw.Write(b); err != nil {
		return err
	}
	for _, rel := range files {
		full := filepath.Join(root, filepath.FromSlash(rel))
		info, err := os.Lstat(full)
		if err != nil {
			return err
		}
		h, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		if info.Mode()&fs.ModeSymlink != 0 {
			if h.Linkname, err = os.Readlink(full); err != nil {
				return err
			}
		}
		h.Name, h.Uname, h.Gname, h.Uid, h.Gid = rel, "", "", 0, 0
		h.Mode &= 0o777
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			f, err := os.Open(full)
			if err != nil {
				return err
			}
			_, err = io.Copy(tw, f)
			_ = f.Close()
			if err != nil {
				return err
			}
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}
