// Package runfiles reads and writes the archives of working copies of runs
// and their changes (FTR.HMR.CMN-0007 tech §2): tar.gz of regular files,
// directories and symbolic links that stay inside the root.
package runfiles

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
)

// Stats are the counts of a checked archive.
type Stats struct {
	Files int   `json:"files"`
	Bytes int64 `json:"bytes"`
}

// TooLargeError is a copy over the limits; the counts are where reading
// stopped (tech §2.1: partial).
type TooLargeError struct {
	Stats    Stats
	MaxFiles int
	MaxBytes int64
}

func (e *TooLargeError) Error() string {
	return fmt.Sprintf("the working copy is too large: %d files, %d bytes (at most %d files, %d bytes)", e.Stats.Files, e.Stats.Bytes, e.MaxFiles, e.MaxBytes)
}

// CleanPath checks a path of a tar entry: relative, inside the root,
// at most 1024 bytes; "" — the root itself.
func CleanPath(name string) (string, error) {
	if len(name) > 1024 || strings.HasPrefix(name, "/") || strings.Contains(name, "\\") || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("bad path %q", name)
	}
	p := path.Clean(name)
	if p == "." {
		return "", nil
	}
	if p == ".." || strings.HasPrefix(p, "../") {
		return "", fmt.Errorf("bad path %q", name)
	}
	return p, nil
}

// Check reads a tar.gz to the end and checks every entry (tech §2.1):
// regular files, directories and symbolic links that stay inside the root.
// A copy over the limits returns *TooLargeError.
func Check(r io.Reader, maxFiles int, maxBytes int64) (Stats, error) {
	var st Stats
	gz, err := gzip.NewReader(r)
	if err != nil {
		return st, fmt.Errorf("not gzip: %w", err)
	}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return st, fmt.Errorf("not tar: %w", err)
		}
		p, err := CleanPath(h.Name)
		if err != nil {
			return st, err
		}
		switch h.Typeflag {
		case tar.TypeDir:
		case tar.TypeReg:
			st.Files++
			st.Bytes += h.Size
		case tar.TypeSymlink:
			st.Files++
			if strings.HasPrefix(h.Linkname, "/") {
				return st, fmt.Errorf("symlink %q points outside", p)
			}
			if _, err := CleanPath(path.Join(path.Dir(p), h.Linkname)); err != nil {
				return st, fmt.Errorf("symlink %q points outside", p)
			}
		default:
			return st, fmt.Errorf("%q: only files, directories and symlinks", p)
		}
		if (maxFiles > 0 && st.Files > maxFiles) || (maxBytes > 0 && st.Bytes > maxBytes) {
			return st, &TooLargeError{Stats: st, MaxFiles: maxFiles, MaxBytes: maxBytes}
		}
	}
	// Drain the gzip trailer so that the whole stream reaches S3.
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return st, err
	}
	return st, nil
}
