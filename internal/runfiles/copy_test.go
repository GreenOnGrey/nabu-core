package runfiles

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// archive builds a tar.gz; a value starting with "->" is a symlink.
func archive(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	for name, body := range files {
		h := &tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}
		if l, ok := strings.CutPrefix(body, "->"); ok {
			h = &tar.Header{Name: name, Linkname: l, Typeflag: tar.TypeSymlink}
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			_, _ = tw.Write([]byte(body))
		}
	}
	_ = tw.Close()
	_ = gz.Close()
	return b.Bytes()
}

func put(t *testing.T, root, rel, body string) {
	t.Helper()
	p := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// read returns changes.json and the files of an archive of changes.
func read(t *testing.T, b []byte) (Changes, map[string]string) {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	var c Changes
	files := map[string]string{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(tr)
		if h.Name == ChangesFile {
			_ = json.Unmarshal(body, &c)
			continue
		}
		files[h.Name] = string(body)
	}
	return c, files
}

// FTR.HMR.CMN-0007 RF-07, RF-08, RF-09: the copy is unpacked, the changes
// skip dependencies, caches and ignored files, but not tracked ones.
func TestCollect(t *testing.T) {
	root := t.TempDir()
	m, err := Extract(bytes.NewReader(archive(t, map[string]string{
		"main.go": "package main", "old.go": "old", ".gitignore": "build/\n*.log\n!keep.log\n/tmp-*\n",
		"build/tracked.txt": "v1", "web/.gitignore": "dist/\n", "link": "->main.go",
	})), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 6 || m["main.go"].SHA == "" || m["link"].Link != "main.go" {
		t.Fatalf("manifest %+v", m)
	}
	put(t, root, "main.go", "package main // changed")
	_ = os.Remove(filepath.Join(root, "old.go"))
	put(t, root, "new_test.go", "package main")
	put(t, root, "node_modules/x/index.js", "x")
	put(t, root, ".venv/bin/python", "x")
	put(t, root, "pkg/__pycache__/a.pyc", "x")
	put(t, root, ".cache/go-build/x", "x")
	put(t, root, "go/pkg/mod/x", "x")
	put(t, root, "go/bin/gopls", "x")
	put(t, root, "go/tools.go", "package tools")
	put(t, root, ".config/app.toml", "x")
	put(t, root, "web/.cache/x", "x")
	put(t, root, "build/out.bin", "x")
	put(t, root, "build/tracked.txt", "v2")
	put(t, root, "run.log", "x")
	put(t, root, "keep.log", "x")
	put(t, root, "tmp-1/a", "x")
	put(t, root, "web/dist/app.js", "x")
	put(t, root, "web/src/app.ts", "x")
	put(t, root, "big.bin", strings.Repeat("x", MaxNewFile+1))
	_ = os.Symlink("/etc/hosts", filepath.Join(root, "hosts"))
	var b bytes.Buffer
	c, err := Collect(root, m, &b)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, ch := range c.Changes {
		got = append(got, ch.Op+" "+ch.Path)
	}
	want := "added .config/app.toml|modified build/tracked.txt|added go/tools.go|added keep.log|modified main.go|added new_test.go|deleted old.go|added web/src/app.ts"
	if strings.Join(got, "|") != want {
		t.Fatalf("changes:\n%s\nwant\n%s", strings.Join(got, "|"), want)
	}
	skipped := map[string]string{}
	for _, s := range c.Skipped {
		skipped[s.Path] = s.Reason
	}
	if skipped["big.bin"] != "too_large" || skipped["hosts"] != "symlink_outside" {
		t.Fatalf("skipped %v", c.Skipped)
	}
	rc, files := read(t, b.Bytes())
	if len(rc.Changes) != 8 || files["main.go"] != "package main // changed" || files["build/tracked.txt"] != "v2" || len(files) != 7 {
		t.Fatalf("archive %v %v", rc.Changes, files)
	}
	if st, err := Check(bytes.NewReader(b.Bytes()), 0, 0); err != nil || st.Files != 8 {
		t.Fatalf("the archive of changes passes the check: %v %+v", err, st)
	}
}

// RF-03: archives with paths outside the root are refused.
func TestCheck(t *testing.T) {
	for _, files := range []map[string]string{{"../a": "x"}, {"/a": "x"}, {"l": "->/etc"}, {"d/l": "->../../x"}} {
		if _, err := Check(bytes.NewReader(archive(t, files)), 0, 0); err == nil {
			t.Errorf("%v accepted", files)
		}
	}
	var tl *TooLargeError
	if _, err := Check(bytes.NewReader(archive(t, map[string]string{"a": "1", "b": "2", "c": "3"})), 2, 0); !errors.As(err, &tl) || tl.Stats.Files != 3 {
		t.Fatalf("limit: %v", err)
	}
	if _, err := Check(bytes.NewReader(archive(t, map[string]string{"a": "12345"})), 0, 4); !errors.As(err, &tl) {
		t.Fatalf("bytes: %v", err)
	}
}

func TestIgnore(t *testing.T) {
	root := t.TempDir()
	put(t, root, ".gitignore", "**/gen/**\ndocs/*.pdf\n")
	g := NewIgnore(root)
	for rel, want := range map[string]bool{"a/gen/x.go": true, "gen/x": true, "docs/a.pdf": true, "docs/sub/a.pdf": false, "x.go": false} {
		if got := g.Ignored(rel, false); got != want {
			t.Errorf("%s: %v", rel, got)
		}
	}
}
