package runfiles

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// CHK-14: HasChanges agrees with Collect.
func TestCHK14HasChanges(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	sum, _ := fileSHA(filepath.Join(root, "a.txt"))
	m := Manifest{"a.txt": {Size: 1, Mode: 0o644, SHA: sum}}
	if changed, err := HasChanges(root, m); err != nil || changed {
		t.Fatalf("unchanged: %v %v", changed, err)
	}
	// node_modules is never a change.
	if err := os.MkdirAll(filepath.Join(root, "node_modules"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "node_modules", "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if changed, err := HasChanges(root, m); err != nil || changed {
		t.Fatalf("skipped dir: %v %v", changed, err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.txt"), []byte("b"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := HasChanges(root, m)
	if err != nil || !changed {
		t.Fatalf("added: %v %v", changed, err)
	}
	c, err := Collect(root, m, &bytes.Buffer{})
	if err != nil || len(c.Changes) != 1 || c.Changes[0].Path != "b.txt" {
		t.Fatalf("%+v %v", c, err)
	}
}
