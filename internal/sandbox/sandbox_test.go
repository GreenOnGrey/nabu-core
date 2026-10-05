package sandbox

import "testing"

// SPC-06: excluded paths are not synced.
func TestExcluded(t *testing.T) {
	pats := []string{"node_modules/", ".venv/", "__pycache__/", "*.tmp"}
	for p, want := range map[string]bool{
		"node_modules/x/index.js": true, "app/node_modules/a.js": true, ".venv/bin/python": true,
		"src/__pycache__/m.pyc": true, "notes.tmp": true, "dir/a.tmp": true,
		"src/main.go": false, "node_modules.md": false, "report.txt": false,
	} {
		if got := Excluded(p, pats); got != want {
			t.Errorf("%s: %v", p, got)
		}
	}
}
