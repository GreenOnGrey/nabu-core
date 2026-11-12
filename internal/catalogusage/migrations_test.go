package catalogusage

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

// MIG-07: the migrations of the feature are 0019 and 0020 — the numbers from
// arch A25 (0015/0016) are taken by the repository; both are the latest.
func TestMigrationsPresent(t *testing.T) {
	files, err := filepath.Glob("../../migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`^(\d{4})_.*\.sql$`)
	max := 0
	found := map[string]bool{}
	for _, f := range files {
		m := re.FindStringSubmatch(filepath.Base(f))
		if m == nil {
			continue
		}
		n := 0
		for _, c := range m[1] {
			n = n*10 + int(c-'0')
		}
		if n > max {
			max = n
		}
		found[filepath.Base(f)] = true
	}
	for _, want := range []string{"0019_catalog_usage.sql", "0020_catalog_usage_actions.sql"} {
		if !found[want] {
			t.Fatalf("migration %s is missing; have %v", want, found)
		}
	}
	if max != 20 {
		t.Fatalf("the latest migration is %04d, want 0020", max)
	}
	// every migration has Down, the aggregates live in 0019 and the journal values in 0020
	for _, f := range []string{"0019_catalog_usage.sql", "0020_catalog_usage_actions.sql"} {
		b, err := os.ReadFile(filepath.Join("../../migrations", f))
		if err != nil {
			t.Fatal(err)
		}
		if !regexp.MustCompile(`(?m)^-- \+goose Up$`).Match(b) || !regexp.MustCompile(`(?m)^-- \+goose Down$`).Match(b) {
			t.Fatalf("%s has no Up/Down", f)
		}
	}
}
