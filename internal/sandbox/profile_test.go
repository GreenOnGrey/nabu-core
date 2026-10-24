package sandbox

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// FTR.NAB.CMN-0008 tech §5.3: sandbox-init copies the binary and writes the
// manifest the api returns; a failing api leaves the name only.
func TestInit(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" || r.URL.Path != "/internal/v1/sandbox/profile" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"name":"documents","kind":"builtin","python":[{"name":"pandas","version":"2.3.3"}]}`))
	}))
	defer api.Close()
	dir := t.TempDir()
	if err := Init(context.Background(), InitConfig{Profile: "documents", APIURL: api.URL, Token: "tok", Dir: dir}); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(filepath.Join(dir, "bin", "nabu")); err != nil || st.Mode()&0o111 == 0 {
		t.Fatalf("binary: %v", err)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "sandbox-profile.json"))
	if !strings.Contains(string(b), "pandas") {
		t.Fatalf("manifest %s", b)
	}
	dir2 := t.TempDir()
	if err := Init(context.Background(), InitConfig{Profile: "base", APIURL: api.URL, Token: "bad", Dir: dir2}); err != nil {
		t.Fatal(err)
	}
	b, _ = os.ReadFile(filepath.Join(dir2, "sandbox-profile.json"))
	if string(b) != `{"name":"base"}` {
		t.Fatalf("fallback %s", b)
	}
}
