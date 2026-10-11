package comm

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ISO-10 (FTR.NAB.CMN-0003 tech §11): postgres.AsMaintenance shows the rows
// of every user, so only background work may call it — the retention, the
// recount of vectors, the recovery of interrupted ticks. A handler of HTTP or
// of an MCP tool must never reach it: the list below is the whole of the
// allowed callers, and a new one fails the build until it is reviewed here.
func TestMaintenanceCallers(t *testing.T) {
	allowed := map[string]bool{
		"internal/comm/purge.go:Purge":            true,
		"internal/comm/batches.go:StaleBatches":   true,
		"internal/comm/batches.go:LostRuns":       true,
		"internal/comm/vectors.go:EmbedPending":   true,
		"internal/comm/vectors.go:PendingVectors": true,
	}
	root := filepath.Join("..", "..")
	seen := map[string]bool{}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "AsMaintenance" {
					return true
				}
				key := rel + ":" + fn.Name.Name
				seen[key] = true
				if !allowed[key] {
					t.Errorf("%s calls postgres.AsMaintenance: it reads the rows of every user and is only for background work", key)
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for key := range allowed {
		if !seen[key] {
			t.Errorf("%s no longer calls AsMaintenance: remove it from the list", key)
		}
	}
	// the callers themselves are not reachable from handlers
	for _, file := range []string{"http.go", "admin.go", "tools.go", "tools_collect.go"} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for key := range allowed {
			name := key[strings.IndexByte(key, ':')+1:]
			if strings.Contains(string(src), "."+name+"(") {
				t.Errorf("%s calls %s, which reads the rows of every user", file, name)
			}
		}
	}
}
