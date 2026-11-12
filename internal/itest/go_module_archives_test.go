//go:build integration

package itest

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/GreenOnGrey/nabu-core/migrations"
)

// FTR.HMR.CMN-0007 PX-22: a proxy made from the template of go before tech 1.4 gets
// storage.googleapis.com from the migration; a proxy without Go stays as it was.
func TestGoModuleArchives(t *testing.T) {
	ctx := context.Background()
	old := `[{"host":"proxy.golang.org","port":443,"internal":false},{"host":"sum.golang.org","port":443,"internal":false}]`
	npm := `[{"host":"registry.npmjs.org","port":443,"internal":false}]`
	for name, allow := range map[string]string{"px22-go": old, "px22-npm": npm} {
		if _, err := pool.Exec(ctx, `INSERT INTO proxies (name, kind, title, description, spec, allow)
			VALUES ($1, 'packages', $1, '', '{}', $2::jsonb)`, name, allow); err != nil {
			t.Fatal(err)
		}
	}
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	if err := goose.DownToContext(ctx, db, ".", 18); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpContext(ctx, db, "."); err != nil {
		t.Fatal(err)
	}
	has := func(name string) bool {
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT allow @> '[{"host":"storage.googleapis.com","port":443}]'::jsonb FROM proxies WHERE name = $1`,
			name).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !has("px22-go") || has("px22-npm") {
		t.Fatalf("PX-22: go %v, npm %v", has("px22-go"), has("px22-npm"))
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT jsonb_array_length(allow) FROM proxies WHERE name = 'px22-go'`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("PX-22: %d hosts, %v", n, err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM proxies WHERE name LIKE 'px22-%'`); err != nil {
		t.Fatal(err)
	}
}
