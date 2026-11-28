//go:build integration

package itest

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
)

// CAT-09 (FTR.NAB.CMN-0001 R11, design §11): the administration deletes an
// item of the catalog of skills and MCP; the item is gone and a second delete
// reports not_found.
func TestCatalogDelete(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	cat := &catalog.Service{Pool: pool, Box: box}
	mode := "platform"
	it, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "del-it-" + uuid.NewString()[:8],
		Source: &catalog.Source{Kind: "url", URL: "https://t/mcp"}, Mode: &mode})
	if err != nil {
		t.Fatal(err)
	}
	// the cascade of CAT-09: the connection of a user and the grant of a group go with the item
	uid := newUser(t, "del-"+uuid.NewString()[:8]+"@x.org")
	if _, err := pool.Exec(ctx, `INSERT INTO user_connections (user_id, item_id) VALUES ($1, $2)`, uid, it.ID); err != nil {
		t.Fatal(err)
	}
	var gid uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM user_groups WHERE builtin = 'all'`).Scan(&gid); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO access_grants (group_id, kind, item_id) VALUES ($1, 'catalog_item', $2)`, gid, it.ID); err != nil {
		t.Fatal(err)
	}
	if err := cat.Delete(ctx, it.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for _, q := range []string{`SELECT count(*) FROM user_connections WHERE item_id = $1`, `SELECT count(*) FROM access_grants WHERE item_id = $1`} {
		var n int
		if err := pool.QueryRow(ctx, q, it.ID).Scan(&n); err != nil || n != 0 {
			t.Fatalf("dependent rows stay: %d %v", n, err)
		}
	}
	if _, err := cat.Get(ctx, it.ID); !hasCode(err, "not_found") {
		t.Fatalf("the item is still there: %v", err)
	}
	if err := cat.Delete(ctx, it.ID); !hasCode(err, "not_found") {
		t.Fatalf("a second delete: %v", err)
	}
}
