//go:build integration

package itest

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/access"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// FTR.NAB.CMN-0001 tech §12, решение 26, FTR.NAB.CMN-0007 tech §3: GRP-09 — the search does
// not depend on the case of the letters. The database of the stand is created
// with the locale C (store_test.go starts the test one the same way), where
// ILIKE folds only ASCII: «кон» did not find «Константин».
func TestSearchFoldsCyrillicCase(t *testing.T) {
	ctx := context.Background()
	// the collation of migration 0022 folds the letters whatever the locale is
	var folded string
	if err := pool.QueryRow(ctx, `SELECT lower('Константин' COLLATE `+postgres.SearchCollation+`)`).Scan(&folded); err != nil {
		t.Fatal(err)
	}
	if folded != "константин" {
		t.Fatalf("the collation does not fold: %q", folded)
	}

	u := newUser(t, "folds-"+uuid.NewString()[:6]+"@x.org")
	if _, err := pool.Exec(ctx, `UPDATE users SET name = 'Константин Стипанюк' WHERE id = $1`, u); err != nil {
		t.Fatal(err)
	}
	// the list of users of Administration → Users
	list, err := repo().List(ctx, "кон", "", 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range list {
		if a.ID == u {
			found = true
		}
	}
	if !found {
		t.Fatalf("the list did not find the user by a lower-case query: %d rows", len(list))
	}

	// the members of a group, and the uniqueness of a name by the same collation
	acc := &access.Service{Pool: pool}
	name := "Юристы " + uuid.NewString()[:4]
	if _, err := acc.Create(ctx, access.Actor{Email: "root@x.org"}, name, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := acc.Create(ctx, access.Actor{Email: "root@x.org"}, strings.ToLower(name), nil); !isCodeErr(err, "group_name_taken") {
		t.Fatalf("GRP-09: a name of another case: %v", err)
	}
	g, err := acc.Create(ctx, access.Actor{Email: "root@x.org"}, "Склад "+uuid.NewString()[:4], nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := acc.AddMembers(ctx, access.Actor{Email: "root@x.org"}, g.ID, []uuid.UUID{u}); err != nil {
		t.Fatal(err)
	}
	members, err := acc.Members(ctx, g.ID, "кон", httpx.Page{Limit: 50})
	if err != nil {
		t.Fatal(err)
	}
	if len(members.Items) != 1 || members.Items[0].ID != u {
		t.Fatalf("the members of the group were not found by a lower-case query: %+v", members.Items)
	}
	// and the groups of the user come with the row of the list (FTR.NAB.CMN-0007 R7)
	list, err = repo().List(ctx, "кон", "", 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range list {
		if a.ID != u {
			continue
		}
		if len(a.Groups) != 1 || a.Groups[0].ID != g.ID || a.Groups[0].Name != g.Name {
			t.Fatalf("the groups of the user: %+v", a.Groups)
		}
	}
}
