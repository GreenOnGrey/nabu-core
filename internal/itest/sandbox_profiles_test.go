//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/pkgsignals"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/sandboxprofile"
)

func profilesService() *sandboxprofile.Service {
	return &sandboxprofile.Service{Pool: pool, Cfg: sandboxprofile.Config{
		Profiles:     map[string]string{"base": "ghcr.io/x/nabu-sandbox-base:v1", "documents": "ghcr.io/x/nabu-sandbox-documents:v1"},
		Defaults:     map[string]string{"personal": "documents", "group": "documents", "service": "base"},
		CustomImages: true, Prefixes: []string{"registry.local/nabu/"}, Version: "v1"}}
}

func cleanSandboxTables(t *testing.T) {
	t.Helper()
	for _, q := range []string{`DELETE FROM sandbox_assignments`, `DELETE FROM sandbox_profiles`, `DELETE FROM package_signals`,
		`DELETE FROM package_status`, `DELETE FROM package_status_log`, `DELETE FROM package_aliases WHERE NOT builtin`} {
		if _, err := pool.Exec(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
}

// FTR.NAB.CMN-0008 ASG-01, ASG-02, ASG-03, CUS-02, CUS-17: built-in profiles,
// defaults, assignments and custom profiles.
func TestSandboxProfiles(t *testing.T) {
	ctx := context.Background()
	cleanSandboxTables(t)
	ps := profilesService()
	if _, err := ps.SyncBuiltins(ctx); err != nil {
		t.Fatal(err)
	}
	uid := newUser(t, "profiles@example.com")
	p, err := ps.Resolve(ctx, sandboxprofile.Target{Kind: sandboxprofile.TargetPersonal, UserID: uid})
	if err != nil || p.Name != "documents" || p.Image != "ghcr.io/x/nabu-sandbox-documents:v1" {
		t.Fatalf("ASG-01: %+v %v", p, err)
	}
	if p, _ := ps.Resolve(ctx, sandboxprofile.Target{Kind: sandboxprofile.TargetService}); p.Name != "base" {
		t.Fatalf("ASG-01 service: %+v", p)
	}
	if err := ps.Create(ctx, "admin", "tools", "docker.io/library/python:3.12", ""); code(err) != "sandbox_image_registry_denied" {
		t.Fatalf("CUS-02: %v", err)
	}
	if err := ps.Create(ctx, "admin", "tools", "registry.local/nabu/tools:1", "Oracle client"); err != nil {
		t.Fatal(err)
	}
	if err := ps.Create(ctx, "admin", "tools", "registry.local/nabu/tools:2", ""); code(err) != "sandbox_profile_name_taken" {
		t.Fatalf("name taken: %v", err)
	}
	if err := ps.Assign(ctx, "admin", map[string]string{"personal": "tools"}); code(err) != "sandbox_profile_not_ready" {
		t.Fatalf("ASG-03: %v", err)
	}
	// The probe passed (the worker's finish).
	inv, _ := json.Marshal(sandboxprofile.Manifest{Commands: []string{"sqlplus", "ffmpeg"}})
	if _, err := pool.Exec(ctx, `UPDATE sandbox_profiles SET state = 'ready', image_digest = 'sha256:abc', inventory = $1 WHERE name = 'tools'`, inv); err != nil {
		t.Fatal(err)
	}
	if err := ps.Assign(ctx, "admin", map[string]string{"personal": "tools"}); err != nil {
		t.Fatal(err)
	}
	p, _ = ps.Resolve(ctx, sandboxprofile.Target{Kind: sandboxprofile.TargetPersonal, UserID: uid})
	if p.Name != "tools" || p.Image != "registry.local/nabu/tools@sha256:abc" {
		t.Fatalf("ASG-02: %+v", p)
	}
	if p, _ := ps.Resolve(ctx, sandboxprofile.Target{Kind: sandboxprofile.TargetGroup}); p.Name != "documents" {
		t.Fatalf("ASG-02 group untouched: %+v", p)
	}
	if err := ps.Delete(ctx, "tools"); code(err) != "sandbox_profile_in_use" {
		t.Fatalf("CUS-17: %v", err)
	}
	if err := ps.Delete(ctx, "base"); code(err) != "sandbox_profile_builtin" {
		t.Fatalf("built-in: %v", err)
	}
	// A refresh keeps the profile on its digest (R24).
	if err := ps.Refresh(ctx, "admin", "tools"); err != nil {
		t.Fatal(err)
	}
	if p, _ := ps.Resolve(ctx, sandboxprofile.Target{Kind: sandboxprofile.TargetPersonal, UserID: uid}); p.Image != "registry.local/nabu/tools@sha256:abc" {
		t.Fatalf("CUS-10: %+v", p)
	}
	m, err := ps.ManifestFor(ctx, sandboxprofile.Caller{UserID: uid})
	if err != nil || m.Description != "Oracle client" || m.Digest != "sha256:abc" {
		t.Fatalf("CUS-19: %+v %v", m, err)
	}
	u, _ := ps.AssignedTo(ctx, "documents")
	if u.Personal || !u.Group {
		t.Fatalf("assigned to %+v", u)
	}
}

// SIG-08, SIG-09, SIG-11, REQ-02, REQ-03, ADM-01, ADM-03, ADM-04, ADM-05: signals,
// statuses and the table of requests.
func TestPackageSignals(t *testing.T) {
	ctx := context.Background()
	cleanSandboxTables(t)
	ps := profilesService()
	ms, err := ps.SyncBuiltins(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The manifests of this run have no packages; give documents pandas.
	inv, _ := json.Marshal(sandboxprofile.Manifest{Python: []sandboxprofile.Package{{Name: "pandas", Version: "2.3.3"}}})
	if _, err := pool.Exec(ctx, `UPDATE sandbox_profiles SET inventory = $1 WHERE name = 'documents'`, inv); err != nil {
		t.Fatal(err)
	}
	_ = ms
	s := &pkgsignals.Service{Pool: pool, Profiles: ps}
	users := make([]uuid.UUID, 3)
	for i := range users {
		users[i] = newUser(t, "sig"+string(rune('a'+i))+"@example.com")
	}
	sig := func(subj uuid.UUID, eco, name, src string) string {
		r, err := s.Record(ctx, pkgsignals.Signal{Ecosystem: eco, Name: name, Source: src, Profile: "documents", AgentKind: "personal", Subject: subj.String()})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	// SIG-08: one signal a day.
	for i := 0; i < 50; i++ {
		sig(users[0], "python-module", "cv2", "auto")
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM package_signals WHERE name = 'cv2'`).Scan(&n)
	if n != 1 {
		t.Fatalf("SIG-08: %d", n)
	}
	// SIG-11: the agent's opencv-python and the automatic cv2 are one row.
	sig(users[1], "pypi", "Opencv_Python", "agent")
	sig(users[2], "pypi", "opencv-python", "agent")
	sig(users[0], "binary", "ffmpeg", "agent")
	rows, err := s.Table(ctx, pkgsignals.Filter{Days: 30})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Name != "opencv-python" || rows[0].Users != 3 || rows[0].BySource["auto"] != 1 || rows[0].BySource["agent"] != 2 {
		t.Fatalf("ADM-01, SIG-11: %+v", rows)
	}
	b, _ := json.Marshal(rows)
	for _, u := range users {
		if strings.Contains(string(b), u.String()) {
			t.Fatal("ADM-03: a user id in the table")
		}
	}
	// SIG-09: 20 requests of an agent a day.
	for i := 0; i < 19; i++ {
		sig(users[1], "pypi", "pkg"+string(rune('a'+i)), "agent")
	}
	if r := sig(users[1], "pypi", "one-too-many", "agent"); r != pkgsignals.ResultRateLimited {
		t.Fatalf("SIG-09: %s", r)
	}
	// REQ-02: pandas is in documents.
	if st, _ := s.StatusOf(ctx, "pypi", "pandas", "base"); st.Status != "available" || st.Profile != "documents" {
		t.Fatalf("REQ-02: %+v", st)
	}
	// ADM-04, REQ-03: a rejection needs a reason, the agent sees it.
	if err := s.SetStatus(ctx, "admin", "pypi", "opencv-python", "rejected", "", "", ""); code(err) != "sandbox_reject_note_required" {
		t.Fatalf("ADM-04: %v", err)
	}
	if err := s.SetStatus(ctx, "admin", "pypi", "opencv-python", "rejected", "", "", "too heavy"); err != nil {
		t.Fatal(err)
	}
	if st, _ := s.StatusOf(ctx, "pypi", "opencv-python", "documents"); st.Status != "rejected" || st.Note != "too heavy" {
		t.Fatalf("REQ-03: %+v", st)
	}
	// ADM-05: a profile that now has ffmpeg marks it added.
	if err := s.MarkAdded(ctx, sandboxprofile.Manifest{Name: "documents", Kind: "builtin", Version: "v2", Commands: []string{"ffmpeg"}}); err != nil {
		t.Fatal(err)
	}
	c, err := s.CardOf(ctx, "binary", "ffmpeg", 30)
	if err != nil || c.Status != "added" || c.Profile != "documents" || c.Version != "v2" || len(c.History) != 1 {
		t.Fatalf("ADM-05: %+v %v", c, err)
	}
	// ADM-13: a module mapped to a package that a profile has is added at once.
	sig(users[2], "python-module", "pandaz", "auto")
	if err := s.Map(ctx, "python-module", "pandaz", "pypi", "pandas"); err != nil {
		t.Fatal(err)
	}
	if c, err := s.CardOf(ctx, "pypi", "pandas", 30); err != nil || c.Status != "added" || c.Profile != "documents" {
		t.Fatalf("ADM-13: %+v %v", c, err)
	}
	// The tool answers an agent of a user with a sandbox.
	tool := s.Tool()
	out, err := tool.Handler(ctx, mcp.Grant{UserID: users[2]}, json.RawMessage(`{"ecosystem":"pypi","name":"pandas","reason":"tables"}`))
	if err != nil || !strings.Contains(out, `"available"`) {
		t.Fatalf("REQ-02 tool: %s %v", out, err)
	}
	if n, err := s.Purge(ctx); err != nil || n != 0 {
		t.Fatalf("purge %d %v", n, err)
	}
}
