//go:build integration

package itest

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/clients"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/proxies"
	"github.com/GreenOnGrey/nabu-core/internal/sandboxprofile"
	"github.com/GreenOnGrey/nabu-core/internal/services"
)

func tarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	for n, body := range files {
		_ = tw.WriteHeader(&tar.Header{Name: n, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg})
		_, _ = tw.Write([]byte(body))
	}
	_ = tw.Close()
	_ = gz.Close()
	return b.Bytes()
}

// FTR.HMR.CMN-0007 RF-01…RF-06, RF-10, RF-14: the working copy of a run.
func TestRunFiles(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	signer := jwt.NewSigner("https://n", key)
	ms := &models.Service{Pool: pool, Box: box}
	conn, err := ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "files-llm", BaseURL: "http://litellm:4000", APIKey: "sk-123456789", Models: models.Refs("gpt-x")})
	if err != nil {
		t.Fatal(err)
	}
	cs := &clients.Service{Pool: pool, Signer: signer}
	cl, err := cs.Create(ctx, clients.Input{Name: "files-client"})
	if err != nil {
		t.Fatal(err)
	}
	other, _ := cs.Create(ctx, clients.Input{Name: "files-other"})
	store := &mem{m: map[string][]byte{}}
	px := &proxies.Service{Pool: pool, Box: box}
	ss := &services.Service{Pool: pool, Models: ms, Signer: signer, Bus: &bus{}, Harnesses: []services.Harness{{Name: "pi"}},
		ProxiesExist: px.Exists, Files: &services.Files{Store: store, Cfg: services.FilesConfig{MaxFiles: 3, MaxBytes: 1000, UploadTTL: 3600e9, Keep: 3600e9}}}
	for _, c := range []services.Config{
		{Name: "files-code", Model: models.Choice{ConnectionID: conn.ID, Model: "gpt-x"}, Workspace: "nabu", Proxies: []string{"packages"}, Clients: []string{"files-client"}},
		{Name: "files-plain", Model: models.Choice{ConnectionID: conn.ID, Model: "gpt-x"}, Workspace: "none", Clients: []string{"files-client"}},
	} {
		if _, err := ss.Save(ctx, nil, "", c, true); err != nil {
			t.Fatal(err)
		}
	}
	// tech §3.3: an unknown proxy of an agent
	if _, err := ss.Save(ctx, nil, "", services.Config{Name: "files-bad", Model: models.Choice{ConnectionID: conn.ID, Model: "gpt-x"},
		Workspace: "nabu", Proxies: []string{"nope"}}, true); code(err) != "unknown_proxy" {
		t.Fatalf("unknown proxy: %v", err)
	}
	hc, _ := cs.Load(ctx, cl.ID)

	// RF-01, RF-02
	up := uuid.New()
	info, err := ss.Upload(ctx, cl.ID, up, bytes.NewReader(tarGz(t, map[string]string{"a.go": "package a", "b.go": "package a"})))
	if err != nil || info.Files != 2 || len(store.m) != 1 {
		t.Fatalf("RF-01: %+v %v", info, err)
	}
	big := uuid.New()
	if _, err := ss.Upload(ctx, cl.ID, big, bytes.NewReader(tarGz(t, map[string]string{"1": "", "2": "", "3": "", "4": ""}))); code(err) != "workspace_too_large" {
		t.Fatalf("RF-02 files: %v", err)
	}
	if _, err := ss.Upload(ctx, cl.ID, big, bytes.NewReader(tarGz(t, map[string]string{"1": strings.Repeat("x", 1001)}))); code(err) != "workspace_too_large" {
		t.Fatalf("RF-02 bytes: %v", err)
	}
	if len(store.m) != 1 {
		t.Fatalf("RF-02: a refused copy stays in S3: %d", len(store.m))
	}
	// RF-03
	if _, err := ss.Upload(ctx, cl.ID, uuid.New(), bytes.NewReader(tarGz(t, map[string]string{"../x": "x"}))); code(err) != "bad_archive" {
		t.Fatalf("RF-03: %v", err)
	}
	// RF-04
	if _, err := ss.Start(ctx, hc, "files-plain", services.RunInput{Input: "x", Files: up.String()}); code(err) != "no_workspace" {
		t.Fatalf("RF-04: %v", err)
	}
	// RF-05: a foreign upload
	if _, err := ss.Start(ctx, hc, "files-code", services.RunInput{Input: "x", Files: uuid.NewString()}); code(err) != "upload_not_found" {
		t.Fatalf("RF-05: %v", err)
	}
	st, err := ss.Start(ctx, hc, "files-code", services.RunInput{Input: "x", Files: up.String(), IdempotencyKey: "task:f:1"})
	if err != nil {
		t.Fatal(err)
	}
	// the upload moved under the key of the run
	if _, ok := store.m["run-files/"+st.RunID.String()+"/in.tar.gz"]; !ok || len(store.m) != 1 {
		t.Fatalf("the copy of the run: %v", len(store.m))
	}
	// RF-06
	if _, err := ss.Upload(ctx, cl.ID, up, bytes.NewReader(tarGz(t, map[string]string{"a.go": "x"}))); code(err) != "upload_in_use" {
		t.Fatalf("RF-06: %v", err)
	}
	// a repeat by the idempotency key returns the run and drops its own upload
	again := uuid.New()
	if _, err := ss.Upload(ctx, cl.ID, again, bytes.NewReader(tarGz(t, map[string]string{"a.go": "x"}))); err != nil {
		t.Fatal(err)
	}
	if rep, err := ss.Start(ctx, hc, "files-code", services.RunInput{Input: "x", Files: again.String(), IdempotencyKey: "task:f:1"}); err != nil || rep.RunID != st.RunID || len(store.m) != 1 {
		t.Fatalf("repeat: %+v %v %v", rep, err, len(store.m))
	}
	if _, err := ss.Upload(ctx, other.ID, uuid.New(), bytes.NewReader(tarGz(t, map[string]string{"a": "x"}))); err != nil {
		t.Fatal(err)
	}
	// RF-14: the run carries the copy and the proxies of the agent
	run, _ := ss.GetRun(ctx, st.RunID)
	if run.Files == nil || run.Files.Files != 2 || len(run.Proxies) != 1 || run.Proxies[0] != "packages" || run.Changes != nil {
		t.Fatalf("RF-14: %+v", run)
	}
	rc, err := ss.RunFiles(ctx, st.RunID)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	// RF-10
	if _, _, err := ss.Changes(ctx, cl.ID, st.RunID); code(err) != "run_not_finished" {
		t.Fatalf("RF-10 running: %v", err)
	}
	_, _ = pool.Exec(ctx, `UPDATE runs SET status = 'succeeded', finished_at = now() WHERE id = $1`, st.RunID)
	if _, _, err := ss.Changes(ctx, cl.ID, st.RunID); code(err) != "no_changes" {
		t.Fatalf("RF-10 none: %v", err)
	}
	out := tarGz(t, map[string]string{"changes.json": `{"changes":[{"path":"a.go","op":"modified"}]}`, "a.go": "package a // v2"})
	if err := ss.PutChanges(ctx, st.RunID, bytes.NewReader(out)); err != nil {
		t.Fatal(err)
	}
	ss.SetChangesCount(ctx, st.RunID, 1)
	if _, _, err := ss.Changes(ctx, other.ID, st.RunID); code(err) != "not_found" {
		t.Fatalf("a foreign client: %v", err)
	}
	rc, n, err := ss.Changes(ctx, cl.ID, st.RunID)
	if err != nil || n != 1 {
		t.Fatalf("changes: %d %v", n, err)
	}
	b, _ := io.ReadAll(rc)
	if !bytes.Equal(b, out) {
		t.Fatal("the archive of changes differs")
	}
	// RF-11: after RUN_FILES_KEEP the files are gone
	_, _ = pool.Exec(ctx, `UPDATE runs SET finished_at = now() - interval '2 days' WHERE id = $1`, st.RunID)
	ss.SweepFiles(ctx)
	if _, _, err := ss.Changes(ctx, cl.ID, st.RunID); code(err) != "expired" {
		t.Fatalf("RF-11: %v", err)
	}
}

// FTR.HMR.CMN-0007 PX-01, PX-02, PX-08, PX-09, PX-21: proxies and their
// assignment.
func TestProxies(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	var audit []string
	px := &proxies.Service{Pool: pool, Box: box, Audit: func(_ context.Context, _, tool string, args any) { audit = append(audit, tool) }}
	// PX-01: the built-in proxy gets the template
	if err := px.EnsureBuiltin(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := px.Get(ctx, "packages")
	if err != nil || len(p.Allow) != 5 {
		t.Fatalf("PX-01: %+v %v", p, err)
	}
	// PX-02: a private registry with a token
	p, err = px.Create(ctx, "admin", proxies.Input{Name: "corp-npm", Title: "Корпоративный npm", Kind: proxies.KindPackages,
		Ecosystems: []string{"npm"}, Registries: []proxies.Registry{{Ecosystem: "npm", URL: "https://npm.company.local", Internal: true, Token: "s3cret"}}})
	if err != nil {
		t.Fatal(err)
	}
	v := proxies.View(*p)
	if len(v.Allow) != 2 || !v.Registries[0].TokenSet || v.Registries[0].Token != "" {
		t.Fatalf("PX-02 view: %+v", v)
	}
	for _, e := range v.Env {
		if e.Secret && (e.Value != "" || !e.Set || e.ValueEnc != nil) {
			t.Fatalf("PX-02 secret leaks: %+v", e)
		}
	}
	rs, err := px.Resolve(ctx, []string{"corp-npm"})
	if err != nil || rs[0].Env["NPM_TOKEN"] != "s3cret" {
		t.Fatalf("PX-02 resolve: %+v %v", rs, err)
	}
	// PX-08: the token survives an update that does not send it
	if _, err := px.Update(ctx, "admin", "corp-npm", proxies.Input{Title: "npm компании", Ecosystems: []string{"npm"},
		Registries: []proxies.Registry{{Ecosystem: "npm", URL: "https://npm.company.local", Internal: true}}}); err != nil {
		t.Fatal(err)
	}
	if rs, _ := px.Resolve(ctx, []string{"corp-npm"}); rs[0].Env["NPM_TOKEN"] != "s3cret" || rs[0].Title != "npm компании" {
		t.Fatalf("PX-08: %+v", rs)
	}
	// PX-21, PX-09: personal agents get the proxy next to the profile; then it cannot be deleted
	cleanSandboxTables(t)
	ps := profilesService()
	ps.ProxiesExist = px.Exists
	var assigned any
	ps.Audit = func(_ context.Context, _, _ string, args any) { assigned = args }
	if _, err := ps.SyncBuiltins(ctx); err != nil {
		t.Fatal(err)
	}
	names := []string{"corp-npm"}
	if err := ps.Assign(ctx, "admin", map[string]sandboxprofile.Assignment{"personal": {Proxies: &names}}); err != nil {
		t.Fatal(err)
	}
	// R21: the audit of an assignment has what was and what is
	if b, _ := json.Marshal(assigned); !strings.Contains(string(b), `"before":{"personal":{"profile":"documents","proxies":[]}}`) ||
		!strings.Contains(string(b), `"after":{"personal":{"profile":"documents","proxies":["corp-npm"]}}`) {
		t.Fatalf("R21 audit: %s", b)
	}
	if got, _ := ps.AssignedProxies(ctx, "personal"); len(got) != 1 {
		t.Fatalf("PX-21: %v", got)
	}
	if err := ps.Assign(ctx, "admin", map[string]sandboxprofile.Assignment{"personal": {Profile: "base"}}); err != nil {
		t.Fatal(err)
	}
	if got, _ := ps.AssignedProxies(ctx, "personal"); len(got) != 1 {
		t.Fatalf("PX-21: the old form drops the proxies: %v", got)
	}
	if err := px.Delete(ctx, "admin", "corp-npm"); code(err) != "proxy_assigned" {
		t.Fatalf("PX-09: %v", err)
	}
	empty := []string{}
	_ = ps.Assign(ctx, "admin", map[string]sandboxprofile.Assignment{"personal": {Proxies: &empty}})
	if err := px.Delete(ctx, "admin", "corp-npm"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(audit, ",") != "proxy.create,proxy.update,proxy.delete" {
		t.Fatalf("PX-10: %v", audit)
	}
	cleanSandboxTables(t)
}
