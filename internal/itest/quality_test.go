//go:build integration && quality

package itest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/collect/quality"
	"github.com/GreenOnGrey/nabu-core/internal/comm"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/operator"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/services"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
)

// QLT-01 … QLT-06 (FTR.NAB.CMN-0003 qa §12): the corpus of 50 conversations
// goes through the real collector — Pi, the instructions of the release and
// the model of the collection — and is scored against its labelling.
//
//	NABU_PI_CMD=pi NABU_QUALITY_LLM_URL=https://… NABU_QUALITY_LLM_KEY=… NABU_QUALITY_LLM_MODEL=… \
//	  go test -tags 'integration quality' -count=1 -timeout 60m -run TestCollectQuality ./internal/itest/
//
// It costs money and is not deterministic: run it before a release and when
// the model or the instructions change, not in the usual CI.
func TestCollectQuality(t *testing.T) {
	cmd := strings.Fields(os.Getenv("NABU_PI_CMD"))
	url, apiKey, model := os.Getenv("NABU_QUALITY_LLM_URL"), os.Getenv("NABU_QUALITY_LLM_KEY"), os.Getenv("NABU_QUALITY_LLM_MODEL")
	if len(cmd) == 0 || url == "" || model == "" {
		t.Skip("NABU_PI_CMD, NABU_QUALITY_LLM_URL and NABU_QUALITY_LLM_MODEL are not set")
	}
	ctx := context.Background()
	corpus, err := quality.Load()
	if err != nil {
		t.Fatal(err)
	}
	s := commStore(t)
	box, _ := crypto.NewBox(key)
	signer := jwt.NewSigner("https://nabu.test", key)
	ms := &models.Service{Pool: pool, Box: box}
	conn, err := ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "Quality " + uuid.NewString()[:6], BaseURL: url, APIKey: apiKey, Models: models.Refs(model)})
	if err != nil {
		t.Fatal(err)
	}
	on := true
	if _, err := s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &on, Model: []byte(fmt.Sprintf(`{"connectionId":%q,"model":%q}`, conn.ID, model))}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		off := false
		_, _ = s.SaveSettings(ctx, comm.SettingsPatch{Enabled: &off})
	})

	gmail := &fakeGmail{token: "quality-token", calls: map[string]int{}, threads: map[string][]gmailMsg{}}
	for _, th := range corpus.Threads {
		for _, m := range th.Messages {
			gmail.threads[th.ID] = append(gmail.threads[th.ID], gmailMsg{ID: m.ID, Sender: m.Sender, To: m.To, Cc: m.Cc, Date: m.Date, Body: m.Body})
		}
	}
	gsrv := httptest.NewServer(gmail)
	defer gsrv.Close()

	cat := &catalog.Service{Pool: pool, Box: box, Signer: signer}
	cat.MemoryRun = s.ProxyGuard
	userEmail := func(context.Context, uuid.UUID) (string, bool) { return corpus.User, true }
	builtin := mcp.NewServer(signer)
	builtin.Register(s.CollectTools(func(context.Context, uuid.UUID) comm.UserInfo {
		return comm.UserInfo{Email: corpus.User, Timezone: corpus.Timezone}
	})...)
	ir := chi.NewRouter()
	ir.Handle("/internal/v1/mcp", builtin)
	cat.ProxyRoutes(ir, userEmail, nil)
	isrv := httptest.NewServer(ir)
	defer isrv.Close()
	cat.InternalURL = isrv.URL

	mode, yes := "personal", true
	item, err := cat.Save(ctx, nil, catalog.Input{Type: "mcp", Name: "gmail-q-" + uuid.NewString()[:6], Source: &catalog.Source{Kind: "url", URL: gsrv.URL},
		Mode: &mode, Published: &yes, PersonalAuth: &catalog.PersonalAuth{Kind: "token"}, ReadOnly: ptr(false)})
	if err != nil {
		t.Fatal(err)
	}
	adapter := "gmail"
	if err := s.SetSource(ctx, item.ID, &adapter, &yes); err != nil {
		t.Fatal(err)
	}
	uid := newUser(t, "quality-"+uuid.NewString()[:6]+"@x.org")
	if _, err := cat.Connect(ctx, uid, item.ID, "quality-token"); err != nil {
		t.Fatal(err)
	}
	_ = s.Consent(ctx, uid)

	// What exists before the collection: the topics and the open commitments.
	for i := 0; i < len(corpus.Topics); i += comm.MaxTopics {
		end := min(i+comm.MaxTopics, len(corpus.Topics))
		if _, err := s.SaveNote(ctx, uid, comm.SaveNote{ItemID: item.ID, Source: "gmail", ThreadKey: fmt.Sprintf("seed-topics-%d", i), Title: "Темы", Summary: "Существующие темы.",
			Topics: corpus.Topics[i:end], Messages: []comm.Message{{ID: "seed"}}}); err != nil {
			t.Fatal(err)
		}
	}
	seeds := map[uuid.UUID]string{}
	for _, sd := range corpus.SeedCommitments {
		thread := "seed-" + sd.Key
		if _, err := s.SaveNote(ctx, uid, comm.SaveNote{ItemID: item.ID, Source: "gmail", ThreadKey: thread, Title: sd.Text, Summary: sd.Text,
			People: []comm.PersonIn{{Email: sd.Person}}, Topics: []string{corpus.Topics[0]}, Messages: []comm.Message{{ID: "seed"}}}); err != nil {
			t.Fatal(err)
		}
		id, err := s.SaveCommitment(ctx, uid, comm.SaveCommitment{ItemID: item.ID, Source: "gmail", ThreadKey: thread, Text: sd.Text, Direction: sd.Direction, PersonEmail: sd.Person})
		if err != nil {
			t.Fatal(err)
		}
		seeds[id] = sd.Key
	}

	op, err := operator.New(operator.Config{Runtime: pi.Runtime{Command: cmd, Options: pi.Options{Path: os.Getenv("PATH")}}, WorkDir: t.TempDir(), ServiceToken: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithCancel(context.Background())
		cancel()
		op.Run(c)
	})
	osrv := httptest.NewServer(op.Handler())
	defer osrv.Close()

	bq := &queue{}
	ev := &capture{}
	ts := &tasks.Service{Pool: pool, Events: ev, Bus: bq, Rules: tasks.Rules{MinInterval: time.Minute, MaxActive: 5}, Catchup: time.Hour, Tick: time.Second,
		MaxFailures: 3, DefaultTimezone: corpus.Timezone}
	eng := &engine.Engine{Cfg: engine.Config{InternalURL: isrv.URL}, Pool: pool, Op: &agent.Client{BaseURL: osrv.URL, Token: "svc"}, Users: repo(), Chat: &chat.Store{Pool: pool},
		Models: ms, Catalog: cat, Tasks: ts, Comm: s, Services: &services.Service{Pool: pool, Events: ev}, Ledger: &ledger.Ledger{Pool: pool}, Signer: signer, Events: ev, Bus: bq,
		CollectCfg: engine.CollectConfig{BatchThreads: 10, RunTimeout: 10 * time.Minute, BackfillDays: 3650}}
	eng.Collector = eng
	mt, err := ts.SetMemoryTask(ctx, uid, "collect", tasks.MemoryPatch{Enabled: &yes, Settings: []byte(fmt.Sprintf(`{"sources":[{"itemId":%q,"source":"gmail"}]}`, item.ID))})
	if err != nil {
		t.Fatal(err)
	}
	denied := counterValue(metrics.MemoryDeniedTools)
	var run uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO task_runs (task_id, scheduled_for, status) VALUES ($1, now(), 'running') RETURNING id`, *mt.TaskID).Scan(&run); err != nil {
		t.Fatal(err)
	}
	msg, _ := json.Marshal(tasks.RunMessage{RunID: run, TaskID: *mt.TaskID, UserID: uid})
	if err := eng.HandleTaskRun(ctx, []byte(uid.String()), msg); err != nil {
		t.Fatal(err)
	}
	for _, m := range bq.take() {
		if err := eng.HandleCollectBatch(ctx, nil, m); err != nil {
			t.Fatal(err)
		}
	}
	var status string
	var class *string
	_ = pool.QueryRow(ctx, `SELECT status, error_class FROM task_runs WHERE id = $1`, run).Scan(&status, &class)
	if status != "succeeded" {
		t.Fatalf("the collection did not finish: %s %v", status, class)
	}

	out := quality.Run{Threads: map[string]quality.Result{}, Denied: int(counterValue(metrics.MemoryDeniedTools) - denied)}
	rows, err := pool.Query(ctx, `SELECT id, thread_key FROM comm_notes WHERE user_id = $1 AND thread_key NOT LIKE 'seed-%'`, uid)
	if err != nil {
		t.Fatal(err)
	}
	notes := map[uuid.UUID]string{}
	for rows.Next() {
		var id uuid.UUID
		var thread string
		if err := rows.Scan(&id, &thread); err != nil {
			t.Fatal(err)
		}
		notes[id] = thread
	}
	rows.Close()
	for id, thread := range notes {
		n, err := s.Get(ctx, uid, id)
		if err != nil {
			t.Fatal(err)
		}
		res := quality.Result{Saved: true, Texts: []string{n.Title, *n.Summary}}
		for _, a := range n.Appended {
			res.Texts = append(res.Texts, a.Text)
		}
		for _, tp := range n.Topics {
			res.Topics = append(res.Topics, tp.Label)
		}
		for _, c := range n.Commitments {
			f := quality.Found{Text: c.Text, Direction: c.Direction}
			if c.Person != nil {
				f.Person = c.Person.Email
			}
			if c.Due != nil {
				f.Due = c.Due.Format("2006-01-02")
			}
			res.Commitments = append(res.Commitments, f)
		}
		out.Threads[thread] = res
	}
	all, err := s.Commitments(ctx, uid, comm.CommitmentFilter{State: "closed"}, 500)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range all {
		if k, ok := seeds[c.ID]; ok {
			out.Closed = append(out.Closed, k)
		}
	}
	report := quality.Score(corpus, out)
	t.Log(report.Summary())
	for _, d := range report.Details {
		t.Log("  " + d)
	}
	for _, f := range report.Failures() {
		t.Error(f)
	}
}

func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	_ = c.Write(&m)
	return m.GetCounter().GetValue()
}
