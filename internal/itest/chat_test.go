//go:build integration

package itest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/app"
	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/groups"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/memory"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/space"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
)

// fakeAgent plays the operator for the turns of FTR.NAB.CHAT-0001: it records
// what it got and reports the fill of the context it is told.
type fakeAgent struct {
	mu        sync.Mutex
	opened    []agent.SessionRequest
	prompts   []agent.PromptRequest
	compacts  []string
	closed    []string
	tokens    int64 // the fill reported after a prompt
	window    int64
	autoCompa bool // the harness compacts during the next prompt
	failCompa bool
	during    func()        // called inside the turn, as a tool call of the agent
	script    []agent.Event // the events of the answer instead of the text "ok"
}

func (f *fakeAgent) Open(_ context.Context, req agent.SessionRequest, _ func(ctx context.Context) ([]byte, error)) (agent.SessionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opened = append(f.opened, req)
	return agent.SessionResponse{SessionID: uuid.NewString()}, nil
}

func (f *fakeAgent) Prompt(_ context.Context, _ string, p agent.PromptRequest, onEvent func(agent.Event)) error {
	f.mu.Lock()
	f.prompts = append(f.prompts, p)
	auto := f.autoCompa
	f.autoCompa = false
	tokens, window, during := f.tokens, f.window, f.during
	f.mu.Unlock()
	if during != nil {
		during()
	}
	if auto {
		onEvent(agent.Event{Type: agent.EventCompaction, Auto: true, Summary: "auto summary", TokensBefore: 190_000, TokensAfter: 8_000})
	}
	f.mu.Lock()
	script := f.script
	f.mu.Unlock()
	if script == nil {
		script = []agent.Event{{Type: agent.EventTextDelta, Delta: "ok"}}
	}
	for _, ev := range script {
		onEvent(ev)
	}
	onEvent(agent.Event{Type: agent.EventUsage, Usage: agent.Usage{TokensIn: 10, TokensOut: 2}})
	onEvent(agent.Event{Type: agent.EventContext, ContextTokens: tokens, ContextWindow: window})
	onEvent(agent.Event{Type: agent.EventSettled})
	return nil
}

func (f *fakeAgent) Compact(_ context.Context, _ string, instructions string) (agent.CompactResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCompa {
		return agent.CompactResponse{}, &agent.APIError{Status: 502, Code: "compact_failed", Message: "429: rate limit exceeded"}
	}
	f.compacts = append(f.compacts, instructions)
	return agent.CompactResponse{Summary: "Договор с «Ромашкой»: два риска.", TokensBefore: f.tokens, TokensAfter: 900, ContextWindow: f.window,
		Usage: agent.Usage{TokensIn: 100, TokensOut: 20}}, nil
}

func (f *fakeAgent) Abort(context.Context, string) error                     { return nil }
func (f *fakeAgent) Patch(context.Context, string, agent.PatchRequest) error { return nil }
func (f *fakeAgent) Snapshot(context.Context, string) ([]byte, error) {
	return []byte(`{"pi":"session"}`), nil
}
func (f *fakeAgent) Close(_ context.Context, id string) error {
	f.mu.Lock()
	f.closed = append(f.closed, id)
	f.mu.Unlock()
	return nil
}

func (f *fakeAgent) last() agent.PromptRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.prompts[len(f.prompts)-1]
}

// fakeWhisper recognizes "voice" and remembers the language.
type fakeWhisper struct {
	mu   sync.Mutex
	lang string
	fail bool
}

func (w *fakeWhisper) Transcribe(_ context.Context, audio io.Reader, _ string, lang string) (string, error) {
	_, _ = io.Copy(io.Discard, audio)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lang = lang
	if w.fail {
		return "", errors.New("whisper is down")
	}
	return "напомни завтра в 10 позвонить Ивану", nil
}

// chatRig is the api and the worker of a conversation with a fake agent.
type chatRig struct {
	t    *testing.T
	api  *chat.API
	eng  *engine.Engine
	op   *fakeAgent
	bus  *bus
	s3   *mem
	ev   *capture
	ms   *models.Service
	cat  *catalog.Service
	text *models.Connection
}

func newChatRig(t *testing.T) *chatRig {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	signer := jwt.NewSigner("https://nabu.test", key)
	r := &chatRig{t: t, op: &fakeAgent{window: 128_000}, bus: &bus{}, s3: &mem{m: map[string][]byte{}}, ev: &capture{}}
	r.ms = &models.Service{Pool: pool, Box: box}
	yes := true
	var err error
	r.text, err = r.ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "Chat " + uuid.NewString()[:6], BaseURL: "http://llm.invalid",
		APIKey: "sk-fake-0003", Models: []models.ModelRef{{ID: "text-1"}, {ID: "vision-1", Overrides: &models.CapsPatch{Image: &yes}},
			{ID: "no-tools", Overrides: &models.CapsPatch{Tools: new(bool)}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ms.PutPersonal(ctx, models.PersonalModels{Default: &models.Choice{ConnectionID: r.text.ID, Model: "text-1"},
		Available: []models.Choice{{ConnectionID: r.text.ID, Model: "vision-1"}}}); err != nil {
		t.Fatal(err)
	}
	r.cat = &catalog.Service{Pool: pool, Box: box, S3: r.s3, Signer: signer, InternalURL: "http://api.test"}
	r.api = &chat.API{Store: &chat.Store{Pool: pool}, Pool: pool, Users: repo(), Models: r.ms, Bus: r.bus, Events: r.ev, S3: r.s3, MaxFile: 1 << 20,
		MaxVoiceSeconds: 2, QueueMax: 10}
	r.api.Skills = func(ctx context.Context, uid uuid.UUID) ([]chat.Skill, error) {
		list, err := r.cat.SkillsFor(ctx, uid)
		out := []chat.Skill{}
		for _, s := range list {
			out = append(out, chat.Skill(s))
		}
		return out, err
	}
	r.eng = &engine.Engine{Cfg: engine.Config{InternalURL: "http://api.test", TurnTimeout: 30_000_000_000, SessionMaxAge: 3_600_000_000_000},
		Pool: pool, Op: r.op, Users: repo(), Chat: r.api.Store, Models: r.ms, Catalog: r.cat, Memory: &memory.Service{Pool: pool, Events: r.ev},
		Space: &space.Service{}, Ledger: &ledger.Ledger{Pool: pool}, Signer: signer, S3: r.s3, Events: r.ev, Bus: r.bus}
	return r
}

// drain runs the queued turns and commands of nabu.inbound as the worker does.
func (r *chatRig) drain() {
	r.t.Helper()
	for {
		r.bus.mu.Lock()
		var next string
		rest := r.bus.msgs[:0:0]
		for _, m := range r.bus.msgs {
			if next == "" && strings.HasPrefix(m, kafka.TopicInbound+" ") {
				next = strings.TrimPrefix(m, kafka.TopicInbound+" ")
				continue
			}
			rest = append(rest, m)
		}
		r.bus.msgs = rest
		r.bus.mu.Unlock()
		if next == "" {
			return
		}
		if err := r.eng.HandleInbound(context.Background(), nil, []byte(next)); err != nil {
			r.t.Fatal(err)
		}
	}
}

// outbound returns the texts sent to messengers and forgets them.
func (r *chatRig) outbound() []string {
	r.bus.mu.Lock()
	defer r.bus.mu.Unlock()
	var out, rest []string
	for _, m := range r.bus.msgs {
		if v, ok := strings.CutPrefix(m, kafka.TopicOutbound+" "); ok {
			var o struct{ Text string }
			_ = json.Unmarshal([]byte(v), &o)
			out = append(out, o.Text)
			continue
		}
		rest = append(rest, m)
	}
	r.bus.msgs = rest
	return out
}

func (r *chatRig) events(conv uuid.UUID) []chat.Event {
	rows, err := pool.Query(context.Background(), `SELECT event FROM messages WHERE conversation_id = $1 AND role = 'event' ORDER BY created_at`, conv)
	if err != nil {
		r.t.Fatal(err)
	}
	defer rows.Close()
	var out []chat.Event
	for rows.Next() {
		var b []byte
		var e chat.Event
		_ = rows.Scan(&b)
		_ = json.Unmarshal(b, &e)
		out = append(out, e)
	}
	return out
}

func isCodeErr(err error, code string) bool {
	e, ok := apperr.As(err)
	return ok && e.Code == code
}

// CTX-01…CTX-03, CTX-05…CTX-09, CTX-11, SEL-01…SEL-03, MDL-06, MDL-11, MDL-12
func TestChatContext(t *testing.T) {
	ctx := context.Background()
	r := newChatRig(t)
	uid := newUser(t, "ctx-"+uuid.NewString()[:6]+"@x.org")
	main, _ := r.api.Store.Main(ctx, uid)

	// CTX-09: the fill of the context after a turn
	r.op.tokens = 1000
	if _, err := r.api.Post(ctx, uid, nil, "привет", domain.ChannelWeb, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	r.drain()
	if f := r.api.Store.Context(ctx, main.ID); f.Tokens != 1000 || f.Window != 128_000 {
		t.Fatalf("CTX-09: %+v", f)
	}
	// CTX-06: a short context is not compacted
	if _, err := r.api.Compact(ctx, uid, nil, domain.ChannelWeb, ""); !isCodeErr(err, "context_too_short") {
		t.Fatalf("CTX-06: %v", err)
	}
	// CTX-02: a running answer blocks a reset
	var stuck uuid.UUID
	_ = pool.QueryRow(ctx, `INSERT INTO messages (conversation_id, role, text, channel, status) VALUES ($1,'assistant','','web','streaming') RETURNING id`, main.ID).Scan(&stuck)
	if _, err := r.api.Reset(ctx, uid, nil, domain.ChannelWeb); !isCodeErr(err, "context_busy") {
		t.Fatalf("CTX-02: %v", err)
	}
	_, _ = pool.Exec(ctx, `UPDATE messages SET status = 'done' WHERE id = $1`, stuck)

	// CTX-05: a compaction with instructions
	r.op.tokens = 150_000
	r.api.Post(ctx, uid, nil, "ещё", domain.ChannelWeb, nil, nil, nil) //nolint:errcheck
	r.drain()
	if _, err := r.api.Compact(ctx, uid, nil, domain.ChannelWeb, "договор с «Ромашкой»"); err != nil {
		t.Fatal(err)
	}
	r.drain()
	evs := r.events(main.ID)
	if len(r.op.compacts) != 1 || r.op.compacts[0] != "договор с «Ромашкой»" || len(evs) != 1 || evs[0].Kind != chat.EventCompact ||
		evs[0].TokensBefore != 150_000 || evs[0].TokensAfter != 900 || evs[0].Auto || !strings.Contains(evs[0].Summary, "Ромашк") {
		t.Fatalf("CTX-05: %+v %+v", r.op.compacts, evs)
	}
	if f := r.api.Store.Context(ctx, main.ID); f.Tokens != 900 {
		t.Fatalf("CTX-05: fill after compaction %+v", f)
	}
	// CTX-07: a failed compaction leaves no event and the same context
	r.op.failCompa = true
	r.op.tokens = 50_000                                                   // compactable, below the 80 % of R17
	r.api.Post(ctx, uid, nil, "ещё раз", domain.ChannelWeb, nil, nil, nil) //nolint:errcheck
	r.drain()
	if _, err := r.api.Compact(ctx, uid, nil, domain.ChannelWeb, ""); err != nil {
		t.Fatal(err)
	}
	r.drain()
	r.op.failCompa = false
	if n := len(r.events(main.ID)); n != 1 {
		t.Fatalf("CTX-07: events %d", n)
	}
	if !strings.Contains(strings.Join(r.ev.types(), ","), "chat.error") {
		t.Fatal("CTX-07: no error shown")
	}
	// CTX-08: the harness compacted by itself
	r.op.autoCompa = true
	r.api.Post(ctx, uid, nil, "длинный", domain.ChannelWeb, nil, nil, nil) //nolint:errcheck
	r.drain()
	if evs := r.events(main.ID); len(evs) != 2 || !evs[1].Auto || evs[1].Summary != "auto summary" {
		t.Fatalf("CTX-08: %+v", evs)
	}
	// CTX-11: a context over 80 % of the window is compacted before the turn
	r.op.tokens = 120_000
	r.api.Post(ctx, uid, nil, "раз", domain.ChannelWeb, nil, nil, nil) //nolint:errcheck
	r.drain()
	before := len(r.op.compacts)
	r.api.Post(ctx, uid, nil, "два", domain.ChannelWeb, nil, nil, nil) //nolint:errcheck
	r.drain()
	if evs := r.events(main.ID); len(r.op.compacts) != before+1 || r.op.compacts[before] != "" || !evs[len(evs)-1].Auto {
		t.Fatalf("CTX-11: %v %+v", r.op.compacts, evs)
	}

	// CTX-01: a reset closes the session without a snapshot; the next session starts empty
	if _, ok := r.s3.m["sessions/"+main.ID.String()+".jsonl"]; !ok {
		t.Fatal("a snapshot is expected before the reset")
	}
	resetID, err := r.api.Reset(ctx, uid, nil, domain.ChannelWeb)
	if err != nil {
		t.Fatal(err)
	}
	r.drain()
	// tech §2.1: 202 {eventId} is the id of the divider; a job delivered twice writes one
	var dividers int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1 AND event->>'kind' = 'reset'`, main.ID).Scan(&dividers)
	again, _ := json.Marshal(chat.Inbound{UserID: uid, ConversationID: main.ID, MessageID: resetID, Channel: domain.ChannelWeb, Command: chat.CommandReset})
	if err := r.eng.HandleInbound(ctx, nil, again); err != nil {
		t.Fatal(err)
	}
	var same, after int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE id = $1 AND event->>'kind' = 'reset'`, resetID).Scan(&same)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1 AND event->>'kind' = 'reset'`, main.ID).Scan(&after)
	if same != 1 || after != dividers {
		t.Fatalf("eventId: %d with the id, %d → %d dividers", same, dividers, after)
	}
	// CTX-06: nothing said since the reset — compaction is refused, the operator is not called
	compacts := len(r.op.compacts)
	if _, err := r.api.Compact(ctx, uid, nil, domain.ChannelWeb, ""); !isCodeErr(err, "context_too_short") || len(r.op.compacts) != compacts {
		t.Fatalf("CTX-06 after a reset: %v", err)
	}
	if _, ok := r.s3.m["sessions/"+main.ID.String()+".jsonl"]; ok {
		t.Fatal("CTX-01: the snapshot outlived the reset")
	}
	if f := r.api.Store.Context(ctx, main.ID); f.Tokens != 0 {
		t.Fatalf("CTX-01: fill after reset %+v", f)
	}
	// CTX-03: reset on the site, the next message from Telegram — a new session without the past
	opened := len(r.op.opened)
	if err := r.api.Receive(ctx, channels.Inbound{UserID: uid, Channel: domain.ChannelTelegram, Text: "о чём мы говорили?"}); err != nil {
		t.Fatal(err)
	}
	r.drain()
	if len(r.op.opened) != opened+1 {
		t.Fatal("CTX-01: a new session is expected")
	}
	if req := r.op.opened[opened]; req.Snapshot != nil {
		t.Fatal("CTX-01: the new session was restored from a snapshot")
	} else {
		for _, h := range req.History { // only what was said after the reset
			if h.Text != "о чём мы говорили?" {
				t.Fatalf("CTX-01: the new session knows the past: %q", h.Text)
			}
		}
	}
	if evs := r.events(main.ID); evs[len(evs)-1].Kind != chat.EventReset {
		t.Fatalf("CTX-01: %+v", evs)
	}

	// MDL-12: the default model does not see images — the prompt has none
	att, err := r.api.Attachments().Store(ctx, uid, "shot.png", "image/png", strings.NewReader("png-bytes"), 9)
	if err != nil {
		t.Fatal(err)
	}
	r.api.Post(ctx, uid, nil, "что на скриншоте?", domain.ChannelWeb, nil, []uuid.UUID{att}, nil) //nolint:errcheck
	r.drain()
	if p := r.op.last(); len(p.Images) != 0 || !strings.Contains(p.Text, "cannot see images") {
		t.Fatalf("MDL-12: %d images, %q", len(p.Images), p.Text)
	}
	// SEL-01, SEL-02, SEL-03, MDL-11: the user chooses the model that sees images
	v, err := r.api.PatchAgent(ctx, uid, nil, nil, nil)
	if err != nil || v.Model == nil || v.Model.Model != "text-1" || v.Chosen || len(v.AvailableModels) != 2 || !v.AvailableModels[1].Caps.Image ||
		v.AvailableModels[0].Caps.Image {
		t.Fatalf("SEL-01: %+v %v", v, err)
	}
	if _, err := r.api.PatchAgent(ctx, uid, nil, nil, &models.Choice{ConnectionID: r.text.ID, Model: "no-tools"}); !isCodeErr(err, "model_not_granted") {
		t.Fatalf("SEL-02: %v", err)
	}
	if v, err = r.api.PatchAgent(ctx, uid, nil, nil, &models.Choice{ConnectionID: r.text.ID, Model: "vision-1"}); err != nil || !v.Chosen {
		t.Fatalf("choose: %+v %v", v, err)
	}
	// SEL-03: the divider comes with the first answer of the new model, right before it
	if evs := r.events(main.ID); slices.ContainsFunc(evs, func(e chat.Event) bool { return e.Kind == chat.EventModel }) {
		t.Fatalf("SEL-03: a divider before any answer of the model: %+v", evs)
	}
	att2, _ := r.api.Attachments().Store(ctx, uid, "shot2.png", "image/png", strings.NewReader("png-bytes"), 9)
	r.api.Post(ctx, uid, nil, "а теперь?", domain.ChannelWeb, nil, []uuid.UUID{att2}, nil) //nolint:errcheck
	r.drain()
	if p := r.op.last(); len(p.Images) != 1 {
		t.Fatalf("MDL-11: %d images", len(p.Images))
	}
	var order []string // the fake harness also compacts after every answer
	rows, _ := pool.Query(ctx, `SELECT role, COALESCE(event->>'kind','') FROM messages WHERE conversation_id = $1 AND (role <> 'event' OR event->>'kind' <> 'compact')
		ORDER BY created_at DESC LIMIT 3`, main.ID)
	for rows.Next() {
		var role, kind string
		_ = rows.Scan(&role, &kind)
		order = append(order, role+":"+kind)
	}
	rows.Close()
	if strings.Join(order, ",") != "assistant:,event:model,user:" {
		t.Fatalf("SEL-03: %v", order)
	}
	if evs := r.events(main.ID); !slices.ContainsFunc(evs, func(e chat.Event) bool {
		return e.Kind == chat.EventModel && e.Model == "vision-1" && e.Name == "vision-1"
	}) {
		t.Fatalf("SEL-03: %+v", evs)
	}
	if v, err = r.api.PatchAgent(ctx, uid, nil, nil, nil, true); err != nil || v.Chosen || v.Model.Model != "text-1" {
		t.Fatalf("R16: back to the default %+v %v", v, err)
	}
	// MDL-06: a model without tools is not offered
	if _, err := r.ms.PutPersonal(ctx, models.PersonalModels{Default: &models.Choice{ConnectionID: r.text.ID, Model: "no-tools"}}); !isCodeErr(err, "model_no_tools") {
		t.Fatalf("MDL-06: %v", err)
	}
}

// packSkills makes a tar.gz of skill directories with SKILL.md.
func packSkills(t *testing.T, skills map[string]string) []byte {
	dir := t.TempDir()
	for name, descr := range skills {
		_ = os.MkdirAll(filepath.Join(dir, name), 0o755)
		md := "---\nname: " + name + "\ndescription: " + descr + "\n---\n# " + name + "\nMARKER-" + name + "\n"
		if err := os.WriteFile(filepath.Join(dir, name, "SKILL.md"), []byte(md), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b, _, err := catalog.Pack(dir)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// SKL-03, SKL-04, SKL-06…SKL-09, VOC-04, VOC-05, VOC-08, VOC-09
func TestChatSkillsAndVoice(t *testing.T) {
	ctx := context.Background()
	r := newChatRig(t)
	uid := newUser(t, "skl-"+uuid.NewString()[:6]+"@x.org")
	yes, no := true, false
	mk := func(name string, published *bool, skills map[string]string) uuid.UUID {
		it, err := r.cat.Save(ctx, nil, catalog.Input{Type: "skill", Name: "skills-" + uuid.NewString()[:6], Title: ptr(name), Source: &catalog.Source{Kind: "upload"}, Published: &yes})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.cat.Upload(ctx, it.ID, strings.NewReader(string(packSkills(t, skills)))); err != nil {
			t.Fatal(err)
		}
		if _, err := r.cat.Connect(ctx, uid, it.ID, ""); err != nil {
			t.Fatal(err)
		}
		if published != nil && !*published {
			if _, err := r.cat.Save(ctx, &it.ID, catalog.Input{Published: published}); err != nil {
				t.Fatal(err)
			}
		}
		return it.ID
	}
	mk("Юристы", nil, map[string]string{"contract-review": "Проверка договора", "nda-check": "Проверка NDA"})
	reports := mk("Отчёты", nil, map[string]string{"weekly-report": "Еженедельный отчёт", "contract-review": "Другая версия"})
	mk("Черновики", &no, map[string]string{"draft-skill": "Не опубликован"})

	// SKL-06, SKL-07: skills of the published items, a name once
	list, err := r.api.Skills(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, s := range list {
		names = append(names, s.Name)
	}
	if strings.Join(names, ",") != "contract-review,nda-check,weekly-report" || list[1].Description != "Проверка NDA" {
		t.Fatalf("SKL-06: %+v", list)
	}
	// SKL-04
	if _, err := r.api.PostMessage(ctx, uid, nil, chat.PostInput{Text: "x", Channel: domain.ChannelWeb, Skill: "draft-skill"}); !isCodeErr(err, "skill_unavailable") {
		t.Fatalf("SKL-04: %v", err)
	}
	// SKL-03, SKL-07: the skill command; the merged snapshot has the skills of both items
	if _, err := r.api.PostMessage(ctx, uid, nil, chat.PostInput{Text: "проверь contracts/a.docx", Channel: domain.ChannelWeb, Skill: "weekly-report"}); err != nil {
		t.Fatal(err)
	}
	r.drain()
	p := r.op.last()
	if !strings.HasPrefix(p.Text, "/skill:weekly-report ") || !strings.Contains(p.Text, "contracts/a.docx") {
		t.Fatalf("SKL-03: %q", p.Text)
	}
	req := r.op.opened[len(r.op.opened)-1]
	if req.Skills == nil || strings.Join(req.Skills.Names, ",") != "contract-review,nda-check,weekly-report" {
		t.Fatalf("SKL-07: %+v", req.Skills)
	}
	bundle, err := r.cat.Bundle(ctx, req.Skills.Hash)
	if err != nil || !strings.Contains(string(gunzip(t, bundle)), "MARKER-weekly-report") || !strings.Contains(string(gunzip(t, bundle)), "MARKER-nda-check") {
		t.Fatalf("SKL-07: the merged snapshot %v", err)
	}
	var skill *string
	_ = pool.QueryRow(ctx, `SELECT skill FROM messages m JOIN conversations c ON c.id = m.conversation_id WHERE c.user_id = $1 AND m.role = 'user' ORDER BY m.created_at DESC LIMIT 1`, uid).Scan(&skill)
	if skill == nil || *skill != "weekly-report" {
		t.Fatal("SKL-03: the skill mark of the message")
	}

	// SKL-08: the commands of a messenger
	tg := func(text string, atts ...uuid.UUID) error {
		return r.api.Receive(ctx, channels.Inbound{UserID: uid, Channel: domain.ChannelTelegram, Text: text, Attachments: atts})
	}
	if err := tg("/skills"); err != nil {
		t.Fatal(err)
	}
	if out := r.outbound(); len(out) != 1 || !strings.Contains(out[0], "weekly-report — Еженедельный отчёт") {
		t.Fatalf("SKL-08 /skills: %v", out)
	}
	if err := tg("/skill nope x"); err != nil {
		t.Fatal(err)
	}
	if out := r.outbound(); len(out) != 1 || !strings.Contains(out[0], "nope") || !strings.Contains(out[0], "contract-review") {
		t.Fatalf("SKL-08 /skill nope: %v", out)
	}
	if err := tg("/skill contract-review проверь последний договор"); err != nil {
		t.Fatal(err)
	}
	r.drain()
	if p := r.op.last(); !strings.HasPrefix(p.Text, "/skill:contract-review ") {
		t.Fatalf("SKL-08 /skill: %q", p.Text)
	}
	r.outbound()
	if err := tg("/new@nabu_bot"); err != nil {
		t.Fatal(err)
	}
	r.drain()
	if out := r.outbound(); len(out) != 1 || out[0] != "— Context reset —" {
		t.Fatalf("SKL-08 /new: %v", out)
	}

	// VOC-04, VOC-08: a voice message becomes its text
	w := &fakeWhisper{}
	r.api.Whisper = w
	_, _ = pool.Exec(ctx, `UPDATE users SET language = 'ru' WHERE id = $1`, uid)
	voice, _ := r.api.Attachments().Store(ctx, uid, "voice.ogg", "audio/ogg", strings.NewReader("ogg"), 3)
	if err := tg("", voice); err != nil {
		t.Fatal(err)
	}
	var text string
	var isVoice bool
	_ = pool.QueryRow(ctx, `SELECT m.text, m.voice FROM messages m JOIN conversations c ON c.id = m.conversation_id WHERE c.user_id = $1 AND m.role = 'user'
		ORDER BY m.created_at DESC LIMIT 1`, uid).Scan(&text, &isVoice)
	if !isVoice || !strings.Contains(text, "позвонить Ивану") || w.lang != "ru" {
		t.Fatalf("VOC-08: %q %v %q", text, isVoice, w.lang)
	}
	r.drain()
	if p := r.op.last(); !strings.Contains(p.Text, "[Voice message") || !strings.Contains(p.Text, "позвонить Ивану") {
		t.Fatalf("R21: the agent gets the mark of a voice message: %q", p.Text)
	}
	// VOC-09: the recognizer is down — the file goes to the agent and the bot says so
	w.fail = true
	r.outbound()
	voice2, _ := r.api.Attachments().Store(ctx, uid, "voice.ogg", "audio/ogg", strings.NewReader("ogg"), 3)
	if err := tg("", voice2); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SELECT m.text, m.voice FROM messages m JOIN conversations c ON c.id = m.conversation_id WHERE c.user_id = $1 AND m.role = 'user'
		ORDER BY m.created_at DESC LIMIT 1`, uid).Scan(&text, &isVoice)
	if out := r.outbound(); isVoice || text != "" || len(out) != 1 || !strings.Contains(out[0], "распознать") {
		t.Fatalf("VOC-09: %q %v %v", text, isVoice, out)
	}
	r.drain()
	r.outbound()
	// VOC-05: longer than the limit (2 s in the rig) by the header of a WAV — not recognized, the user is told
	w.fail = false
	long := wavOf(3)
	longVoice, _ := r.api.Attachments().Store(ctx, uid, "voice.wav", "audio/wav", bytes.NewReader(long), int64(len(long)))
	w.lang = ""
	if err := tg("", longVoice); err != nil {
		t.Fatal(err)
	}
	if out := r.outbound(); w.lang != "" || len(out) != 1 || !strings.Contains(out[0], "распознать") {
		t.Fatalf("VOC-05: recognized %q, said %v", w.lang, out)
	}
	r.drain()
	r.outbound()

	// tech §4: a skill that left the session between the message and the turn fails the turn, not silently
	if _, err := r.api.PostMessage(ctx, uid, nil, chat.PostInput{Text: "отчёт", Channel: domain.ChannelWeb, Skill: "weekly-report"}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.cat.Save(ctx, &reports, catalog.Input{Published: &no}); err != nil {
		t.Fatal(err)
	}
	prompts := len(r.op.prompts)
	r.drain()
	var status, class string
	_ = pool.QueryRow(ctx, `SELECT m.status, COALESCE(m.error_class,'') FROM messages m JOIN conversations c ON c.id = m.conversation_id
		WHERE c.user_id = $1 AND m.role = 'assistant' ORDER BY m.created_at DESC LIMIT 1`, uid).Scan(&status, &class)
	if status != "failed" || class != "skill_unavailable" || len(r.op.prompts) != prompts {
		t.Fatalf("a skill out of the session: %s %s, prompts %d → %d", status, class, prompts, len(r.op.prompts))
	}

	// SKL-09: in a group the commands act on the conversation of the group and list the skills of the group agent
	gs := &groups.Service{Pool: pool, Users: repo()}
	r.api.Skills = func(ctx context.Context, u uuid.UUID) ([]chat.Skill, error) { return app.ChatSkills(ctx, r.cat, gs, u) }
	var data uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO users (email, created_via) VALUES ($1, 'group') RETURNING id`, "grp-"+uuid.NewString()[:6]+"@groups.nabu").Scan(&data); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO group_agents (channel, external_chat_id, owner_id, data_user_id, skills) VALUES ('telegram', $1, $2, $3, '{nda-check}')`,
		"-100"+uuid.NewString()[:8], uid, data); err != nil {
		t.Fatal(err)
	}
	group := func(text string) {
		if err := r.api.Receive(ctx, channels.Inbound{UserID: data, Channel: domain.ChannelTelegram, Text: text}); err != nil {
			t.Fatal(err)
		}
	}
	group("/skills@nabu_bot")
	if out := r.outbound(); len(out) != 1 || !strings.Contains(out[0], "nda-check") || strings.Contains(out[0], "weekly-report") || strings.Contains(out[0], "contract-review") {
		t.Fatalf("SKL-09 /skills: %v", out)
	}
	umain, _ := r.api.Store.Main(ctx, uid)
	resets := func(conv uuid.UUID) int {
		n := 0
		for _, e := range r.events(conv) {
			if e.Kind == chat.EventReset {
				n++
			}
		}
		return n
	}
	ownerResets := resets(umain.ID)
	group("/new@nabu_bot")
	r.drain()
	gmain, _ := r.api.Store.Main(ctx, data)
	if evs := r.events(gmain.ID); len(evs) != 1 || evs[0].Kind != chat.EventReset {
		t.Fatalf("SKL-09 /new: %+v", evs)
	}
	if resets(umain.ID) != ownerResets {
		t.Fatal("SKL-09: the conversation of the owner was reset")
	}
}

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(zr)
	return out
}

// MDL-09, MDL-10, MDL-16: the refresh of the models of a connection.
func TestModelsRefresh(t *testing.T) {
	ctx := context.Background()
	var calls int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			mu.Lock()
			calls++
			mu.Unlock()
			_, _ = io.WriteString(w, `{"data":[{"id":"kept"},{"id":"brand-new"}]}`)
			return
		}
		w.WriteHeader(404)
	}))
	defer srv.Close()
	box, _ := crypto.NewBox(key)
	ms := &models.Service{Pool: pool, Box: box}
	var mt int64 = 4242
	c, err := ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "Refresh " + uuid.NewString()[:6], BaseURL: srv.URL + "/v1",
		APIKey: "sk-refresh-1", Models: []models.ModelRef{{ID: "kept", Overrides: &models.CapsPatch{MaxTokens: &mt}}, {ID: "old-gone"}}})
	if err != nil {
		t.Fatal(err)
	}
	// MDL-16: models of a connection work as agents
	if !c.Models[0].Tools {
		t.Fatal("MDL-16")
	}
	c, err = ms.Refresh(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]agent.ModelDef{}
	for _, m := range c.Models {
		by[m.ID] = m
	}
	if !by["old-gone"].Gone || by["kept"].Gone || by["kept"].MaxTokens != 4242 || len(c.Models) != 2 {
		t.Fatalf("MDL-09: %+v", c.Models)
	}
	if c.NewCount != 1 || c.Discovered[0].ID != "brand-new" {
		t.Fatalf("MDL-09: new %+v", c.Discovered)
	}
	// adding the new model clears its mark
	c, err = ms.Update(ctx, c.ID, models.Patch{Models: &[]models.ModelRef{{ID: "kept"}, {ID: "brand-new"}}})
	if err != nil || c.NewCount != 0 || len(c.Models) != 2 || c.Models[0].MaxTokens != 4242 {
		t.Fatalf("add the new model: %+v %v", c, err)
	}
	// MDL-10: two workers — one refresh within the hour
	_, _ = pool.Exec(ctx, `UPDATE model_connections SET refreshed_at = NULL WHERE id = $1`, c.ID)
	mu.Lock()
	calls = 0
	mu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); ms.RefreshAll(ctx) }()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("MDL-10: %d refreshes", calls)
	}
}

// MDL-07, MDL-08: the probes of a check report the capabilities; a confirmed
// tool call becomes the source, an unconfirmed image changes nothing.
func TestModelProbes(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	ms := &models.Service{Pool: pool, Box: box}
	yes := true
	conn, err := ms.Create(ctx, models.Input{Type: models.TypeOpenAICompatible, Name: "Probes " + uuid.NewString()[:6], BaseURL: "http://llm.invalid",
		APIKey: "sk-probe-0001", Models: []models.ModelRef{{ID: "probe-x", Overrides: &models.CapsPatch{Image: &yes}}}})
	if err != nil {
		t.Fatal(err)
	}
	if conn.Models[0].Sources[models.FTools] != models.SrcDefault {
		t.Fatalf("the tools of an unknown model are expected from the defaults: %+v", conn.Models[0].Sources)
	}
	var asked []string
	ms.Check = func(_ context.Context, req agent.LLMCheckRequest) (*agent.LLMCheckResponse, error) {
		asked = req.Probes
		return &agent.LLMCheckResponse{Results: []agent.LLMCheckResult{{Model: "probe-x", OK: true,
			Caps: map[string]string{agent.ProbeImage: agent.ProbeUnconfirmed, agent.ProbeTools: agent.ProbeConfirmed}}}}, nil
	}
	res, err := ms.CheckConnection(ctx, conn.ID)
	if err != nil || len(res.Results) != 1 || res.Results[0].Caps[agent.ProbeImage] != agent.ProbeUnconfirmed || !slices.Contains(asked, agent.ProbeImage) {
		t.Fatalf("MDL-07: %+v %v (probes %v)", res, err, asked)
	}
	after, _ := ms.Get(ctx, conn.ID)
	m := after.Models[0]
	if !m.HasInput("image") || m.Sources[models.FImage] != models.SrcManual {
		t.Fatalf("MDL-07: the check switched off the image: %+v", m)
	}
	if !m.Tools || m.Sources[models.FTools] != models.SrcProbe {
		t.Fatalf("MDL-08: %+v", m.Sources)
	}
}

// GC-09: Pi gets a token of the bridge, never the authorization key of GigaChat.
func TestGigaChatKeyStaysInAPI(t *testing.T) {
	ctx := context.Background()
	box, _ := crypto.NewBox(key)
	const secret = "R2lnYUNoYXQtYXV0aG9yaXphdGlvbi1rZXk="
	enc, _ := box.Seal(secret)
	var id uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO model_connections (name, type, base_url, api_key_enc, options, models) VALUES ($1, 'gigachat', 'https://gigachat.devices.sberbank.ru/api/v1', $2,
		'{"scope":"GIGACHAT_API_PERS"}', '[{"id":"GigaChat-2-Max","tools":true,"contextWindow":128000}]') RETURNING id`, "GigaChat "+uuid.NewString()[:6], enc).Scan(&id); err != nil {
		t.Fatal(err)
	}
	ms := &models.Service{Pool: pool, Box: box, Signer: jwt.NewSigner("https://nabu.test", key), InternalURL: "http://api:8081"}
	spec, tok, err := ms.Resolve(ctx, models.Choice{ConnectionID: id, Model: "GigaChat-2-Max"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(spec)
	if strings.Contains(string(raw)+tok, secret) || !strings.HasPrefix(spec.BaseURL, "http://api:8081/internal/llm/gigachat/"+id.String()) || !ms.VerifyBridge(tok, id) {
		t.Fatalf("GC-09: %s %q", raw, tok)
	}
	if c, err := ms.BridgeConn(ctx, id); err != nil || c.AuthKey != secret {
		t.Fatalf("the bridge reads the key in api: %+v %v", c, err)
	}
}

// wavOf is a WAV recording of the given length: 16 kHz mono 16-bit.
func wavOf(seconds int) []byte {
	const rate = 32000
	var b bytes.Buffer
	b.WriteString("RIFF")
	_ = binary.Write(&b, binary.LittleEndian, uint32(36+seconds*rate))
	b.WriteString("WAVEfmt ")
	for _, v := range []any{uint32(16), uint16(1), uint16(1), uint32(16000), uint32(rate), uint16(2), uint16(16)} {
		_ = binary.Write(&b, binary.LittleEndian, v)
	}
	b.WriteString("data")
	_ = binary.Write(&b, binary.LittleEndian, uint32(seconds*rate))
	b.Write(make([]byte, seconds*rate))
	return b.Bytes()
}

// albumBot is a messenger with albums (Telegram) for the delivery of files;
// it records what it sent.
type albumBot struct {
	mu   sync.Mutex
	sent []string // "album:<names>" or "file:<name>"
}

func (*albumBot) Channel() string                            { return domain.ChannelVKTeams }
func (*albumBot) Send(context.Context, string, string) error { return nil }
func (*albumBot) Typing(context.Context, string)             {}
func (*albumBot) FetchFile(context.Context, string) (io.ReadCloser, string, error) {
	return nil, "", errors.New("no")
}

func (b *albumBot) SendFile(ctx context.Context, _ string, f channels.OutFile) error {
	rc, err := f.Open(ctx)
	if err != nil {
		return err
	}
	rc.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sent = append(b.sent, "file:"+f.Name)
	return nil
}

func (b *albumBot) SendAlbum(ctx context.Context, _ string, _ bool, files []channels.OutFile) error {
	var names []string
	for _, f := range files {
		rc, err := f.Open(ctx)
		if err != nil {
			return err
		}
		rc.Close()
		names = append(names, f.Name)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sent = append(b.sent, "album:"+strings.Join(names, ","))
	return nil
}

// FTR.NAB.CHAT-0001 R33: the agent sends files of its space; they are attached
// to the answer and go to the messenger after the text: photos as one album,
// documents apart; a run of a task keeps its files for its result.
func TestChatAgentFiles(t *testing.T) {
	ctx := context.Background()
	r := newChatRig(t)
	bot := &albumBot{}
	r.eng.Adapters = map[string]channels.Adapter{domain.ChannelTelegram: bot, domain.ChannelVKTeams: bot}
	uid := newUser(t, "att-"+uuid.NewString()[:6]+"@x.org")
	files := map[string][]byte{"reports/summary.md": []byte("# Итоги\n"), "big.bin": make([]byte, 2<<20),
		"a.png": []byte("\x89PNG a"), "b.png": []byte("\x89PNG b")}
	tool := r.api.AttachTool(func(_ context.Context, _ uuid.UUID, p string, max int64) ([]byte, error) {
		b, ok := files[p]
		if !ok {
			return nil, &mcp.ToolError{Msg: "no such file in your space: " + p}
		}
		if int64(len(b)) > max {
			return nil, &mcp.ToolError{Msg: "the file is larger than 1 MB"}
		}
		return b, nil
	})
	main, err := r.api.Store.Main(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	g := mcp.Grant{UserID: uid, Conversation: main.ID, Channel: domain.ChannelTelegram}
	if tool.Available(ctx, mcp.Grant{UserID: uid}) {
		t.Fatal("ATT-05: chat_attach is offered to a session with neither a conversation nor a task")
	}
	call := func(g mcp.Grant, p string) (string, error) {
		return tool.Handler(ctx, g, json.RawMessage(`{"path":"`+p+`"}`))
	}
	if _, err := call(g, "reports/summary.md"); err == nil || !strings.Contains(err.Error(), "no answer is being written in this conversation") {
		t.Fatalf("ATT-04: a file is attached with no answer being written: %v", err)
	}
	var results []string
	var errs []error
	r.op.during = func() {
		for _, p := range []string{"reports/summary.md", "missing.txt", "big.bin", "a.png", "b.png"} {
			out, err := call(g, p)
			results, errs = append(results, out), append(errs, err)
		}
	}
	if err := r.api.Receive(ctx, channels.Inbound{UserID: uid, Channel: domain.ChannelTelegram, Text: "пришли итоги файлом"}); err != nil {
		t.Fatal(err)
	}
	r.drain()
	r.op.during = nil
	if errs[0] != nil || !strings.Contains(results[0], "summary.md") {
		t.Fatalf("ATT-01: %q %v", results[0], errs[0])
	}
	if errs[1] == nil || !strings.Contains(errs[1].Error(), "missing.txt") || errs[2] == nil || !strings.Contains(errs[2].Error(), "larger") {
		t.Fatalf("ATT-03: %v / %v", errs[1], errs[2])
	}
	answerFiles := func(conv uuid.UUID) []chat.Attachment {
		var atts []chat.Attachment
		var b []byte
		if err := pool.QueryRow(ctx, `SELECT attachments FROM messages WHERE conversation_id = $1 AND role = 'assistant' ORDER BY created_at DESC LIMIT 1`, conv).Scan(&b); err != nil {
			t.Fatal(err)
		}
		_ = json.Unmarshal(b, &atts)
		return atts
	}
	atts := answerFiles(main.ID)
	if len(atts) != 3 || atts[0].FileName != "summary.md" || atts[0].MimeType != "text/markdown" || atts[0].Size != int64(len(files["reports/summary.md"])) ||
		atts[1].MimeType != "image/png" {
		t.Fatalf("ATT-01: the answer carries %+v", atts)
	}
	// ATT-02, ATT-12: the text, then the photos as one album, then the document alone
	r.bus.mu.Lock()
	var outs []engine.Outbound
	for _, m := range r.bus.msgs {
		if v, ok := strings.CutPrefix(m, kafka.TopicOutbound+" "); ok {
			var o engine.Outbound
			_ = json.Unmarshal([]byte(v), &o)
			outs = append(outs, o)
		}
	}
	r.bus.msgs = nil
	r.bus.mu.Unlock()
	if len(outs) != 3 || outs[0].Text != "ok" || len(outs[0].Attachments) != 0 ||
		!outs[1].Photos || len(outs[1].Attachments) != 2 || outs[1].Attachments[0] != atts[1].ID ||
		outs[2].Photos || len(outs[2].Attachments) != 1 || outs[2].Attachments[0] != atts[0].ID || outs[2].Text != "" {
		t.Fatalf("ATT-02, ATT-12: outbound %+v", outs)
	}

	// ATT-13: a run of a task keeps the files for its result; an earlier attempt's are dropped
	ts := &tasks.Service{Pool: pool, Events: r.ev, Bus: r.bus, Rules: tasks.Rules{MinInterval: 15 * time.Minute, MaxActive: 20},
		Catchup: time.Hour, Tick: time.Second, MaxFailures: 3, DefaultTimezone: "UTC"}
	r.eng.Tasks = ts
	r.eng.Cfg.TaskRunTimeout = time.Minute
	in := tasks.CreateInput{Title: "Отчёт", Instruction: "пришли отчёт"}
	in.Schedule.Once = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	tk, err := ts.Create(ctx, uid, in, "web", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `UPDATE scheduled_tasks SET next_run_at = now() - interval '1 minute' WHERE id = $1`, tk.ID)
	if n, err := ts.Due(ctx); err != nil || n != 1 {
		t.Fatalf("due %d %v", n, err)
	}
	var runID uuid.UUID
	_ = pool.QueryRow(ctx, `SELECT id FROM task_runs WHERE task_id = $1`, tk.ID).Scan(&runID)
	tg := mcp.Grant{UserID: uid, TaskID: tk.ID}
	if !tool.Available(ctx, tg) {
		t.Fatal("ATT-13: chat_attach is not offered to a run of a task")
	}
	if _, err := call(tg, "big.bin"); err == nil {
		t.Fatal("ATT-03: a large file in a run")
	}
	if _, err := call(tg, "a.png"); err != nil { // the attempt before a restart
		t.Fatal(err)
	}
	var stale []chat.Attachment
	var sb []byte
	_ = pool.QueryRow(ctx, `SELECT attachments FROM task_runs WHERE id = $1`, runID).Scan(&sb)
	_ = json.Unmarshal(sb, &stale)
	r.op.during = func() {
		if _, err := call(tg, "reports/summary.md"); err != nil {
			t.Error(err)
		}
	}
	payload, _ := json.Marshal(tasks.RunMessage{RunID: runID, TaskID: tk.ID, UserID: uid})
	if err := r.eng.HandleTaskRun(ctx, nil, payload); err != nil {
		t.Fatal(err)
	}
	r.op.during = nil
	got := answerFiles(main.ID)
	if len(stale) != 1 || len(got) != 1 || got[0].FileName != "summary.md" {
		t.Fatalf("ATT-13: stale %+v, the result carries %+v", stale, got)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM attachments WHERE id = $1`, stale[0].ID).Scan(&n)
	if n != 0 {
		t.Fatal("ATT-13: the file of the earlier attempt stays")
	}

	// ATT-17: an album that lost a file goes file by file without it, and is not retried
	if _, err := pool.Exec(ctx, `INSERT INTO channel_contacts (channel, user_id, chat_id) VALUES ('vkteams', $1, '42') ON CONFLICT DO NOTHING`, uid); err != nil {
		t.Fatal(err)
	}
	if key, err := r.api.Store.DeleteAttachment(ctx, uid, atts[2].ID); err != nil || r.s3.Delete(ctx, key) != nil {
		t.Fatal(err)
	}
	ob, _ := json.Marshal(engine.Outbound{UserID: uid, Channel: domain.ChannelVKTeams, Attachments: []uuid.UUID{atts[1].ID, atts[2].ID}, Photos: true})
	if err := r.eng.HandleOutbound(ctx, nil, ob); err != nil {
		t.Fatalf("ATT-17: %v", err)
	}
	if len(bot.sent) != 1 || bot.sent[0] != "file:"+atts[1].FileName {
		t.Fatalf("ATT-17: sent %q", bot.sent)
	}

	// ATT-18: at most 10 files with an answer
	ans, err := r.api.Store.AddAssistant(ctx, main.ID, domain.ChannelWeb, nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range chat.MaxAgentFiles {
		if _, err := call(g, "a.png"); err != nil {
			t.Fatalf("ATT-18: file %d: %v", i+1, err)
		}
	}
	if _, err := call(g, "a.png"); err == nil || !strings.Contains(err.Error(), "at most 10 files") {
		t.Fatalf("ATT-18: the 11th file: %v", err)
	}
	if _, err := r.api.Store.Finish(ctx, ans.ID, "", "done", nil, "", ""); err != nil {
		t.Fatal(err)
	}

	// SEC-05: the file the agent sent is downloaded by its user only
	router := chi.NewRouter()
	as := func(u uuid.UUID) func(http.Handler) http.Handler {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				next.ServeHTTP(w, req.WithContext(httpx.WithPrincipal(req.Context(), &domain.Principal{UserID: u})))
			})
		}
	}
	other := newUser(t, "att-other-"+uuid.NewString()[:6]+"@x.org")
	router.Route("/own", func(rr chi.Router) { rr.Use(as(uid)); r.api.Routes(rr, true) })
	router.Route("/other", func(rr chi.Router) { rr.Use(as(other)); r.api.Routes(rr, true) })
	get := func(prefix string) (int, http.Header) {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, prefix+"/attachments/"+atts[0].ID.String(), nil))
		return w.Code, w.Header()
	}
	if code, h := get("/own"); code != http.StatusOK || h.Get("Content-Type") != "application/octet-stream" || h.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("SEC-04, SEC-05: own %d %v", code, h)
	}
	if code, _ := get("/other"); code != http.StatusNotFound {
		t.Fatalf("SEC-05: another user gets %d", code)
	}
}
