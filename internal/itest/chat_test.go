//go:build integration

package itest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/chat"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/engine"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/memory"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/space"
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
	tokens, window := f.tokens, f.window
	f.mu.Unlock()
	if auto {
		onEvent(agent.Event{Type: agent.EventCompaction, Auto: true, Summary: "auto summary", TokensBefore: 190_000, TokensAfter: 8_000})
	}
	onEvent(agent.Event{Type: agent.EventTextDelta, Delta: "ok"})
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
		MaxVoice: 64 << 10}
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

// CTX-01, CTX-02, CTX-05…CTX-09, CTX-11, SEL-01…SEL-03, MDL-06, MDL-11, MDL-12
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
	if _, err := r.api.Reset(ctx, uid, nil, domain.ChannelWeb); err != nil {
		t.Fatal(err)
	}
	r.drain()
	if _, ok := r.s3.m["sessions/"+main.ID.String()+".jsonl"]; ok {
		t.Fatal("CTX-01: the snapshot outlived the reset")
	}
	if f := r.api.Store.Context(ctx, main.ID); f.Tokens != 0 {
		t.Fatalf("CTX-01: fill after reset %+v", f)
	}
	opened := len(r.op.opened)
	r.api.Post(ctx, uid, nil, "о чём мы говорили?", domain.ChannelWeb, nil, nil, nil) //nolint:errcheck
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
	if evs := r.events(main.ID); evs[len(evs)-1].Kind != chat.EventModel || evs[len(evs)-1].Model != "vision-1" {
		t.Fatalf("SEL-03: %+v", evs)
	}
	att2, _ := r.api.Attachments().Store(ctx, uid, "shot2.png", "image/png", strings.NewReader("png-bytes"), 9)
	r.api.Post(ctx, uid, nil, "а теперь?", domain.ChannelWeb, nil, []uuid.UUID{att2}, nil) //nolint:errcheck
	r.drain()
	if p := r.op.last(); len(p.Images) != 1 {
		t.Fatalf("MDL-11: %d images", len(p.Images))
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

// SKL-03, SKL-04, SKL-06, SKL-07, SKL-08, VOC-04, VOC-08, VOC-09
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
	mk("Отчёты", nil, map[string]string{"weekly-report": "Еженедельный отчёт", "contract-review": "Другая версия"})
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
