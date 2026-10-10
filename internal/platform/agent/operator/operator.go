// Package operator is the agent operator — the `nabu agent` mode
// (FTR.NAB.CMN-0001 arch §3, ported from FTR.HMR.CMN-0004). It runs one Pi
// process per session behind the internal API agent:8090/v1. The pod has no
// database, Kafka, object storage or Nabu secrets: everything a session needs
// comes in the request from the worker.
package operator

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/pi"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// Config configures the operator.
type Config struct {
	Runtime      pi.Runtime
	WorkDir      string
	ServiceToken string
	MaxSessions  int
	MaxRuns      int
	IdleTimeout  time.Duration
	// MaxBody bounds request bodies (skills bundles, snapshots, images).
	MaxBody int64
	// Mode is all (one operator for everything: development), pool (service
	// agents and checks) or owner (the pod of one owner): FTR.NAB.CMN-0004
	// tech §3.1.
	Mode string
	// Owner, Generation and PublicKey identify the pod of an owner: it admits
	// only tokens the worker signed for this owner and this start of the pod.
	Owner      string
	Generation int64
	PublicKey  ed25519.PublicKey
	Version    string
}

// Modes of the operator.
const (
	ModeAll   = "all"
	ModePool  = "pool"
	ModeOwner = "owner"
)

// Operator runs Pi sessions.
type Operator struct {
	cfg    Config
	skills *skillCache

	mu       sync.Mutex
	sessions map[string]*entry
	counts   map[agent.SessionKind]int
}

type entry struct {
	id, token string
	kind      agent.SessionKind
	conn      string
	label     string
	s         *pi.Session

	run  sync.Mutex // one prompt at a time
	mu   sync.Mutex
	last time.Time
	busy bool
}

// New creates the operator; WorkDir must be writable.
func New(cfg Config) (*Operator, error) {
	switch cfg.Mode {
	case "":
		cfg.Mode = ModeAll
	case ModeAll, ModePool, ModeOwner:
	default:
		return nil, errors.New("AGENT_MODE must be all, pool or owner")
	}
	if cfg.Mode == ModeOwner {
		if cfg.Owner == "" || cfg.Generation <= 0 || len(cfg.PublicKey) != ed25519.PublicKeySize {
			return nil, errors.New("AGENT_OWNER, AGENT_GENERATION and AGENT_JWT_PUBLIC_KEY are required in the owner mode")
		}
		cfg.ServiceToken = "" // a pod of an owner has no shared secret
	} else if cfg.ServiceToken == "" {
		return nil, errors.New("AGENT_SERVICE_TOKEN is required")
	}
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = 100
	}
	if cfg.MaxRuns <= 0 {
		cfg.MaxRuns = 10
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 15 * time.Minute
	}
	if cfg.MaxBody <= 0 {
		cfg.MaxBody = 64 << 20
	}
	if len(cfg.Runtime.Command) == 0 {
		return nil, errors.New("the Pi command is empty (PI_BINARY)")
	}
	if err := os.MkdirAll(filepath.Join(cfg.WorkDir, "sessions"), 0o700); err != nil {
		return nil, fmt.Errorf("AGENT_WORKDIR: %w", err)
	}
	// One empty working directory for all sessions: Pi records it in session
	// files, so restored sessions find it again (also after a pod restart).
	cfg.Runtime.CWD = filepath.Join(cfg.WorkDir, "cwd")
	if err := os.MkdirAll(cfg.Runtime.CWD, 0o700); err != nil {
		return nil, fmt.Errorf("AGENT_WORKDIR: %w", err)
	}
	return &Operator{cfg: cfg, skills: &skillCache{dir: filepath.Join(cfg.WorkDir, "skills")},
		sessions: map[string]*entry{}, counts: map[agent.SessionKind]int{}}, nil
}

// Handler serves /v1.
func (o *Operator) Handler() http.Handler {
	r := chi.NewRouter()
	r.Route("/v1", func(r chi.Router) {
		r.With(o.service).Post("/sessions", o.open)
		r.Route("/sessions/{id}", func(r chi.Router) {
			r.With(o.sessionOrService).Post("/prompt", o.prompt)
			r.With(o.sessionOrService).Post("/abort", o.abort)
			r.With(o.sessionOrService).Delete("/", o.close)
			r.With(o.service).Patch("/", o.patch)
			r.With(o.service).Get("/snapshot", o.snapshot)
		})
		if o.cfg.Mode == ModeOwner {
			r.With(o.service).Get("/status", o.status)
		} else {
			r.With(o.service).Post("/checks/llm", o.checkLLM)
			r.With(o.service).Post("/checks/mcp", o.checkMCP)
		}
	})
	return r
}

// ─── auth (PI-11) ───────────────────────────────────────────────────

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if t, ok := strings.CutPrefix(h, "Bearer "); ok {
		return strings.TrimSpace(t)
	}
	return ""
}

func equal(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

// podToken admits the token of this pod: signed by Nabu for this owner and
// this generation (TOK-01). A token of another pod is rejected, so a request
// sent to a reused address never runs here (TOK-02).
func (o *Operator) podToken(tok string) bool {
	c, err := jwt.VerifyWith(tok, o.cfg.PublicKey, "", jwt.AudAgent, time.Now())
	return err == nil && c.Subject == o.cfg.Owner && c.Generation == o.cfg.Generation
}

func (o *Operator) owner(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !o.podToken(bearer(r)) {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "a valid token of this pod is required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (o *Operator) service(next http.Handler) http.Handler {
	if o.cfg.Mode == ModeOwner {
		return o.owner(next)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !equal(bearer(r), o.cfg.ServiceToken) {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "a valid service token is required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// sessionOrService admits the service token, or the token of the session in
// the path — never a token of another session.
func (o *Operator) sessionOrService(next http.Handler) http.Handler {
	if o.cfg.Mode == ModeOwner {
		return o.owner(next)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tok := bearer(r)
		if tok != "" && equal(tok, o.cfg.ServiceToken) {
			next.ServeHTTP(w, r)
			return
		}
		e := o.get(chi.URLParam(r, "id"))
		switch {
		case tok == "":
			writeErr(w, http.StatusUnauthorized, "unauthorized", "a token is required")
		case e != nil && equal(tok, e.token):
			next.ServeHTTP(w, r)
		case o.isSessionToken(tok):
			writeErr(w, http.StatusForbidden, "forbidden", "the token belongs to another session")
		default:
			writeErr(w, http.StatusUnauthorized, "unauthorized", "invalid token")
		}
	})
}

func (o *Operator) isSessionToken(tok string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, e := range o.sessions {
		if equal(tok, e.token) {
			return true
		}
	}
	return false
}

// ─── sessions ───────────────────────────────────────────────────────

func (o *Operator) get(id string) *entry {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.sessions[id]
}

func (o *Operator) limit(k agent.SessionKind) int {
	if k == agent.KindRun {
		return o.cfg.MaxRuns
	}
	return o.cfg.MaxSessions
}

func validate(req *agent.SessionRequest) error {
	if req.Kind == "" {
		req.Kind = agent.KindChat
	}
	if req.Kind != agent.KindChat && req.Kind != agent.KindRun {
		return fmt.Errorf("unknown kind %q", req.Kind)
	}
	if req.Workspace != nil && (req.Workspace.URL == "" || req.Workspace.Token == "") {
		return errors.New("workspace url and token are required")
	}
	if req.Snapshot != nil && len(req.History) > 0 {
		return errors.New("snapshot and history are mutually exclusive")
	}
	m := req.Model
	if m.Provider == "" || m.API == "" || m.BaseURL == "" || m.ModelID == "" || len(m.Models) == 0 {
		return errors.New("model provider, api, baseUrl, modelId and models are required")
	}
	if req.Secrets.LLMKey == "" {
		return errors.New("secrets.llmKey is required")
	}
	seen := map[string]bool{}
	for _, s := range req.MCP {
		if s.Name == "" || s.URL == "" || seen[s.Name] {
			return fmt.Errorf("invalid MCP server %q", s.Name)
		}
		seen[s.Name] = true
	}
	return nil
}

func (o *Operator) open(w http.ResponseWriter, r *http.Request) {
	var req agent.SessionRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, o.cfg.MaxBody)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if err := validate(&req); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_session", err.Error())
		return
	}
	// POD-05: the pool runs service agents, a pod of an owner — conversations.
	if (o.cfg.Mode == ModePool && req.Kind != agent.KindRun) || (o.cfg.Mode == ModeOwner && req.Kind != agent.KindChat) {
		writeErr(w, http.StatusBadRequest, "invalid_request", "sessions of the kind "+string(req.Kind)+" do not run in the "+o.cfg.Mode+" mode")
		return
	}
	skillsDir := ""
	if req.Skills != nil && req.Skills.Hash != "" && len(req.Skills.Names) > 0 {
		dir, err := o.skills.ensure(req.Skills.Hash, req.Skills.Bundle)
		if errors.Is(err, errBundleRequired) {
			writeErr(w, http.StatusConflict, "skills_bundle_required", "send the skills bundle of "+req.Skills.Hash)
			return
		}
		if err != nil {
			writeErr(w, http.StatusUnprocessableEntity, "invalid_skills", err.Error())
			return
		}
		skillsDir = dir
	}
	o.mu.Lock()
	if o.counts[req.Kind] >= o.limit(req.Kind) {
		o.mu.Unlock()
		w.Header().Set("Retry-After", "10")
		writeErr(w, http.StatusServiceUnavailable, "agent_busy", fmt.Sprintf("the agent runs %d %s sessions, retry later", o.limit(req.Kind), req.Kind))
		return
	}
	o.counts[req.Kind]++ // reserved before the slow start
	o.mu.Unlock()
	release := func() {
		o.mu.Lock()
		o.counts[req.Kind]--
		o.mu.Unlock()
	}

	id, tok := randID(), randID()+randID()
	rt := o.cfg.Runtime
	rt.SkillsDir = skillsDir
	rt.Stderr = &logWriter{session: id, label: req.Label}
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	reason := "new"
	if req.Snapshot != nil {
		reason = "restore"
	}
	metrics.AgentProcessStarts.WithLabelValues(reason).Inc()
	s, err := pi.Start(ctx, rt, filepath.Join(o.cfg.WorkDir, "sessions", id), req)
	if err != nil {
		release()
		slog.Error("agent session start failed", "kind", req.Kind, "label", req.Label, "err", err)
		writeErr(w, http.StatusBadGateway, "agent_start_failed", err.Error())
		return
	}
	e := &entry{id: id, token: tok, kind: req.Kind, conn: req.Model.ConnectionID, label: req.Label, s: s, last: time.Now()}
	o.mu.Lock()
	o.sessions[id] = e
	metrics.AgentSessions.WithLabelValues(string(req.Kind)).Set(float64(o.counts[req.Kind]))
	o.mu.Unlock()
	model, thinking := s.Model()
	slog.Info("agent session opened", "session", id, "kind", req.Kind, "model", model, "label", req.Label)
	writeJSON(w, http.StatusCreated, agent.SessionResponse{SessionID: id, SessionToken: tok, Model: model, Thinking: thinking})
}

func (o *Operator) prompt(w http.ResponseWriter, r *http.Request) {
	e := o.get(chi.URLParam(r, "id"))
	if e == nil {
		writeErr(w, http.StatusNotFound, "session_not_found", "no such session")
		return
	}
	var p agent.PromptRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, o.cfg.MaxBody)).Decode(&p); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if !e.run.TryLock() {
		writeErr(w, http.StatusConflict, "session_busy", "the session is already running a prompt")
		return
	}
	defer e.run.Unlock()
	e.touch(true)
	defer e.touch(false)

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	fl, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)
	model, _ := e.s.Model()
	metrics.LLMRequests.WithLabelValues(e.conn, model, string(e.kind)).Inc()
	emit := func(ev agent.Event) {
		switch ev.Type {
		case agent.EventUsage:
			metrics.LLMTokens.WithLabelValues("in").Add(float64(ev.TokensIn))
			metrics.LLMTokens.WithLabelValues("out").Add(float64(ev.TokensOut))
			metrics.LLMTokens.WithLabelValues("cache_read").Add(float64(ev.CacheRead))
			metrics.LLMTokens.WithLabelValues("cache_write").Add(float64(ev.CacheWrite))
			metrics.LLMCost.WithLabelValues(e.conn, model).Add(ev.CostUSD)
		case agent.EventError:
			metrics.LLMErrors.WithLabelValues(string(ev.ErrorClass), e.conn).Inc()
			slog.Warn("agent run failed", "session", e.id, "label", e.label, "class", ev.ErrorClass, "status", ev.HTTPStatus, "message", ev.Message)
		}
		_ = enc.Encode(ev)
		if fl != nil {
			fl.Flush()
		}
	}
	if err := e.s.Prompt(r.Context(), p, emit); err != nil {
		if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
			return // the caller went away or aborted
		}
		emit(agent.Event{Type: agent.EventError, ErrorClass: agent.ErrAgentCrashed, Message: err.Error()})
	}
}

func (e *entry) touch(busy bool) {
	e.mu.Lock()
	e.busy, e.last = busy, time.Now()
	e.mu.Unlock()
}

func (o *Operator) abort(w http.ResponseWriter, r *http.Request) {
	e := o.get(chi.URLParam(r, "id"))
	if e == nil {
		writeErr(w, http.StatusNotFound, "session_not_found", "no such session")
		return
	}
	if err := e.s.Abort(r.Context()); err != nil {
		writeErr(w, http.StatusBadGateway, "abort_failed", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (o *Operator) patch(w http.ResponseWriter, r *http.Request) {
	e := o.get(chi.URLParam(r, "id"))
	if e == nil {
		writeErr(w, http.StatusNotFound, "session_not_found", "no such session")
		return
	}
	var p agent.PatchRequest
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if err := e.s.SetModel(r.Context(), p.ModelID, p.Thinking); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_model", err.Error())
		return
	}
	m, t := e.s.Model()
	writeJSON(w, http.StatusOK, map[string]string{"model": m, "thinking": t})
}

func (o *Operator) snapshot(w http.ResponseWriter, r *http.Request) {
	e := o.get(chi.URLParam(r, "id"))
	if e == nil {
		writeErr(w, http.StatusNotFound, "session_not_found", "no such session")
		return
	}
	b, err := e.s.Snapshot(r.Context())
	if err != nil {
		writeErr(w, http.StatusConflict, "no_snapshot", err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(b)
}

func (o *Operator) close(w http.ResponseWriter, r *http.Request) {
	if !o.drop(chi.URLParam(r, "id"), "closed") {
		writeErr(w, http.StatusNotFound, "session_not_found", "no such session")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// drop closes a session and frees its slot.
func (o *Operator) drop(id, why string) bool {
	o.mu.Lock()
	e, ok := o.sessions[id]
	if ok {
		delete(o.sessions, id)
		o.counts[e.kind]--
		metrics.AgentSessions.WithLabelValues(string(e.kind)).Set(float64(o.counts[e.kind]))
	}
	o.mu.Unlock()
	if !ok {
		return false
	}
	_ = e.s.Close()
	slog.Info("agent session closed", "session", id, "label", e.label, "reason", why)
	return true
}

// Run closes idle sessions until ctx ends, then closes all.
func (o *Operator) Run(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			o.mu.Lock()
			ids := make([]string, 0, len(o.sessions))
			for id := range o.sessions {
				ids = append(ids, id)
			}
			o.mu.Unlock()
			for _, id := range ids {
				o.drop(id, "shutdown")
			}
			return
		case <-t.C:
			o.reap()
		}
	}
}

func (o *Operator) reap() {
	o.mu.Lock()
	var idle []string
	for id, e := range o.sessions {
		e.mu.Lock()
		if !e.busy && time.Since(e.last) > o.cfg.IdleTimeout {
			idle = append(idle, id)
		}
		e.mu.Unlock()
	}
	o.mu.Unlock()
	for _, id := range idle {
		o.drop(id, "idle")
	}
}

// status reports the sessions of the pod to the pod manager.
func (o *Operator) status(w http.ResponseWriter, _ *http.Request) {
	st := agent.Status{Owner: o.cfg.Owner, Generation: o.cfg.Generation, Version: o.cfg.Version, Skills: o.skills.known()}
	o.mu.Lock()
	st.Sessions = len(o.sessions)
	for _, e := range o.sessions {
		e.mu.Lock()
		if e.busy {
			st.Busy++
		}
		if e.last.After(st.LastActivity) {
			st.LastActivity = e.last
		}
		e.mu.Unlock()
	}
	o.mu.Unlock()
	writeJSON(w, http.StatusOK, st)
}

// ─── checks ─────────────────────────────────────────────────────────

func (o *Operator) checkLLM(w http.ResponseWriter, r *http.Request) {
	var req agent.LLMCheckRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	metrics.AgentProcessStarts.WithLabelValues("check").Inc()
	rt := o.cfg.Runtime
	rt.Stderr = &logWriter{session: "check", label: "llm_check"}
	writeJSON(w, http.StatusOK, pi.CheckLLM(r.Context(), rt, filepath.Join(o.cfg.WorkDir, "sessions", "check-"+randID()), req))
}

func (o *Operator) checkMCP(w http.ResponseWriter, r *http.Request) {
	var req agent.MCPCheckRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, pi.CheckMCP(r.Context(), o.cfg.Runtime, filepath.Join(o.cfg.WorkDir, "sessions", "check-"+randID()), req))
}

// ─── helpers ────────────────────────────────────────────────────────

func randID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, errCode, msg string) {
	writeJSON(w, code, map[string]string{"error": errCode, "message": msg})
}

// logWriter forwards Pi's stderr (already masked) to the operator's log.
type logWriter struct {
	session, label string
	buf            []byte
	mu             sync.Mutex
}

func (l *logWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf = append(l.buf, p...)
	for {
		i := strings.IndexByte(string(l.buf), '\n')
		if i < 0 {
			if len(l.buf) > 8<<10 {
				i = len(l.buf)
			} else {
				break
			}
		}
		line := strings.TrimSpace(string(l.buf[:i]))
		if i < len(l.buf) {
			l.buf = l.buf[i+1:]
		} else {
			l.buf = l.buf[:0]
		}
		if line != "" {
			slog.Info("pi", "session", l.session, "label", l.label, "stderr", line)
		}
	}
	return len(p), nil
}

var _ io.Writer = (*logWriter)(nil)

// RetryAfter parses the Retry-After of agent_busy for clients.
func RetryAfter(h http.Header) time.Duration {
	if n, err := strconv.Atoi(h.Get("Retry-After")); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return 10 * time.Second
}
