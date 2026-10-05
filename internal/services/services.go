// Package services is the service agents of Nabu and their runs
// (FTR.NAB.CMN-0001 R15–R18, arch §4.3, tech §4–5): administrators configure
// agents; service clients start runs through the client API, follow the
// events and get the result. The worker executes runs (engine).
package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Workspace modes of a service agent (R15).
const (
	WorkspaceNone     = "none"
	WorkspaceNabu     = "nabu"
	WorkspaceExternal = "external"
)

// Limits bound a run (SVC-06).
type Limits struct {
	TimeoutSec int   `json:"timeoutSec"`
	MaxTokens  int64 `json:"maxTokens"`
}

// Config is the configuration of a service agent (tech §4).
type Config struct {
	Name           string        `json:"name"`
	Description    string        `json:"description"`
	Harness        string        `json:"harness"`
	Model          models.Choice `json:"model"`
	Instructions   string        `json:"instructions"`
	Skills         []string      `json:"skills"`
	MCP            []string      `json:"mcp"`
	AcceptCallerMC bool          `json:"acceptCallerMcp"`
	Workspace      string        `json:"workspace"`
	Limits         Limits        `json:"limits"`
	Clients        []string      `json:"clients"`
}

// Agent is a stored service agent with run statistics.
type Agent struct {
	Config
	Enabled     bool      `json:"enabled"`
	UpdatedAt   time.Time `json:"updatedAt"`
	RunsWeek    int       `json:"runsWeek"`
	FailedWeek  int       `json:"failedWeek"`
	ModelName   string    `json:"modelName,omitempty"`
	ClientNames []string  `json:"-"`
}

// Run is a run of a service agent.
type Run struct {
	ID             uuid.UUID       `json:"id"`
	Agent          string          `json:"agent"`
	ClientID       uuid.UUID       `json:"clientId"`
	ClientName     string          `json:"clientName"`
	InitiatorEmail *string         `json:"initiator"`
	Status         string          `json:"status"`
	Summary        *string         `json:"summary"`
	ErrorClass     *string         `json:"errorClass"`
	ErrorText      *string         `json:"errorText"`
	Usage          json.RawMessage `json:"usage"`
	StartedAt      time.Time       `json:"startedAt"`
	FinishedAt     *time.Time      `json:"finishedAt"`
}

// CallerMCP is an MCP server the caller passes for one run (R15, SVC-08).
type CallerMCP struct {
	Name    string            `json:"name"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
}

// RunInput is the body of POST /client/v1/agents/{name}/runs (tech §5).
type RunInput struct {
	Input          string          `json:"input"`
	Context        json.RawMessage `json:"context"`
	CallerMCP      []CallerMCP     `json:"callerMcp"`
	Initiator      string          `json:"initiator"`
	IdempotencyKey string          `json:"idempotencyKey"`
}

// Service manages service agents and runs.
type Service struct {
	Pool   *pgxpool.Pool
	Models *models.Service
	Signer *jwt.Signer
	Bus    kafka.Publisher
	Events events.Publisher
	// RelayPublicURL is where external workspaces connect.
	RelayPublicURL string
	PublicAPIURL   string
	// Harnesses are the installed harnesses (R20: pi).
	Harnesses []Harness
	// WorkspaceTTL is the lifetime of a workspace token.
	WorkspaceTTL time.Duration
}

// Harness is an installed harness (tech §4: GET /harnesses).
type Harness struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	Status  string `json:"status"`
}

var nameRe = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

func (s *Service) validate(ctx context.Context, c *Config) error {
	c.Name = strings.TrimSpace(c.Name)
	if !nameRe.MatchString(c.Name) || len(c.Name) > 64 {
		return apperr.Unprocessable("invalid_name", "the name is lower-case words with dashes").With("field", "name")
	}
	if c.Harness == "" {
		c.Harness = "pi"
	}
	known := false
	for _, h := range s.Harnesses {
		known = known || h.Name == c.Harness
	}
	if !known {
		return apperr.Unprocessable("invalid_harness", "unknown harness").With("field", "harness")
	}
	if _, _, err := s.Models.Resolve(ctx, c.Model); err != nil {
		return apperr.Unprocessable("invalid_model", "the model is not available: "+err.Error()).With("field", "model")
	}
	switch c.Workspace {
	case "":
		c.Workspace = WorkspaceNone
	case WorkspaceNone, WorkspaceNabu, WorkspaceExternal:
	default:
		return apperr.Unprocessable("invalid_workspace", "workspace is none, nabu or external").With("field", "workspace")
	}
	if c.Limits.TimeoutSec <= 0 {
		c.Limits.TimeoutSec = 1800
	}
	if c.Limits.TimeoutSec > 24*3600 {
		return apperr.Unprocessable("invalid_limits", "the timeout is at most 24 hours").With("field", "limits")
	}
	for _, list := range []*[]string{&c.Skills, &c.MCP, &c.Clients} {
		if *list == nil {
			*list = []string{}
		}
	}
	return nil
}

// Save creates or replaces a service agent and the clients' rights to it.
func (s *Service) Save(ctx context.Context, actor *uuid.UUID, name string, c Config, create bool) (*Agent, error) {
	if !create {
		c.Name = name
	}
	if err := s.validate(ctx, &c); err != nil {
		return nil, err
	}
	b, _ := json.Marshal(c)
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		var err error
		if create {
			_, err = tx.Exec(ctx, `INSERT INTO service_agents (name, config, updated_by) VALUES ($1,$2,$3)`, c.Name, b, actor)
			if postgres.IsUniqueViolation(err) {
				return apperr.Conflict("name_taken", "a service agent with this name exists").With("field", "name")
			}
		} else {
			var tag interface{ RowsAffected() int64 }
			tag, err = tx.Exec(ctx, `UPDATE service_agents SET config = $2, updated_by = $3, updated_at = now() WHERE name = $1`, c.Name, b, actor)
			if err == nil && tag.RowsAffected() == 0 {
				return apperr.NotFound("not_found", "service agent not found")
			}
		}
		if err != nil {
			return err
		}
		// The clients of the configuration are the clients allowed to run it (R17).
		if _, err := tx.Exec(ctx, `UPDATE service_clients SET agents = array_remove(agents, $1) WHERE NOT (name = ANY($2))`, c.Name, c.Clients); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE service_clients SET agents = array_append(agents, $1) WHERE name = ANY($2) AND NOT ($1 = ANY(agents))`, c.Name, c.Clients)
		return err
	})
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, c.Name)
}

const agentSelect = `SELECT a.name, a.config, a.enabled, a.updated_at,
	(SELECT count(*) FROM runs r WHERE r.agent = a.name AND r.started_at > now() - interval '7 days'),
	(SELECT count(*) FROM runs r WHERE r.agent = a.name AND r.started_at > now() - interval '7 days' AND r.status = 'failed'),
	COALESCE((SELECT array_agg(c.name ORDER BY c.name) FROM service_clients c WHERE a.name = ANY(c.agents)), '{}')
	FROM service_agents a`

func scanAgent(row pgx.Row) (*Agent, error) {
	var a Agent
	var cfg []byte
	if err := row.Scan(&a.Name, &cfg, &a.Enabled, &a.UpdatedAt, &a.RunsWeek, &a.FailedWeek, &a.ClientNames); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(cfg, &a.Config); err != nil {
		return nil, err
	}
	a.Clients = a.ClientNames
	if a.Clients == nil {
		a.Clients = []string{}
	}
	return &a, nil
}

// List lists the service agents.
func (s *Service) List(ctx context.Context) ([]Agent, error) {
	rows, err := s.Pool.Query(ctx, agentSelect+` ORDER BY a.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Agent{}
	for rows.Next() {
		a, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *a)
	}
	return out, rows.Err()
}

// Get loads a service agent.
func (s *Service) Get(ctx context.Context, name string) (*Agent, error) {
	a, err := scanAgent(s.Pool.QueryRow(ctx, agentSelect+` WHERE a.name = $1`, name))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "service agent not found")
	}
	return a, err
}

// Delete removes a service agent without runs; with runs it is disabled.
func (s *Service) Delete(ctx context.Context, name string) error {
	var n int
	_ = s.Pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE agent = $1`, name).Scan(&n)
	var err error
	if n > 0 {
		_, err = s.Pool.Exec(ctx, `UPDATE service_agents SET enabled = false WHERE name = $1`, name)
	} else {
		_, err = s.Pool.Exec(ctx, `DELETE FROM service_agents WHERE name = $1`, name)
	}
	if err == nil {
		_, err = s.Pool.Exec(ctx, `UPDATE service_clients SET agents = array_remove(agents, $1)`, name)
	}
	return err
}

// ─── runs ───────────────────────────────────────────────────────────

const runSelect = `SELECT r.id, r.agent, r.client_id, c.name, r.initiator_email, r.status, r.summary, r.error_class, r.error_text,
	r.usage, r.started_at, r.finished_at FROM runs r JOIN service_clients c ON c.id = r.client_id`

func scanRun(row pgx.Row) (*Run, error) {
	var r Run
	err := row.Scan(&r.ID, &r.Agent, &r.ClientID, &r.ClientName, &r.InitiatorEmail, &r.Status, &r.Summary, &r.ErrorClass, &r.ErrorText,
		&r.Usage, &r.StartedAt, &r.FinishedAt)
	return &r, err
}

// GetRun loads a run.
func (s *Service) GetRun(ctx context.Context, id uuid.UUID) (*Run, error) {
	r, err := scanRun(s.Pool.QueryRow(ctx, runSelect+` WHERE r.id = $1`, id))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "run not found")
	}
	return r, err
}

// Runs lists the runs of an agent (the run log of the administration).
func (s *Service) Runs(ctx context.Context, name string, page httpx.Page) (httpx.List[Run], error) {
	args := []any{name, page.Limit + 1}
	cond := ""
	if page.Cursor != nil {
		cond = ` AND (r.started_at, r.id) < ($3, $4)`
		args = append(args, page.Cursor.T, page.Cursor.ID)
	}
	rows, err := s.Pool.Query(ctx, runSelect+` WHERE r.agent = $1`+cond+` ORDER BY r.started_at DESC, r.id DESC LIMIT $2`, args...)
	if err != nil {
		return httpx.List[Run]{}, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return httpx.List[Run]{}, err
		}
		out = append(out, *r)
	}
	return httpx.NewList(out, page.Limit, func(r Run) (time.Time, string) { return r.StartedAt, r.ID.String() }), rows.Err()
}

// Started is the answer of POST /client/v1/agents/{name}/runs.
type Started struct {
	RunID          uuid.UUID `json:"runId"`
	WorkspaceToken string    `json:"workspaceToken,omitempty"`
	WorkspaceID    string    `json:"workspaceId,omitempty"`
	RelayURL       string    `json:"relayUrl,omitempty"`
	EventsURL      string    `json:"eventsUrl"`
	EventsToken    string    `json:"eventsToken"`
	Existing       bool      `json:"-"`
}

// RunMessage is the payload of nabu.runs.
type RunMessage struct {
	RunID uuid.UUID `json:"runId"`
}

// WorkspaceID is the relay id of a run's external workspace.
func WorkspaceID(run uuid.UUID) string { return "run-" + run.String() }

// Start creates a run for a client (SVC-02…04): the agent must be allowed to
// the client; a repeated idempotency key returns the same run.
func (s *Service) Start(ctx context.Context, cl *httpx.Client, name string, in RunInput) (*Started, error) {
	allowed := false
	for _, a := range cl.Agents {
		allowed = allowed || a == name
	}
	if !allowed {
		return nil, apperr.Forbidden("agent_not_allowed", "the service agent is not allowed to this client")
	}
	ag, err := s.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if !ag.Enabled {
		return nil, apperr.Forbidden("agent_not_allowed", "the service agent is disabled")
	}
	if strings.TrimSpace(in.Input) == "" {
		return nil, apperr.Unprocessable("input_required", "input is required").With("field", "input")
	}
	if len(in.CallerMCP) > 0 && !ag.AcceptCallerMC {
		return nil, apperr.Unprocessable("caller_mcp_not_accepted", "the agent does not accept MCP servers of the caller")
	}
	for _, m := range in.CallerMCP {
		if !nameRe.MatchString(m.Name) || !strings.HasPrefix(m.URL, "http") {
			return nil, apperr.Unprocessable("invalid_caller_mcp", "callerMcp needs a name and an http(s) url")
		}
	}
	body, _ := json.Marshal(in)
	var key *string
	if in.IdempotencyKey != "" {
		key = &in.IdempotencyKey
	}
	var initiator *string
	if in.Initiator != "" {
		e := strings.ToLower(strings.TrimSpace(in.Initiator))
		initiator = &e
	}
	var id uuid.UUID
	existing := false
	err = s.Pool.QueryRow(ctx, `INSERT INTO runs (agent, client_id, initiator_email, idempotency_key, status, input)
		VALUES ($1,$2,$3,$4,'queued',$5) ON CONFLICT (client_id, idempotency_key) DO NOTHING RETURNING id`,
		name, cl.ID, initiator, key, body).Scan(&id)
	if postgres.IsNoRows(err) && key != nil {
		existing = true
		err = s.Pool.QueryRow(ctx, `SELECT id FROM runs WHERE client_id = $1 AND idempotency_key = $2`, cl.ID, *key).Scan(&id)
	}
	if err != nil {
		return nil, err
	}
	st := &Started{RunID: id, Existing: existing, EventsURL: s.PublicAPIURL + "/client/v1/runs/" + id.String() + "/events",
		EventsToken: s.Signer.Issue(jwt.Claims{Audience: jwt.AudRunEvents, Run: id.String()}, 24*time.Hour)}
	if ag.Workspace == WorkspaceExternal {
		st.WorkspaceID = WorkspaceID(id)
		st.RelayURL = s.RelayPublicURL
		st.WorkspaceToken = s.Signer.Issue(jwt.Claims{Audience: jwt.AudWorkspace, Workspace: st.WorkspaceID, Kind: "external", Run: id.String()}, s.WorkspaceTTL)
	}
	if !existing {
		b, _ := json.Marshal(RunMessage{RunID: id})
		if err := s.Bus.Publish(ctx, kafka.TopicRuns, id.String(), b); err != nil {
			_, _ = s.Pool.Exec(ctx, `UPDATE runs SET status = 'failed', error_class = 'unavailable', error_text = 'the run could not be queued', finished_at = now() WHERE id = $1`, id)
			return nil, apperr.Unavailable("agent_unavailable", "the run could not be queued")
		}
	}
	return st, nil
}

// Cancel cancels a run of the client.
func (s *Service) Cancel(ctx context.Context, clientID, id uuid.UUID) (*Run, error) {
	r, err := s.GetRun(ctx, id)
	if err != nil {
		return nil, err
	}
	if r.ClientID != clientID {
		return nil, apperr.NotFound("not_found", "run not found")
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE runs SET status = 'cancelled', finished_at = COALESCE(finished_at, now()) WHERE id = $1 AND status IN ('queued','running')`, id); err != nil {
		return nil, err
	}
	s.AppendEvent(ctx, id, "cancelled", map[string]any{})
	return s.GetRun(ctx, id)
}

// AppendEvent stores an event of a run and notifies its readers.
func (s *Service) AppendEvent(ctx context.Context, id uuid.UUID, typ string, data any) {
	b, _ := json.Marshal(data)
	var seq int
	err := s.Pool.QueryRow(context.WithoutCancel(ctx), `WITH r AS (UPDATE runs SET last_seq = last_seq + 1 WHERE id = $1 RETURNING last_seq)
		INSERT INTO run_events (run_id, seq, type, data) SELECT $1, last_seq, $2, $3 FROM r RETURNING seq`, id, typ, b).Scan(&seq)
	if err != nil {
		return
	}
	if s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: events.RunEvent, RunID: &id, Data: RunEventData{Seq: seq, Type: typ, Data: b}})
	}
}

// RunEventData is a stored run event.
type RunEventData struct {
	Seq  int             `json:"seq"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func (s *Service) eventsSince(ctx context.Context, id uuid.UUID, after int) ([]RunEventData, error) {
	rows, err := s.Pool.Query(ctx, `SELECT seq, type, data FROM run_events WHERE run_id = $1 AND seq > $2 ORDER BY seq`, id, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RunEventData
	for rows.Next() {
		var e RunEventData
		if err := rows.Scan(&e.Seq, &e.Type, &e.Data); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ─── HTTP ───────────────────────────────────────────────────────────

// AdminRoutes mounts /service-agents and /harnesses (tech §4).
func (s *Service) AdminRoutes(r chi.Router) {
	r.Get("/harnesses", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		httpx.JSON(w, 200, map[string]any{"items": s.Harnesses})
		return nil
	}))
	r.Get("/service-agents", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		l, err := s.List(r.Context())
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Post("/service-agents", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, _ := httpx.MustPrincipal(r)
		var c Config
		if err := httpx.Decode(r, &c); err != nil {
			return err
		}
		a, err := s.Save(r.Context(), &p.UserID, "", c, true)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, a)
		return nil
	}))
	r.Get("/service-agents/{name}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		a, err := s.Get(r.Context(), chi.URLParam(r, "name"))
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, a)
		return nil
	}))
	r.Put("/service-agents/{name}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, _ := httpx.MustPrincipal(r)
		var c Config
		if err := httpx.Decode(r, &c); err != nil {
			return err
		}
		a, err := s.Save(r.Context(), &p.UserID, chi.URLParam(r, "name"), c, false)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, a)
		return nil
	}))
	r.Delete("/service-agents/{name}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		if err := s.Delete(r.Context(), chi.URLParam(r, "name")); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Get("/service-agents/{name}/runs", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		page, err := httpx.ParsePage(r)
		if err != nil {
			return err
		}
		l, err := s.Runs(r.Context(), chi.URLParam(r, "name"), page)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, l)
		return nil
	}))
}

// ClientRoutes mounts the runs of the client API (tech §5).
func (s *Service) ClientRoutes(r chi.Router) {
	r.Get("/agents", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		cl := httpx.ClientFrom(r.Context())
		out := []map[string]any{}
		for _, n := range cl.Agents {
			a, err := s.Get(r.Context(), n)
			if err != nil || !a.Enabled {
				continue
			}
			out = append(out, map[string]any{"name": a.Name, "description": a.Description, "workspace": a.Workspace})
		}
		httpx.JSON(w, 200, map[string]any{"items": out})
		return nil
	}))
	r.Post("/agents/{name}/runs", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in RunInput
		if err := httpx.DecodeMax(r, &in, 4<<20); err != nil {
			return err
		}
		st, err := s.Start(r.Context(), httpx.ClientFrom(r.Context()), chi.URLParam(r, "name"), in)
		if err != nil {
			return err
		}
		code := http.StatusCreated
		if st.Existing {
			code = http.StatusOK
		}
		httpx.JSON(w, code, st)
		return nil
	}))
	r.Get("/runs/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		run, err := s.GetRun(r.Context(), id)
		if err != nil {
			return err
		}
		if run.ClientID != httpx.ClientFrom(r.Context()).ID {
			return apperr.NotFound("not_found", "run not found")
		}
		httpx.JSON(w, 200, run)
		return nil
	}))
	r.Post("/runs/{id}/cancel", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		run, err := s.Cancel(r.Context(), httpx.ClientFrom(r.Context()).ID, id)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, run)
		return nil
	}))
}

// EventsRoute mounts GET /client/v1/runs/{id}/events: SSE with the client
// token or the run's eventsToken; Last-Event-ID resumes (tech §5).
func (s *Service) EventsRoute(r chi.Router, hub *events.Hub, clientOf func(r *http.Request) (uuid.UUID, bool)) {
	r.Get("/client/v1/runs/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			httpx.Error(w, r, err)
			return
		}
		tok := httpx.Bearer(r)
		if tok == "" {
			tok = r.URL.Query().Get("token")
		}
		allowed := false
		if c, err := s.Signer.Verify(tok, jwt.AudRunEvents); err == nil && c.Run == id.String() {
			allowed = true
		} else if cid, ok := clientOf(r); ok {
			if run, err := s.GetRun(r.Context(), id); err == nil && run.ClientID == cid {
				allowed = true
			}
		}
		if !allowed {
			httpx.Error(w, r, apperr.Unauthorized("unauthenticated", "a client token or the events token of the run is required"))
			return
		}
		after, _ := strconv.Atoi(r.Header.Get("Last-Event-ID"))
		sub := hub.SubscribeRun(id)
		defer hub.Unsubscribe(sub)
		past, err := s.eventsSince(r.Context(), id, after)
		if err != nil {
			httpx.Error(w, r, err)
			return
		}
		fl, ok := w.(http.Flusher)
		if !ok {
			return
		}
		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)
		last := after
		write := func(e RunEventData) bool {
			if e.Seq <= last {
				return false
			}
			last = e.Seq
			fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.Seq, e.Type, e.Data)
			fl.Flush()
			return e.Type == "completed"
		}
		for _, e := range past {
			if write(e) {
				return
			}
		}
		fl.Flush()
		ping := time.NewTicker(20 * time.Second)
		defer ping.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ping.C:
				fmt.Fprint(w, ": ping\n\n")
				fl.Flush()
			case ev := <-sub.C:
				d, ok := ev.Data.(RunEventData)
				if !ok {
					b, _ := json.Marshal(ev.Data)
					_ = json.Unmarshal(b, &d)
				}
				if d.Seq > last+1 { // missed events (slow reader): read them from the table
					rest, _ := s.eventsSince(r.Context(), id, last)
					for _, e := range rest {
						if write(e) {
							return
						}
					}
					continue
				}
				if write(d) {
					return
				}
			}
		}
	})
}
