package proxies

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// States of a proxy (R16).
const (
	StateChecking    = "checking"
	StateOK          = "ok"
	StateUnreachable = "unreachable"
)

// Proxy is a stored proxy. Secret values stay sealed in ValueEnc/TokenEnc;
// View strips them before anything leaves the api.
type Proxy struct {
	Name        string     `json:"name"`
	Kind        string     `json:"kind"`
	Title       string     `json:"title"`
	Description string     `json:"description"`
	Ecosystems  []string   `json:"ecosystems,omitempty"`
	Registries  []Registry `json:"registries,omitempty"`
	Allow       []Host     `json:"allow"`
	Env         []Env      `json:"env"`
	State       string     `json:"state"`
	CheckError  string     `json:"checkError,omitempty"`
	CheckedAt   *time.Time `json:"checkedAt,omitempty"`
	UpdatedAt   time.Time  `json:"updatedAt"`
	Assigned    *Usage     `json:"assigned,omitempty"`
}

// Usage is who a proxy is given to (R19).
type Usage struct {
	Personal bool     `json:"personal"`
	Group    bool     `json:"group"`
	Services []string `json:"services"`
}

// Any reports that the proxy is given to someone.
func (u Usage) Any() bool { return u.Personal || u.Group || len(u.Services) > 0 }

// Resolved is a proxy as a sandbox gets it: open secrets.
type Resolved struct {
	Name        string            `json:"name"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Allow       []Host            `json:"allow"`
	Env         map[string]string `json:"-"`
	// Secrets are the values of the secret variables: the events of a run mask them (FTR.HMR.DEV-0004 R10).
	Secrets   []string  `json:"-"`
	UpdatedAt time.Time `json:"-"`
}

// Hosts lists "host:port" for the description of a sandbox (R20).
func (r Resolved) Hosts() []string {
	out := make([]string, 0, len(r.Allow))
	for _, h := range r.Allow {
		out = append(out, h.String())
	}
	return out
}

// AuditFunc writes an action of an administrator to the audit (R21).
type AuditFunc func(ctx context.Context, actor, tool string, args any)

// Service stores proxies.
type Service struct {
	Pool  *pgxpool.Pool
	Box   *crypto.Box
	Audit AuditFunc
}

const cols = `name, kind, title, description, spec, allow, env, state, coalesce(check_error, ''), checked_at, updated_at`

func scan(row pgx.Row) (*Proxy, error) {
	var p Proxy
	var spec, allow, env []byte
	if err := row.Scan(&p.Name, &p.Kind, &p.Title, &p.Description, &spec, &allow, &env, &p.State, &p.CheckError, &p.CheckedAt, &p.UpdatedAt); err != nil {
		return nil, err
	}
	var sp Spec
	_ = json.Unmarshal(spec, &sp)
	p.Ecosystems, p.Registries = sp.Ecosystems, sp.Registries
	_ = json.Unmarshal(allow, &p.Allow)
	_ = json.Unmarshal(env, &p.Env)
	if p.Allow == nil {
		p.Allow = []Host{}
	}
	if p.Env == nil {
		p.Env = []Env{}
	}
	return &p, nil
}

// List returns all proxies with their usage.
func (s *Service) List(ctx context.Context) ([]Proxy, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+cols+` FROM proxies ORDER BY title, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Proxy
	for rows.Next() {
		p, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		u, err := s.usage(ctx, s.Pool, out[i].Name)
		if err != nil {
			return nil, err
		}
		out[i].Assigned = &u
	}
	return out, nil
}

// Get returns one proxy.
func (s *Service) Get(ctx context.Context, name string) (*Proxy, error) {
	p, err := scan(s.Pool.QueryRow(ctx, `SELECT `+cols+` FROM proxies WHERE name = $1`, name))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("proxy_not_found", "no such proxy")
	}
	return p, err
}

// usage finds who the proxy is given to.
func (s *Service) usage(ctx context.Context, q postgres.Querier, name string) (Usage, error) {
	u := Usage{Services: []string{}}
	rows, err := q.Query(ctx, `SELECT name FROM service_agents WHERE config->'proxies' ? $1 ORDER BY name`, name)
	if err != nil {
		return u, err
	}
	svc, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return u, err
	}
	u.Services = append(u.Services, svc...)
	rows, err = q.Query(ctx, `SELECT target FROM sandbox_assignments WHERE $1 = ANY(proxies)`, name)
	if err != nil {
		return u, err
	}
	targets, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return u, err
	}
	for _, t := range targets {
		u.Personal = u.Personal || t == "personal"
		u.Group = u.Group || t == "group"
	}
	return u, nil
}

// Exists checks that every name is a proxy (unknown_proxy).
func (s *Service) Exists(ctx context.Context, names []string, field string) error {
	if len(names) == 0 {
		return nil
	}
	var n int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM proxies WHERE name = ANY($1)`, names).Scan(&n); err != nil {
		return err
	}
	if n != len(uniq(names)) {
		return apperr.Unprocessable("unknown_proxy", "no such proxy").With("field", field)
	}
	return nil
}

func uniq(v []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range v {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// Resolve returns the proxies by name with open secrets for a sandbox;
// proxies that no longer exist are skipped.
func (s *Service) Resolve(ctx context.Context, names []string) ([]Resolved, error) {
	if len(names) == 0 {
		return nil, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+cols+` FROM proxies WHERE name = ANY($1) ORDER BY name`, names)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Resolved
	for rows.Next() {
		p, err := scan(rows)
		if err != nil {
			return nil, err
		}
		r := Resolved{Name: p.Name, Title: p.Title, Description: p.Description, Allow: p.Allow, Env: map[string]string{}, UpdatedAt: p.UpdatedAt}
		for _, e := range p.Env {
			v := e.Value
			if e.Secret {
				if len(e.ValueEnc) == 0 {
					continue
				}
				if v, err = s.Box.Open(e.ValueEnc); err != nil {
					return nil, fmt.Errorf("proxy %s: open %s: %w", p.Name, e.Name, err)
				}
				r.Secrets = append(r.Secrets, v)
			}
			r.Env[e.Name] = v
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Allow returns the addresses of the proxies by name, for the gateway.
func (s *Service) Allow(ctx context.Context) (map[string][]Host, error) {
	rows, err := s.Pool.Query(ctx, `SELECT name, allow FROM proxies`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]Host{}
	for rows.Next() {
		var name string
		var raw []byte
		if err := rows.Scan(&name, &raw); err != nil {
			return nil, err
		}
		var hs []Host
		_ = json.Unmarshal(raw, &hs)
		out[name] = hs
	}
	return out, rows.Err()
}

// Input is the body of POST and PUT …/proxies.
type Input struct {
	Name        string     `json:"name"`
	Title       string     `json:"title"`
	Description string     `json:"description"`
	Kind        string     `json:"kind"`
	Ecosystems  []string   `json:"ecosystems"`
	Registries  []Registry `json:"registries"`
	Allow       []Host     `json:"allow"`
	Env         []Env      `json:"env"`
}

// prepare validates the input and builds what is stored; prev keeps the
// secrets that were not sent again (R23).
func (s *Service) prepare(in Input, prev *Proxy) (*Proxy, error) {
	in.Title = strings.TrimSpace(in.Title)
	if n := len([]rune(in.Title)); n < 1 || n > 60 {
		return nil, apperr.Unprocessable("invalid_title", "the title is 1–60 characters").With("field", "title")
	}
	if len([]rune(in.Description)) > 500 {
		return nil, apperr.Unprocessable("invalid_description", "the description is at most 500 characters").With("field", "description")
	}
	p := &Proxy{Name: in.Name, Kind: in.Kind, Title: in.Title, Description: strings.TrimSpace(in.Description)}
	switch in.Kind {
	case KindPackages:
		regs := make([]Registry, len(in.Registries))
		for i, r := range in.Registries {
			r.TokenEnc, r.TokenSet = nil, false
			if r.Token == "" && prev != nil {
				for _, o := range prev.Registries {
					if o.Ecosystem == r.Ecosystem && o.URL == r.URL {
						r.TokenEnc = o.TokenEnc
					}
				}
			}
			regs[i] = r
		}
		allow, env, err := Build(Spec{Ecosystems: in.Ecosystems, Registries: regs})
		if err != nil {
			return nil, err
		}
		if err := s.seal(env); err != nil {
			return nil, err
		}
		for i, r := range regs {
			if r.Token != "" {
				if e, ok := findEnv(env, secretEnvName(r)); ok {
					regs[i].TokenEnc = e.ValueEnc
				}
			}
			regs[i].Token = ""
		}
		p.Ecosystems, p.Registries, p.Allow, p.Env = in.Ecosystems, regs, allow, env
	case KindHosts:
		allow := make([]Host, len(in.Allow))
		for i, h := range in.Allow {
			allow[i] = NormalizeHost(h)
			if err := ValidHost(allow[i], fmt.Sprintf("allow[%d]", i)); err != nil {
				return nil, err
			}
		}
		env := make([]Env, len(in.Env))
		for i, e := range in.Env {
			e.Name = strings.TrimSpace(e.Name)
			e.ValueEnc, e.Set = nil, false
			if e.Secret && e.Value == "" && prev != nil {
				if o, ok := findEnv(prev.Env, e.Name); ok && o.Secret {
					e.ValueEnc = o.ValueEnc
				}
			}
			env[i] = e
		}
		if err := s.seal(env); err != nil {
			return nil, err
		}
		p.Allow, p.Env = dedupHosts(allow), env
	default:
		return nil, apperr.Unprocessable("invalid_kind", "the kind is packages or hosts").With("field", "kind")
	}
	if len(p.Allow) > maxHosts || len(p.Env) > maxEnv {
		return nil, apperr.Unprocessable("too_many", fmt.Sprintf("at most %d addresses and %d variables", maxHosts, maxEnv))
	}
	if err := ValidEnv(p.Env); err != nil {
		return nil, err
	}
	return p, nil
}

// secretEnvName is the variable that carries the token of a registry.
func secretEnvName(r Registry) string {
	if r.Ecosystem == EcoPyPI {
		return "PIP_EXTRA_INDEX_URL"
	}
	return "NPM_TOKEN"
}

func findEnv(env []Env, name string) (Env, bool) {
	for _, e := range env {
		if e.Name == name {
			return e, true
		}
	}
	return Env{}, false
}

// seal encrypts secret values that came in open.
func (s *Service) seal(env []Env) error {
	for i := range env {
		if !env[i].Secret {
			env[i].ValueEnc = nil
			continue
		}
		if env[i].Value != "" {
			b, err := s.Box.Seal(env[i].Value)
			if err != nil {
				return err
			}
			env[i].ValueEnc = b
		}
		env[i].Value = ""
	}
	return nil
}

// View strips sealed values: secrets are reported as set or not (R23).
func View(p Proxy) Proxy {
	env := make([]Env, len(p.Env))
	for i, e := range p.Env {
		e.Set = e.Secret && len(e.ValueEnc) > 0
		e.ValueEnc = nil
		env[i] = e
	}
	p.Env = env
	regs := make([]Registry, len(p.Registries))
	for i, r := range p.Registries {
		r.TokenSet = len(r.TokenEnc) > 0
		r.TokenEnc, r.Token = nil, ""
		regs[i] = r
	}
	p.Registries = regs
	return p
}

func (s *Service) store(ctx context.Context, q postgres.Querier, p *Proxy, actor string, insert bool) error {
	spec, _ := json.Marshal(Spec{Ecosystems: p.Ecosystems, Registries: p.Registries})
	allow, _ := json.Marshal(p.Allow)
	env, _ := json.Marshal(p.Env)
	if insert {
		_, err := q.Exec(ctx, `INSERT INTO proxies (name, kind, title, description, spec, allow, env, updated_by)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, p.Name, p.Kind, p.Title, p.Description, spec, allow, env, actor)
		if postgres.IsUniqueViolation(err) {
			return apperr.Unprocessable("name_taken", "the name is taken").With("field", "name")
		}
		return err
	}
	_, err := q.Exec(ctx, `UPDATE proxies SET title = $2, description = $3, spec = $4, allow = $5, env = $6,
		state = 'checking', check_error = NULL, updated_by = $7, updated_at = now() WHERE name = $1`,
		p.Name, p.Title, p.Description, spec, allow, env, actor)
	return err
}

// Create creates a proxy (R21).
func (s *Service) Create(ctx context.Context, actor string, in Input) (*Proxy, error) {
	in.Name = strings.TrimSpace(in.Name)
	if err := ValidName(in.Name); err != nil {
		return nil, err
	}
	p, err := s.prepare(in, nil)
	if err != nil {
		return nil, err
	}
	if err := s.store(ctx, s.Pool, p, actor, true); err != nil {
		return nil, err
	}
	s.audit(ctx, actor, "proxy.create", map[string]any{"after": View(*p)})
	return s.Get(ctx, p.Name)
}

// Update changes a proxy; its name and kind stay (R21).
func (s *Service) Update(ctx context.Context, actor, name string, in Input) (*Proxy, error) {
	prev, err := s.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	in.Name, in.Kind = prev.Name, prev.Kind
	p, err := s.prepare(in, prev)
	if err != nil {
		return nil, err
	}
	if err := s.store(ctx, s.Pool, p, actor, false); err != nil {
		return nil, err
	}
	s.audit(ctx, actor, "proxy.update", map[string]any{"before": View(*prev), "after": View(*p)})
	return s.Get(ctx, name)
}

// Delete removes a proxy given to nobody (R19).
func (s *Service) Delete(ctx context.Context, actor, name string) error {
	var prev *Proxy
	err := postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		p, err := scan(tx.QueryRow(ctx, `SELECT `+cols+` FROM proxies WHERE name = $1 FOR UPDATE`, name))
		if postgres.IsNoRows(err) {
			return apperr.NotFound("proxy_not_found", "no such proxy")
		}
		if err != nil {
			return err
		}
		u, err := s.usage(ctx, tx, name)
		if err != nil {
			return err
		}
		if u.Any() {
			return apperr.Conflict("proxy_assigned", "the proxy is given to agents").With("assigned", u)
		}
		prev = p
		_, err = tx.Exec(ctx, `DELETE FROM proxies WHERE name = $1`, name)
		return err
	})
	if err == nil {
		s.audit(ctx, actor, "proxy.delete", map[string]any{"before": View(*prev)})
	}
	return err
}

// SetState stores the result of a check (R16).
func (s *Service) SetState(ctx context.Context, name, state, checkErr string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE proxies SET state = $2, check_error = NULLIF($3, ''), checked_at = now() WHERE name = $1`, name, state, checkErr)
	return err
}

// EnsureBuiltin fills the addresses and variables of proxies of the
// "packages" kind created by migrations with an empty allow (tech §9.2).
func (s *Service) EnsureBuiltin(ctx context.Context) error {
	rows, err := s.Pool.Query(ctx, `SELECT `+cols+` FROM proxies WHERE kind = 'packages' AND allow = '[]'::jsonb`)
	if err != nil {
		return err
	}
	ps, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (*Proxy, error) { return scan(r) })
	if err != nil {
		return err
	}
	for _, p := range ps {
		allow, env, err := Build(Spec{Ecosystems: p.Ecosystems, Registries: p.Registries})
		if err != nil {
			slog.WarnContext(ctx, "proxy template", "proxy", p.Name, "err", err)
			continue
		}
		p.Allow, p.Env = allow, env
		if err := s.store(ctx, s.Pool, p, "system", false); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) audit(ctx context.Context, actor, tool string, args any) {
	if s.Audit != nil {
		s.Audit(ctx, actor, tool, args)
	}
}

// Checker checks a proxy through the gateway; nil — the gateway is off.
type Checker func(ctx context.Context, proxy string, h Host) error

// Check checks one proxy now and stores the state (R16).
func (s *Service) Check(ctx context.Context, name string, check Checker) (*Proxy, error) {
	p, err := s.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	state, msg := StateOK, ""
	var target *Host
	for _, h := range p.Allow {
		if !strings.HasPrefix(h.Host, "*.") {
			target = &h
			break
		}
	}
	switch {
	case check == nil:
		state, msg = StateUnreachable, "the egress gateway is off"
	case target != nil:
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := check(cctx, name, *target)
		cancel()
		if err != nil {
			state, msg = StateUnreachable, fmt.Sprintf("%s: %v", target.Host, err)
		}
	}
	if err := s.SetState(ctx, name, state, msg); err != nil {
		return nil, err
	}
	up := 0.0
	if state == StateOK {
		up = 1
	}
	metrics.ProxyUp.WithLabelValues(name).Set(up)
	return s.Get(ctx, name)
}

// CheckAll checks every proxy; the api runs it every minute.
func (s *Service) CheckAll(ctx context.Context, check Checker) {
	rows, err := s.Pool.Query(ctx, `SELECT name FROM proxies`)
	if err != nil {
		slog.WarnContext(ctx, "proxy check", "err", err)
		return
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return
	}
	for _, n := range names {
		if _, err := s.Check(ctx, n, check); err != nil {
			slog.WarnContext(ctx, "proxy check", "proxy", n, "err", err)
		}
	}
}

// AdminRoutes mounts /proxies (tech §3.1).
func (s *Service) AdminRoutes(r chi.Router, check func() Checker) {
	actor := func(r *http.Request) string {
		p, _ := httpx.MustPrincipal(r)
		if p == nil {
			return ""
		}
		return p.UserID.String()
	}
	r.Get("/proxies", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		l, err := s.List(r.Context())
		if err != nil {
			return err
		}
		out := make([]Proxy, 0, len(l))
		for _, p := range l {
			out = append(out, View(p))
		}
		httpx.JSON(w, http.StatusOK, map[string]any{"items": out})
		return nil
	}))
	r.Get("/proxies/templates", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		httpx.JSON(w, http.StatusOK, TemplateView())
		return nil
	}))
	r.Post("/proxies", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in Input
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		p, err := s.Create(r.Context(), actor(r), in)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, View(*p))
		return nil
	}))
	r.Put("/proxies/{name}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in Input
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		p, err := s.Update(r.Context(), actor(r), chi.URLParam(r, "name"), in)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusOK, View(*p))
		return nil
	}))
	r.Delete("/proxies/{name}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		if err := s.Delete(r.Context(), actor(r), chi.URLParam(r, "name")); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Post("/proxies/{name}/check", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := s.Check(r.Context(), chi.URLParam(r, "name"), check())
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusOK, View(*p))
		return nil
	}))
}

// ActorUUID parses the actor of the audit; "" or "system" — none.
func ActorUUID(actor string) *uuid.UUID {
	id, err := uuid.Parse(actor)
	if err != nil {
		return nil
	}
	return &id
}
