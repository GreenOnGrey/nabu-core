// Package ledger records usage and the audit of agent actions
// (FTR.NAB.CMN-0001 R27–R28, arch §10, tech §10): every tool call of every
// agent — when, which agent, on whose behalf, channel, tool and server,
// result. The audit table is partitioned by month; the worker creates
// partitions ahead and drops those older than AUDIT_RETENTION.
package ledger

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

// ArgsMax bounds the stored arguments (tech §10).
const ArgsMax = 500

// Entry is one audit record.
type Entry struct {
	ID             int64      `json:"id"`
	At             time.Time  `json:"at"`
	AgentKind      string     `json:"agentKind"` // personal | service
	Agent          string     `json:"agent"`
	UserID         *uuid.UUID `json:"userId"`
	UserEmail      *string    `json:"userEmail"`
	ClientID       *uuid.UUID `json:"clientId"`
	ClientName     *string    `json:"clientName"`
	InitiatorEmail *string    `json:"initiatorEmail"`
	Channel        string     `json:"channel"`
	Server         *string    `json:"server"`
	Tool           string     `json:"tool"`
	ArgsSummary    *string    `json:"argsSummary"`
	Result         string     `json:"result"` // ok | error
	Error          *string    `json:"error"`
}

// Ledger writes and reads usage and audit.
type Ledger struct{ Pool *pgxpool.Pool }

// Mask replaces secret values in s and clips it (AUD-02).
func Mask(s string, secrets []string, max int) string {
	sorted := append([]string(nil), secrets...)
	sort.Slice(sorted, func(i, j int) bool { return len(sorted[i]) > len(sorted[j]) })
	for _, v := range sorted {
		if len(v) >= 6 {
			s = strings.ReplaceAll(s, v, "***")
		}
	}
	r := []rune(s)
	if len(r) > max {
		s = string(r[:max]) + "…"
	}
	return s
}

// SplitTool reads the server and tool of a Pi tool name: mcp__<server>__<tool>
// for MCP tools; Pi's own tools (read, bash, …) belong to the workspace.
func SplitTool(name string) (server, tool string) {
	if rest, ok := strings.CutPrefix(name, "mcp__"); ok {
		if s, t, ok := strings.Cut(rest, "__"); ok {
			return s, t
		}
	}
	switch name {
	case "read", "write", "edit", "bash", "ls", "find", "grep":
		return "workspace", name
	}
	return "", name
}

// Audit records a tool call.
func (l *Ledger) Audit(ctx context.Context, e Entry, args string, secrets []string) {
	var a *string
	if args != "" {
		m := Mask(args, secrets, ArgsMax)
		a = &m
	}
	if e.Error != nil {
		m := Mask(*e.Error, secrets, 1000)
		e.Error = &m
	}
	if _, err := l.Pool.Exec(context.WithoutCancel(ctx), `INSERT INTO audit (agent_kind, agent, user_id, client_id, initiator_email, channel, server, tool, args_summary, result, error)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, e.AgentKind, e.Agent, e.UserID, e.ClientID, e.InitiatorEmail, e.Channel, e.Server, e.Tool, a, e.Result, e.Error); err != nil {
		slog.ErrorContext(ctx, "audit write failed", "err", err, "tool", e.Tool)
	}
}

// UsageRow is one usage record.
type UsageRow struct {
	UserID, RunID, ClientID *uuid.UUID
	Agent, Model            string
	ConnectionID            *uuid.UUID
	agent.Usage
}

// Usage records token use and cost.
func (l *Ledger) Usage(ctx context.Context, u UsageRow) {
	if u.IsZero() {
		return
	}
	if _, err := l.Pool.Exec(context.WithoutCancel(ctx), `INSERT INTO usage (user_id, run_id, client_id, agent, connection_id, model, tokens_in, tokens_out, cache_read, cache_write, cost_usd)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, u.UserID, u.RunID, u.ClientID, nilIfEmpty(u.Agent), u.ConnectionID, nilIfEmpty(u.Model),
		u.TokensIn, u.TokensOut, u.CacheRead, u.CacheWrite, u.CostUSD); err != nil {
		slog.ErrorContext(ctx, "usage write failed", "err", err)
	}
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Filter of the audit (tech §4).
type Filter struct {
	From, To  time.Time
	AgentKind string
	Agent     string
	User      *uuid.UUID
	Client    *uuid.UUID
	Tool      string
	Result    string
}

// ParseFilter reads the query of /audit.
func ParseFilter(r *http.Request) (Filter, error) {
	q := r.URL.Query()
	f := Filter{To: time.Now().Add(time.Minute), From: time.Now().AddDate(0, 0, -30), AgentKind: q.Get("agentKind"),
		Agent: q.Get("agent"), Tool: q.Get("tool"), Result: q.Get("result")}
	for k, dst := range map[string]*time.Time{"from": &f.From, "to": &f.To} {
		if v := q.Get(k); v != "" {
			t, err := parseTime(v)
			if err != nil {
				return f, apperr.BadRequest("invalid_"+k, "use RFC 3339 or YYYY-MM-DD")
			}
			*dst = t
		}
	}
	for k, dst := range map[string]**uuid.UUID{"user": &f.User, "client": &f.Client} {
		if v := q.Get(k); v != "" {
			id, err := uuid.Parse(v)
			if err != nil {
				return f, apperr.BadRequest("invalid_"+k, "invalid id")
			}
			*dst = &id
		}
	}
	return f, nil
}

func parseTime(v string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, nil
	}
	return time.Parse("2006-01-02", v)
}

func (f Filter) where(args *[]any) string {
	add := func(v any) string { *args = append(*args, v); return "$" + strconv.Itoa(len(*args)) }
	conds := []string{"a.at >= " + add(f.From), "a.at < " + add(f.To)}
	if f.AgentKind != "" {
		conds = append(conds, "a.agent_kind = "+add(f.AgentKind))
	}
	if f.Agent != "" {
		conds = append(conds, "a.agent = "+add(f.Agent))
	}
	if f.User != nil {
		conds = append(conds, "a.user_id = "+add(*f.User))
	}
	if f.Client != nil {
		conds = append(conds, "a.client_id = "+add(*f.Client))
	}
	if f.Tool != "" {
		conds = append(conds, "a.tool ILIKE '%' || "+add(f.Tool)+" || '%'")
	}
	if f.Result != "" {
		conds = append(conds, "a.result = "+add(f.Result))
	}
	return strings.Join(conds, " AND ")
}

const auditSelect = `SELECT a.id, a.at, a.agent_kind, a.agent, a.user_id, u.email, a.client_id, c.name, a.initiator_email, a.channel, a.server, a.tool,
	a.args_summary, a.result, a.error FROM audit a LEFT JOIN users u ON u.id = a.user_id LEFT JOIN service_clients c ON c.id = a.client_id`

// List reads the audit, newest first.
func (l *Ledger) List(ctx context.Context, f Filter, page httpx.Page) (httpx.List[Entry], error) {
	args := []any{}
	where := f.where(&args)
	if page.Cursor != nil {
		args = append(args, page.Cursor.T, page.Cursor.ID)
		where += fmt.Sprintf(" AND (a.at, a.id) < ($%d, $%d::bigint)", len(args)-1, len(args))
	}
	args = append(args, page.Limit+1)
	rows, err := l.Pool.Query(ctx, auditSelect+" WHERE "+where+fmt.Sprintf(" ORDER BY a.at DESC, a.id DESC LIMIT $%d", len(args)), args...)
	if err != nil {
		return httpx.List[Entry]{}, err
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.At, &e.AgentKind, &e.Agent, &e.UserID, &e.UserEmail, &e.ClientID, &e.ClientName, &e.InitiatorEmail,
			&e.Channel, &e.Server, &e.Tool, &e.ArgsSummary, &e.Result, &e.Error); err != nil {
			return httpx.List[Entry]{}, err
		}
		out = append(out, e)
	}
	return httpx.NewList(out, page.Limit, func(e Entry) (time.Time, string) { return e.At, strconv.FormatInt(e.ID, 10) }), rows.Err()
}

// ExportCSV writes the audit of the filter as CSV (AUD-03).
func (l *Ledger) ExportCSV(ctx context.Context, w http.ResponseWriter, f Filter) error {
	args := []any{}
	rows, err := l.Pool.Query(ctx, auditSelect+" WHERE "+f.where(&args)+" ORDER BY a.at, a.id", args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="nabu-audit.csv"`)
	cw := csv.NewWriter(w)
	_ = cw.Write([]string{"time", "agent_kind", "agent", "user", "client", "initiator", "channel", "server", "tool", "args", "result", "error"})
	s := func(p *string) string {
		if p == nil {
			return ""
		}
		return *p
	}
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.At, &e.AgentKind, &e.Agent, &e.UserID, &e.UserEmail, &e.ClientID, &e.ClientName, &e.InitiatorEmail,
			&e.Channel, &e.Server, &e.Tool, &e.ArgsSummary, &e.Result, &e.Error); err != nil {
			return err
		}
		_ = cw.Write([]string{e.At.UTC().Format(time.RFC3339), e.AgentKind, e.Agent, s(e.UserEmail), s(e.ClientName), s(e.InitiatorEmail),
			e.Channel, s(e.Server), e.Tool, s(e.ArgsSummary), e.Result, s(e.Error)})
	}
	cw.Flush()
	return rows.Err()
}

// UsageGroup is one row of the usage report.
type UsageGroup struct {
	Key        string  `json:"key"`
	Label      string  `json:"label"`
	TokensIn   int64   `json:"tokensIn"`
	TokensOut  int64   `json:"tokensOut"`
	CacheRead  int64   `json:"cacheRead"`
	CacheWrite int64   `json:"cacheWrite"`
	CostUSD    float64 `json:"costUsd"`
	Requests   int64   `json:"requests"`
}

// UsageReport groups usage by user, agent, client, model, connection or day (USE-01).
func (l *Ledger) UsageReport(ctx context.Context, from, to time.Time, groupBy string) ([]UsageGroup, error) {
	var key, label, join string
	switch groupBy {
	case "user":
		key, label, join = "u.user_id::text", "COALESCE(x.email, 'service')", " LEFT JOIN users x ON x.id = u.user_id"
	case "agent":
		key, label = "COALESCE(u.agent, 'personal')", "COALESCE(u.agent, 'personal')"
	case "client":
		key, label, join = "u.client_id::text", "COALESCE(x.name, '—')", " LEFT JOIN service_clients x ON x.id = u.client_id"
	case "model":
		key, label = "COALESCE(u.model,'')", "COALESCE(u.model,'')"
	case "connection":
		key, label, join = "u.connection_id::text", "COALESCE(x.name, '—')", " LEFT JOIN model_connections x ON x.id = u.connection_id"
	case "day", "":
		key, label = "to_char(u.created_at, 'YYYY-MM-DD')", "to_char(u.created_at, 'YYYY-MM-DD')"
	default:
		return nil, apperr.BadRequest("invalid_group", "groupBy is user, agent, client, model, connection or day")
	}
	rows, err := l.Pool.Query(ctx, `SELECT COALESCE(`+key+`,''), `+label+`, sum(u.tokens_in), sum(u.tokens_out), sum(u.cache_read), sum(u.cache_write),
		sum(u.cost_usd)::float8, count(*) FROM usage u`+join+` WHERE u.created_at >= $1 AND u.created_at < $2 GROUP BY 1, 2 ORDER BY 7 DESC, 1`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UsageGroup{}
	for rows.Next() {
		var g UsageGroup
		if err := rows.Scan(&g.Key, &g.Label, &g.TokensIn, &g.TokensOut, &g.CacheRead, &g.CacheWrite, &g.CostUSD, &g.Requests); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// Partitions creates the partitions of the current and the next two months
// and drops those that end before the retention (AUD-04).
func (l *Ledger) Partitions(ctx context.Context, retention time.Duration) error {
	now := time.Now().UTC()
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		from := start.AddDate(0, i, 0)
		to := from.AddDate(0, 1, 0)
		name := "audit_" + from.Format("2006_01")
		var exists bool
		_ = l.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1)`, name).Scan(&exists)
		if exists {
			continue
		}
		// Rows of the range that went to the default partition move into the new one.
		tx, err := l.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		stmts := []string{
			fmt.Sprintf(`CREATE TEMP TABLE audit_move ON COMMIT DROP AS SELECT * FROM audit_default WHERE at >= '%s' AND at < '%s'`, from.Format(time.RFC3339), to.Format(time.RFC3339)),
			fmt.Sprintf(`DELETE FROM audit_default WHERE at >= '%s' AND at < '%s'`, from.Format(time.RFC3339), to.Format(time.RFC3339)),
			fmt.Sprintf(`CREATE TABLE %s PARTITION OF audit FOR VALUES FROM ('%s') TO ('%s')`, name, from.Format(time.RFC3339), to.Format(time.RFC3339)),
			`INSERT INTO audit SELECT * FROM audit_move`,
		}
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("partition %s: %w", name, err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		slog.Info("audit partition created", "partition", name)
	}
	rows, err := l.Pool.Query(ctx, `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = 'audit' AND c.relname ~ '^audit_[0-9]{4}_[0-9]{2}$'`)
	if err != nil {
		return err
	}
	var parts []string
	for rows.Next() {
		var n string
		if rows.Scan(&n) == nil {
			parts = append(parts, n)
		}
	}
	rows.Close()
	cutoff := now.Add(-retention)
	for _, n := range parts {
		t, err := time.Parse("audit_2006_01", n)
		if err != nil {
			continue
		}
		if t.AddDate(0, 1, 0).Before(cutoff) {
			if _, err := l.Pool.Exec(ctx, `DROP TABLE `+n); err != nil {
				return err
			}
			slog.Info("audit partition dropped", "partition", n)
		}
	}
	_, err = l.Pool.Exec(ctx, `DELETE FROM audit_default WHERE at < $1`, cutoff)
	return err
}

// ─── HTTP ───────────────────────────────────────────────────────────

// AdminRoutes mounts /usage, /audit and /audit/export.csv (tech §4).
func (l *Ledger) AdminRoutes(r chi.Router) {
	r.Get("/usage", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		q := r.URL.Query()
		from, to := time.Now().AddDate(0, 0, -30), time.Now().Add(time.Minute)
		if v := q.Get("from"); v != "" {
			t, err := parseTime(v)
			if err != nil {
				return apperr.BadRequest("invalid_from", "invalid from")
			}
			from = t
		}
		if v := q.Get("to"); v != "" {
			t, err := parseTime(v)
			if err != nil {
				return apperr.BadRequest("invalid_to", "invalid to")
			}
			to = t
		}
		g, err := l.UsageReport(r.Context(), from, to, q.Get("groupBy"))
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": g})
		return nil
	}))
	r.Get("/audit", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		f, err := ParseFilter(r)
		if err != nil {
			return err
		}
		page, err := httpx.ParsePage(r)
		if err != nil {
			return err
		}
		list, err := l.List(r.Context(), f, page)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, list)
		return nil
	}))
	r.Get("/audit/export.csv", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		f, err := ParseFilter(r)
		if err != nil {
			return err
		}
		return l.ExportCSV(r.Context(), w, f)
	}))
}

// UserRoutes mounts GET /audit for the user's own agent (R28, AUD-05).
func (l *Ledger) UserRoutes(r chi.Router) {
	r.Get("/audit", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		f, err := ParseFilter(r)
		if err != nil {
			return err
		}
		f.User, f.Client, f.AgentKind = &p.UserID, nil, "personal"
		page, err := httpx.ParsePage(r)
		if err != nil {
			return err
		}
		list, err := l.List(r.Context(), f, page)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, list)
		return nil
	}))
}
