// Package pkgsignals is the requests of libraries (FTR.NAB.CMN-0008 R10–R18):
// signals of missing packages sent by agents (sandbox_request_package) and
// noticed by sandboxes in the output of commands, mappings of modules and
// commands to packages, the status of a package for the administration and
// for agents.
package pkgsignals

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/sandboxprofile"
)

// Ecosystems of signals: packages (pypi, apt, binary) and what a sandbox
// notices before a mapping (python-module, command).
const (
	PyPI         = "pypi"
	Apt          = "apt"
	Binary       = "binary"
	PythonModule = "python-module"
	Command      = "command"

	SourceAgent = "agent"
	SourceAuto  = "auto"

	// Statuses of a package (R16).
	StatusNew      = "new"
	StatusPlanned  = "planned"
	StatusRejected = "rejected"
	StatusAdded    = "added"

	// AgentDailyLimit is how many requests an agent makes per user and day (R12).
	AgentDailyLimit = 20
	// MaxReason is the length of a reason (R10).
	MaxReason = 300
)

// Results of recording a signal (the result label of the metric).
const (
	ResultOK          = "ok"
	ResultDuplicate   = "duplicate"
	ResultRateLimited = "rate_limited"
	ResultInvalid     = "invalid"
)

var (
	rePyPI    = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	reApt     = regexp.MustCompile(`^[a-z0-9][a-z0-9+.-]{1,62}$`)
	reCommand = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.+-]{0,63}$`)
	reModule  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
)

// Normalize is the name of a package or a module in its ecosystem, or false
// for a bad name (tech §7.2).
func Normalize(ecosystem, name string) (string, bool) {
	name = strings.TrimSpace(name)
	switch ecosystem {
	case PyPI:
		name = sandboxprofile.NormalizePyPI(name)
		return name, rePyPI.MatchString(name) && len(name) <= 100
	case Apt:
		name = strings.ToLower(name)
		return name, reApt.MatchString(name)
	case Binary, Command:
		return name, reCommand.MatchString(name)
	case PythonModule:
		return name, reModule.MatchString(name)
	}
	return name, false
}

// Signal is one signal of a missing package.
type Signal struct {
	Ecosystem, Name, Source, Reason string
	Profile, AgentKind, Subject     string
}

// Service records and reports signals.
type Service struct {
	Pool     *pgxpool.Pool
	Profiles *sandboxprofile.Service
	Signer   *jwt.Signer
}

// Record stores a signal once per subject, package, source and day; an agent
// makes at most AgentDailyLimit requests a day (R12).
func (s *Service) Record(ctx context.Context, sg Signal) (string, error) {
	name, ok := Normalize(sg.Ecosystem, sg.Name)
	result := ResultInvalid
	defer func() {
		metrics.SandboxPackageSignals.WithLabelValues(sg.Ecosystem, sg.Source, sg.Profile, result).Inc()
	}()
	if !ok {
		return result, nil
	}
	if r := []rune(sg.Reason); len(r) > MaxReason {
		sg.Reason = string(r[:MaxReason])
	}
	if sg.Source == SourceAgent {
		var n int
		if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM package_signals WHERE subject = $1 AND day = current_date AND source = 'agent'`,
			sg.Subject).Scan(&n); err != nil {
			return "", err
		}
		if n >= AgentDailyLimit {
			result = ResultRateLimited
			return result, nil
		}
	}
	tag, err := s.Pool.Exec(ctx, `INSERT INTO package_signals (ecosystem, name, source, reason, profile, agent_kind, subject)
		VALUES ($1, $2, $3, nullif($4, ''), $5, $6, $7) ON CONFLICT (subject, ecosystem, name, source, day) DO NOTHING`,
		sg.Ecosystem, name, sg.Source, sg.Reason, sg.Profile, sg.AgentKind, sg.Subject)
	if err != nil {
		return "", err
	}
	if tag.RowsAffected() == 0 {
		result = ResultDuplicate
		return result, nil
	}
	eco, pkg, err := s.packageOf(ctx, sg.Ecosystem, name)
	if err != nil {
		return "", err
	}
	if _, err := s.Pool.Exec(ctx, `INSERT INTO package_status (package_ecosystem, package_name, status) VALUES ($1, $2, 'new')
		ON CONFLICT DO NOTHING`, eco, pkg); err != nil {
		return "", err
	}
	result = ResultOK
	return result, nil
}

// packageOf maps a module or a command to its package (R13).
func (s *Service) packageOf(ctx context.Context, ecosystem, name string) (string, string, error) {
	if ecosystem != PythonModule && ecosystem != Command {
		return ecosystem, name, nil
	}
	var eco, pkg string
	err := s.Pool.QueryRow(ctx, `SELECT package_ecosystem, package_name FROM package_aliases WHERE ecosystem = $1 AND name = $2`,
		ecosystem, name).Scan(&eco, &pkg)
	if postgres.IsNoRows(err) {
		return ecosystem, name, nil
	}
	return eco, pkg, err
}

// Status is what an agent learns about a package (R10).
type Status struct {
	Status  string `json:"status"` // available | planned | rejected | added | registered | rate_limited
	Profile string `json:"profile,omitempty"`
	Note    string `json:"note,omitempty"`
}

// StatusOf is the status of a package for an agent working in a profile.
func (s *Service) StatusOf(ctx context.Context, ecosystem, name, current string) (Status, error) {
	ms, err := s.Profiles.Manifests(ctx)
	if err != nil {
		return Status{}, err
	}
	// The profile of the agent first: the package may be there already.
	other := ""
	for _, m := range ms {
		if !m.Has(ecosystem, name) {
			continue
		}
		if m.Name == current {
			return Status{Status: "available", Profile: current, Note: "already in your sandbox profile"}, nil
		}
		if other == "" {
			other = m.Name
		}
	}
	if other != "" {
		return Status{Status: "available", Profile: other}, nil
	}
	var st Status
	var profile, note *string
	err = s.Pool.QueryRow(ctx, `SELECT status, profile, note FROM package_status WHERE package_ecosystem = $1 AND package_name = $2`,
		ecosystem, name).Scan(&st.Status, &profile, &note)
	if postgres.IsNoRows(err) || st.Status == StatusNew {
		return Status{Status: "registered"}, nil
	}
	if err != nil {
		return Status{}, err
	}
	if profile != nil {
		st.Profile = *profile
	}
	if note != nil && st.Status == StatusRejected {
		st.Note = *note
	}
	return st, nil
}

// MarkAdded sets «added» for the packages a profile now contains (R16).
func (s *Service) MarkAdded(ctx context.Context, m sandboxprofile.Manifest) error {
	version := m.Version
	if m.Kind == sandboxprofile.KindCustom && m.Digest != "" {
		version = strings.TrimPrefix(m.Digest, "sha256:")
		if len(version) > 12 {
			version = version[:12]
		}
	}
	rows, err := s.Pool.Query(ctx, `SELECT package_ecosystem, package_name FROM package_status WHERE status <> 'added'
		AND package_ecosystem IN ('pypi', 'apt', 'binary', 'command')`)
	if err != nil {
		return err
	}
	type pkg struct{ eco, name string }
	var hit []pkg
	for rows.Next() {
		var p pkg
		if rows.Scan(&p.eco, &p.name) == nil && m.Has(p.eco, p.name) {
			hit = append(hit, p)
		}
	}
	rows.Close()
	for _, p := range hit {
		if err := s.setStatus(ctx, p.eco, p.name, Change{Status: StatusAdded, Profile: m.Name, Version: version, By: "system"}); err != nil {
			return err
		}
	}
	return nil
}

// setStatus records a decision on a package and its history (R16).
func (s *Service) setStatus(ctx context.Context, eco, name string, c Change) error {
	_, err := s.Pool.Exec(ctx, `WITH up AS (
		INSERT INTO package_status (package_ecosystem, package_name, status, profile, version, note, updated_by)
		VALUES ($1, $2, $3, nullif($4, ''), nullif($5, ''), nullif($6, ''), $7)
		ON CONFLICT (package_ecosystem, package_name) DO UPDATE SET status = EXCLUDED.status, profile = EXCLUDED.profile,
			version = EXCLUDED.version, note = EXCLUDED.note, updated_by = EXCLUDED.updated_by, updated_at = now() RETURNING 1)
		INSERT INTO package_status_log (package_ecosystem, package_name, status, profile, version, note, updated_by)
		SELECT $1, $2, $3, nullif($4, ''), nullif($5, ''), nullif($6, ''), $7 FROM up`, eco, name, c.Status, c.Profile, c.Version, c.Note, c.By)
	return err
}

// ─── The tool of the built-in MCP ───────────────────────────────────

// Tool is sandbox_request_package (R10). It only records a signal, so it is
// read-only for the confirmation of mail topics (tech §7.3).
func (s *Service) Tool() mcp.Tool {
	return mcp.Tool{Name: "sandbox_request_package", ReadOnly: true,
		Description: "Report that a task needs a package missing from your sandbox profile. Do not try to install it: " +
			"the sandbox has no internet and no package managers. Returns the status of the package: available (in another " +
			"profile), planned, rejected (with the reason of the administrator), added or registered.",
		InputSchema: mcp.Schema(map[string]any{
			"ecosystem": map[string]any{"type": "string", "enum": []string{PyPI, Apt, Binary}},
			"name":      map[string]any{"type": "string", "description": "The package name, e.g. opencv-python, ffmpeg."},
			"reason":    map[string]any{"type": "string", "description": "Why the task needs it, up to 300 characters."},
		}, "ecosystem", "name", "reason"),
		Available: func(ctx context.Context, g mcp.Grant) bool { return s.Profiles != nil && g.UserID != uuid.Nil },
		Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
			var in struct{ Ecosystem, Name, Reason string }
			if err := json.Unmarshal(args, &in); err != nil {
				return "", &mcp.ToolError{Msg: "bad arguments"}
			}
			switch in.Ecosystem {
			case PyPI, Apt, Binary:
			default:
				return "", &mcp.ToolError{Msg: "ecosystem is pypi, apt or binary"}
			}
			name, ok := Normalize(in.Ecosystem, in.Name)
			if !ok {
				return "", &mcp.ToolError{Msg: "not a valid package name for " + in.Ecosystem}
			}
			kind, err := s.Profiles.KindOf(ctx, g.UserID)
			if err != nil {
				return "", err
			}
			current := s.currentProfile(ctx, g.UserID, kind)
			res, err := s.Record(ctx, Signal{Ecosystem: in.Ecosystem, Name: name, Source: SourceAgent, Reason: strings.TrimSpace(in.Reason),
				Profile: current, AgentKind: kind, Subject: g.UserID.String()})
			if err != nil {
				return "", err
			}
			if res == ResultRateLimited {
				b, _ := json.Marshal(Status{Status: "rate_limited", Note: "the daily limit of requests is reached; tell the user"})
				return string(b), nil
			}
			st, err := s.StatusOf(ctx, in.Ecosystem, name, current)
			if err != nil {
				return "", err
			}
			b, _ := json.Marshal(st)
			return string(b), nil
		}}
}

// currentProfile is the profile of the user's sandbox: the one it started
// with, else the assigned one.
func (s *Service) currentProfile(ctx context.Context, uid uuid.UUID, kind string) string {
	var p *string
	_ = s.Pool.QueryRow(ctx, `SELECT profile FROM spaces WHERE user_id = $1`, uid).Scan(&p)
	if p != nil && *p != "" {
		return *p
	}
	if n, err := s.Profiles.Assigned(ctx, sandboxprofile.Target{Kind: kind, UserID: uid}); err == nil {
		return n
	}
	return "base"
}

// ─── Signals of sandboxes ───────────────────────────────────────────

// InternalRoutes mounts POST /internal/v1/sandbox/signals (tech §7.1).
func (s *Service) InternalRoutes(r chi.Router) {
	r.Post("/internal/v1/sandbox/signals", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		c, err := s.Profiles.Authenticate(r.Context(), s.Signer, r)
		if err != nil {
			return err
		}
		if c.Probe != "" {
			return apperr.Forbidden("forbidden", "a probe sends no signals")
		}
		var in struct {
			Profile string `json:"profile"`
			Items   []struct {
				Ecosystem string `json:"ecosystem"`
				Name      string `json:"name"`
			} `json:"items"`
		}
		if err := httpx.DecodeMax(r, &in, 64<<10); err != nil {
			return err
		}
		if len(in.Items) > 5 {
			in.Items = in.Items[:5]
		}
		kind := sandboxprofile.TargetService
		if c.Service == "" {
			if kind, err = s.Profiles.KindOf(r.Context(), c.UserID); err != nil {
				return err
			}
		}
		profile := in.Profile
		if !utf8.ValidString(profile) || len(profile) > 40 || profile == "" {
			profile = "base"
		}
		for _, it := range in.Items {
			if it.Ecosystem != PythonModule && it.Ecosystem != Command {
				continue
			}
			if _, err := s.Record(r.Context(), Signal{Ecosystem: it.Ecosystem, Name: it.Name, Source: SourceAuto,
				Profile: profile, AgentKind: kind, Subject: c.Subject()}); err != nil {
				return err
			}
		}
		httpx.NoContent(w)
		return nil
	}))
}

// Purge deletes the signals older than 180 days (R12).
func (s *Service) Purge(ctx context.Context) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM package_signals WHERE day < current_date - 180`)
	return tag.RowsAffected(), err
}
