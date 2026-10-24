package sandboxprofile

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Caller is whoever calls /internal/v1/sandbox/…: the sandbox of a user, the
// temporary sandbox of a service agent's run or the probe of a custom image.
type Caller struct {
	UserID  uuid.UUID // a sandbox of a space
	RunID   string    // a temporary sandbox of a run
	Service string    // the service agent of the run
	Probe   string    // the profile a probe pod checks
}

// Subject is how signals count the caller (tech §4): the user or the service
// agent.
func (c Caller) Subject() string {
	if c.Service != "" {
		return "service:" + c.Service
	}
	return c.UserID.String()
}

// RunToken is the token of a run's temporary sandbox: it reports signals and
// reads its profile (tech §5.3).
func RunToken(s *jwt.Signer, runID string, ttl time.Duration) string {
	return s.Issue(jwt.Claims{Audience: jwt.AudSandbox, Subject: "run", Run: runID}, ttl)
}

// ProbeToken is the token of the probe pod of a custom profile.
func ProbeToken(s *jwt.Signer, profile string, ttl time.Duration) string {
	return s.Issue(jwt.Claims{Audience: jwt.AudSandbox, Subject: "probe:" + profile}, ttl)
}

// Authenticate reads the caller from a sandbox, run or probe token.
func (s *Service) Authenticate(ctx context.Context, signer *jwt.Signer, r *http.Request) (Caller, error) {
	c, err := signer.Verify(httpx.Bearer(r), jwt.AudSandbox)
	if err != nil {
		return Caller{}, apperr.Forbidden("forbidden", "a sandbox token is required")
	}
	switch {
	case c.Subject == "sandbox":
		uid, err := uuid.Parse(c.User)
		if err != nil {
			return Caller{}, apperr.Forbidden("forbidden", "a sandbox token is required")
		}
		return Caller{UserID: uid}, nil
	case c.Subject == "run" && c.Run != "":
		var agent string
		err := s.Pool.QueryRow(ctx, `SELECT agent FROM runs WHERE id::text = $1`, c.Run).Scan(&agent)
		if postgres.IsNoRows(err) {
			return Caller{}, apperr.Forbidden("forbidden", "unknown run")
		}
		if err != nil {
			return Caller{}, err
		}
		return Caller{RunID: c.Run, Service: agent}, nil
	case strings.HasPrefix(c.Subject, "probe:"):
		return Caller{Probe: strings.TrimPrefix(c.Subject, "probe:")}, nil
	}
	return Caller{}, apperr.Forbidden("forbidden", "a sandbox token is required")
}

// ManifestFor is the content of the profile a caller's sandbox runs, with the
// description of the administrator (R5, R25).
func (s *Service) ManifestFor(ctx context.Context, c Caller) (*Manifest, error) {
	t := Target{Kind: TargetService}
	if c.Service != "" {
		var p string
		_ = s.Pool.QueryRow(ctx, `SELECT coalesce(config->>'workspaceProfile', '') FROM service_agents WHERE name = $1`, c.Service).Scan(&p)
		t.Profile = p
	} else {
		k, err := s.KindOf(ctx, c.UserID)
		if err != nil {
			return nil, err
		}
		t = Target{Kind: k, UserID: c.UserID}
	}
	p, err := s.running(ctx, c)
	if errors.Is(err, errNotReady) {
		p, err = s.Resolve(ctx, t)
	}
	if err != nil {
		return nil, err
	}
	m := Manifest{Name: p.Name, Kind: p.Kind}
	if p.Inventory != nil {
		m = *p.Inventory
		m.Name, m.Kind = p.Name, p.Kind
	}
	m.Description = p.Description
	if i := strings.Index(p.Image, "@"); i >= 0 {
		m.Digest = p.Image[i+1:]
	}
	return &m, nil
}

// running is the profile the sandbox of a space runs now (decision 15): after
// a new assignment the sandbox keeps the old image until it restarts idle, and
// the agent must see what it has. errNotReady — no running sandbox or no such
// profile any more: the assigned one is meant.
func (s *Service) running(ctx context.Context, c Caller) (Profile, error) {
	if c.UserID == uuid.Nil || s.Cfg.Legacy() {
		return Profile{}, errNotReady
	}
	var name string
	err := s.Pool.QueryRow(ctx, `SELECT profile FROM spaces WHERE user_id = $1 AND state IN ('starting', 'running') AND profile IS NOT NULL`,
		c.UserID).Scan(&name)
	if postgres.IsNoRows(err) {
		return Profile{}, errNotReady
	}
	if err != nil {
		return Profile{}, err
	}
	return s.load(ctx, name)
}

// ProbeReport is what a probe pod sends (tech §5.7).
type ProbeReport struct {
	OK        bool      `json:"ok"`
	Error     string    `json:"error,omitempty"` // sandbox_probe_no_bash | sandbox_probe_user | sandbox_probe_fs
	Inventory *Manifest `json:"inventory,omitempty"`
}

// InternalRoutes mounts the profile of a sandbox and the report of a probe
// under /internal/v1/sandbox.
func (s *Service) InternalRoutes(r chi.Router, signer *jwt.Signer) {
	r.Get("/internal/v1/sandbox/profile", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		c, err := s.Authenticate(r.Context(), signer, r)
		if err != nil {
			return err
		}
		if c.Probe != "" {
			return apperr.Forbidden("forbidden", "a probe has no profile")
		}
		m, err := s.ManifestFor(r.Context(), c)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusOK, m)
		return nil
	}))
	r.Post("/internal/v1/sandbox/probe", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		c, err := s.Authenticate(r.Context(), signer, r)
		if err != nil {
			return err
		}
		if c.Probe == "" {
			return apperr.Forbidden("forbidden", "a probe token is required")
		}
		b, err := io.ReadAll(io.LimitReader(r.Body, 2<<20+1))
		if err != nil {
			return err
		}
		if len(b) > 2<<20 {
			return apperr.TooLarge("probe_report_too_large", "the report is over 2 MB")
		}
		var rep ProbeReport
		if err := json.Unmarshal(b, &rep); err != nil {
			return apperr.BadRequest("invalid_body", "the report is not JSON")
		}
		// The first report of a running probe counts; a pod cannot rewrite it.
		if _, err := s.Pool.Exec(r.Context(), `UPDATE sandbox_profiles SET probe_report = $2
			WHERE name = $1 AND kind = 'custom' AND probe_started_at IS NOT NULL AND probe_report IS NULL`, c.Probe, b); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
}
