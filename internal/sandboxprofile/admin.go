package sandboxprofile

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/config"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// View is a profile in the administration (R14).
type View struct {
	Name        string     `json:"name"`
	Kind        string     `json:"kind"`
	ImageRef    string     `json:"imageRef"`
	ImageDigest string     `json:"imageDigest,omitempty"`
	PendingRef  string     `json:"pendingRef,omitempty"`
	Description string     `json:"description"`
	State       string     `json:"state"`
	Error       string     `json:"error,omitempty"`
	BaseVersion string     `json:"baseVersion,omitempty"`
	NodesReady  int        `json:"nodesReady"`
	NodesTotal  int        `json:"nodesTotal"`
	AssignedTo  Usage      `json:"assignedTo"`
	Inventory   *Manifest  `json:"inventory,omitempty"`
	CheckedAt   *time.Time `json:"checkedAt,omitempty"`
}

// List is the profiles with their assignments.
func (s *Service) List(ctx context.Context) ([]View, error) {
	rows, err := s.Pool.Query(ctx, `SELECT name, kind, image_ref, coalesce(image_digest, ''), coalesce(pending_ref, ''),
		description, state, coalesce(error, ''), coalesce(base_version, ''), nodes_ready, nodes_total, inventory, checked_at
		FROM sandbox_profiles ORDER BY kind, name`)
	if err != nil {
		return nil, err
	}
	var out []View
	for rows.Next() {
		var (
			v   View
			inv []byte
		)
		if err := rows.Scan(&v.Name, &v.Kind, &v.ImageRef, &v.ImageDigest, &v.PendingRef, &v.Description, &v.State, &v.Error,
			&v.BaseVersion, &v.NodesReady, &v.NodesTotal, &inv, &v.CheckedAt); err != nil {
			rows.Close()
			return nil, err
		}
		if len(inv) > 0 {
			var m Manifest
			if json.Unmarshal(inv, &m) == nil {
				v.Inventory = &m
			}
		}
		out = append(out, v)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		if out[i].AssignedTo, err = s.AssignedTo(ctx, out[i].Name); err != nil {
			return nil, err
		}
	}
	if out == nil {
		out = []View{}
	}
	return out, nil
}

// AllowedImage checks a custom image against the prefixes of the stand (R22).
func (s *Service) AllowedImage(ref string) error {
	ref = strings.TrimSpace(ref)
	bad := ref == "" || strings.ContainsAny(ref, " \t\n\\") || strings.Contains(ref, "..") || strings.Contains(ref, "//")
	if !bad {
		for _, p := range s.Cfg.Prefixes {
			if strings.HasPrefix(ref, p) && len(ref) > len(p) {
				return nil
			}
		}
	}
	return apperr.Unprocessable("sandbox_image_registry_denied", "the image is not from an allowed registry").
		With("field", "image").With("prefixes", s.Cfg.Prefixes)
}

// Create adds a custom profile; the worker checks it (R22).
func (s *Service) Create(ctx context.Context, actor, name, image, description string) error {
	if !s.Cfg.CustomImages {
		return apperr.Conflict("sandbox_custom_images_disabled", "custom images are off on this stand")
	}
	if !config.ProfileName(name) {
		return apperr.Unprocessable("sandbox_profile_name_invalid", "the name is lower-case letters, digits and dashes").With("field", "name")
	}
	if err := s.AllowedImage(image); err != nil {
		return err
	}
	if len([]rune(description)) > 1000 {
		return apperr.Unprocessable("sandbox_profile_description_invalid", "the description is up to 1000 characters").With("field", "description")
	}
	tag, err := s.Pool.Exec(ctx, `INSERT INTO sandbox_profiles (name, kind, image_ref, description, state, updated_by)
		VALUES ($1, 'custom', $2, $3, 'checking', $4) ON CONFLICT (name) DO NOTHING`, name, strings.TrimSpace(image), description, actor)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return apperr.Unprocessable("sandbox_profile_name_taken", "a profile with this name exists").With("field", "name")
	}
	return nil
}

func (s *Service) custom(ctx context.Context, name string) error {
	var kind string
	err := s.Pool.QueryRow(ctx, `SELECT kind FROM sandbox_profiles WHERE name = $1`, name).Scan(&kind)
	if postgres.IsNoRows(err) {
		return apperr.NotFound("sandbox_profile_not_found", "no such profile")
	}
	if err != nil {
		return err
	}
	if kind != KindCustom {
		return apperr.Unprocessable("sandbox_profile_builtin", "built-in profiles change with Nabu")
	}
	return nil
}

// Describe changes the description for the agent.
func (s *Service) Describe(ctx context.Context, actor, name, description string) error {
	if err := s.custom(ctx, name); err != nil {
		return err
	}
	if len([]rune(description)) > 1000 {
		return apperr.Unprocessable("sandbox_profile_description_invalid", "the description is up to 1000 characters").With("field", "description")
	}
	_, err := s.Pool.Exec(ctx, `UPDATE sandbox_profiles SET description = $2, updated_by = $3, updated_at = now() WHERE name = $1`, name, description, actor)
	return err
}

// Refresh checks the tag of a custom profile again (R24); the profile keeps
// its digest until the check passes.
func (s *Service) Refresh(ctx context.Context, actor, name string) error {
	if err := s.custom(ctx, name); err != nil {
		return err
	}
	_, err := s.Pool.Exec(ctx, `UPDATE sandbox_profiles SET pending_ref = image_ref, probe_report = NULL, probe_started_at = NULL,
		state = CASE WHEN image_digest IS NULL THEN 'checking' ELSE state END, updated_by = $2, updated_at = now() WHERE name = $1`, name, actor)
	return err
}

// Delete removes a custom profile that nothing uses (R27).
func (s *Service) Delete(ctx context.Context, name string) error {
	if err := s.custom(ctx, name); err != nil {
		return err
	}
	u, err := s.AssignedTo(ctx, name)
	if err != nil {
		return err
	}
	if u.Any() {
		return apperr.Conflict("sandbox_profile_in_use", "the profile is assigned").With("assignedTo", u)
	}
	_, err = s.Pool.Exec(ctx, `DELETE FROM sandbox_profiles WHERE name = $1`, name)
	return err
}

// Assign sets the profiles of personal and group agents (R6).
func (s *Service) Assign(ctx context.Context, actor string, in map[string]string) error {
	for t, name := range in {
		if t != TargetPersonal && t != TargetGroup {
			return apperr.Unprocessable("sandbox_assignment_invalid", "personal and group only").With("field", t)
		}
		ok, err := s.Ready(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			return apperr.Unprocessable("sandbox_profile_not_ready", "the profile is not ready").With("field", t)
		}
	}
	return postgres.InTx(ctx, s.Pool, func(tx pgx.Tx) error {
		for t, name := range in {
			if _, err := tx.Exec(ctx, `INSERT INTO sandbox_assignments (target, profile, updated_by) VALUES ($1, $2, $3)
				ON CONFLICT (target) DO UPDATE SET profile = EXCLUDED.profile, updated_by = EXCLUDED.updated_by, updated_at = now()`,
				t, name, actor); err != nil {
				return err
			}
		}
		return nil
	})
}

// AdminRoutes mounts /sandbox/profiles and /sandbox/assignments (tech §7.7).
func (s *Service) AdminRoutes(r chi.Router) {
	actor := func(r *http.Request) string {
		p, _ := httpx.MustPrincipal(r)
		if p == nil {
			return ""
		}
		return p.UserID.String()
	}
	r.Get("/sandbox/profiles", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		l, err := s.List(r.Context())
		if err != nil {
			return err
		}
		personal, err := s.Assigned(r.Context(), Target{Kind: TargetPersonal})
		if err != nil {
			return err
		}
		group, err := s.Assigned(r.Context(), Target{Kind: TargetGroup})
		if err != nil {
			return err
		}
		prefixes := s.Cfg.Prefixes
		if prefixes == nil {
			prefixes = []string{}
		}
		httpx.JSON(w, http.StatusOK, map[string]any{
			"customImages": s.Cfg.CustomImages, "prefixes": prefixes, "currentVersion": s.Cfg.Version,
			"profiles":    l,
			"assignments": map[string]any{"personal": personal, "group": group, "defaults": s.Cfg.Defaults},
		})
		return nil
	}))
	r.Post("/sandbox/profiles", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in struct{ Name, Image, Description string }
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := s.Create(r.Context(), actor(r), strings.TrimSpace(in.Name), in.Image, in.Description); err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, map[string]string{"name": in.Name, "state": StateChecking})
		return nil
	}))
	r.Patch("/sandbox/profiles/{name}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in struct{ Description string }
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := s.Describe(r.Context(), actor(r), chi.URLParam(r, "name"), in.Description); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Post("/sandbox/profiles/{name}/refresh", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		if err := s.Refresh(r.Context(), actor(r), chi.URLParam(r, "name")); err != nil {
			return err
		}
		w.WriteHeader(http.StatusAccepted)
		return nil
	}))
	r.Delete("/sandbox/profiles/{name}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		if err := s.Delete(r.Context(), chi.URLParam(r, "name")); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Put("/sandbox/assignments", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in map[string]string
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := s.Assign(r.Context(), actor(r), in); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
}
