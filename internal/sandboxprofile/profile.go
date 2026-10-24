// Package sandboxprofile is the profiles of sandbox images
// (FTR.NAB.CMN-0008): built-in profiles built with Nabu and declared by the
// stand, custom images of the installation team checked by a probe pod, the
// assignment of profiles to personal, group and service agents, the
// preloading of their images on the nodes.
package sandboxprofile

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Kinds of profiles and targets of assignments.
const (
	KindBuiltin = "builtin"
	KindCustom  = "custom"

	TargetPersonal = "personal"
	TargetGroup    = "group"
	TargetService  = "service"

	StateChecking = "checking"
	StateReady    = "ready"
	StateError    = "error"
)

// Config is the stand's part of the profiles (tech §3).
type Config struct {
	// Profiles are the built-in profiles: name → image with the tag of the
	// Nabu version. Empty — the single legacy profile base = LegacyImage.
	Profiles     map[string]string
	Defaults     map[string]string // personal, group, service → profile
	LegacyImage  string            // SANDBOX_IMAGE
	CustomImages bool
	Prefixes     []string
	// ProfilesDir holds the manifests of built-in profiles (/opt/nabu/profiles).
	ProfilesDir string
	Version     string
}

// Legacy reports that no profiles are declared: sandboxes run SANDBOX_IMAGE
// as before the feature, without the init container (tech §3).
func (c Config) Legacy() bool { return len(c.Profiles) == 0 }

// Manifest is the content of a profile (tech §2.3): /nabu/sandbox-profile.json
// in a sandbox, the inventory of a profile in the database.
type Manifest struct {
	Name        string    `json:"name"`
	Kind        string    `json:"kind"`
	Version     string    `json:"version,omitempty"`
	Digest      string    `json:"digest,omitempty"`
	Tools       []Tool    `json:"tools,omitempty"`
	Python      []Package `json:"python,omitempty"`
	System      []Package `json:"system,omitempty"`
	Commands    []string  `json:"commands,omitempty"`
	Includes    string    `json:"includes,omitempty"`
	Description string    `json:"description,omitempty"`
}

// Tool is a tool of a built-in profile with its group (R2, R3).
type Tool struct {
	Group   string `json:"group"`
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// Package is a package with its version.
type Package struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// Has reports whether the profile contains a package of an ecosystem
// (pypi — Python packages, apt — system packages, binary — commands).
func (m *Manifest) Has(ecosystem, name string) bool {
	if m == nil {
		return false
	}
	switch ecosystem {
	case "pypi":
		for _, p := range m.Python {
			if NormalizePyPI(p.Name) == name {
				return true
			}
		}
	case "apt":
		for _, p := range m.System {
			if strings.ToLower(p.Name) == name {
				return true
			}
		}
	case "binary", "command":
		for _, c := range m.Commands {
			if c == name {
				return true
			}
		}
		for _, t := range m.Tools {
			if t.Name == name {
				return true
			}
		}
	}
	return false
}

// NormalizePyPI is the name of a PyPI package by PEP 503.
func NormalizePyPI(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	sep := false
	for _, r := range s {
		if r == '-' || r == '_' || r == '.' {
			sep = true
			continue
		}
		if sep && b.Len() > 0 {
			b.WriteByte('-')
		}
		sep = false
		b.WriteRune(r)
	}
	return b.String()
}

// Profile is what a sandbox is started from.
type Profile struct {
	Name, Kind string
	// Image is the reference for the pod: the tag of a built-in profile or
	// repository@digest of a custom one.
	Image       string
	Legacy      bool
	Description string
	Inventory   *Manifest
}

// Target is whom a sandbox is for.
type Target struct {
	Kind    string // personal | group | service
	UserID  uuid.UUID
	Profile string // a service agent's workspaceProfile; "" — the default
}

// Service reads profiles and assignments.
type Service struct {
	Pool *pgxpool.Pool
	Cfg  Config
}

// KindOf tells personal from group spaces: a group agent's data belong to its
// technical user (FTR.NAB.CMN-0002 R16).
func (s *Service) KindOf(ctx context.Context, uid uuid.UUID) (string, error) {
	var group bool
	err := s.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM group_agents WHERE data_user_id = $1)`, uid).Scan(&group)
	if err != nil {
		return "", err
	}
	if group {
		return TargetGroup, nil
	}
	return TargetPersonal, nil
}

// Assigned is the profile name assigned to a target (R6): the administration
// for personal and group agents, the agent's configuration for a service
// agent, the defaults of the stand otherwise.
func (s *Service) Assigned(ctx context.Context, t Target) (string, error) {
	switch t.Kind {
	case TargetService:
		if t.Profile != "" {
			return t.Profile, nil
		}
	case TargetPersonal, TargetGroup:
		var name string
		err := s.Pool.QueryRow(ctx, `SELECT profile FROM sandbox_assignments WHERE target = $1`, t.Kind).Scan(&name)
		if err == nil {
			return name, nil
		}
		if !postgres.IsNoRows(err) {
			return "", err
		}
	default:
		return "", errors.New("sandboxprofile: unknown target " + t.Kind)
	}
	return s.Cfg.Defaults[t.Kind], nil
}

// Resolve is the profile a sandbox of the target starts from. A profile that
// is not ready (deleted, failed after an assignment) falls back to the default
// of the target and then to base, with a warning: the administration does not
// let such a profile be assigned, so this is a repair path.
func (s *Service) Resolve(ctx context.Context, t Target) (Profile, error) {
	if s.Cfg.Legacy() {
		return Profile{Name: "base", Kind: KindBuiltin, Image: s.Cfg.LegacyImage, Legacy: true}, nil
	}
	name, err := s.Assigned(ctx, t)
	if err != nil {
		return Profile{}, err
	}
	for _, n := range []string{name, s.Cfg.Defaults[t.Kind], "base"} {
		p, err := s.load(ctx, n)
		if err == nil {
			if n != name {
				slog.WarnContext(ctx, "sandbox profile not ready, falling back", "profile", name, "fallback", n)
			}
			return p, nil
		}
		if !errors.Is(err, errNotReady) {
			return Profile{}, err
		}
	}
	// No row yet (the api has not synced): the built-in base of the stand.
	if img, ok := s.Cfg.Profiles["base"]; ok {
		return Profile{Name: "base", Kind: KindBuiltin, Image: img}, nil
	}
	return Profile{}, errors.New("sandboxprofile: no ready profile")
}

var errNotReady = errors.New("profile not ready")

func (s *Service) load(ctx context.Context, name string) (Profile, error) {
	var (
		p          Profile
		ref, state string
		digest     *string
		inv        []byte
	)
	err := s.Pool.QueryRow(ctx, `SELECT name, kind, image_ref, image_digest, state, description, inventory
		FROM sandbox_profiles WHERE name = $1`, name).Scan(&p.Name, &p.Kind, &ref, &digest, &state, &p.Description, &inv)
	if postgres.IsNoRows(err) {
		return p, errNotReady
	}
	if err != nil {
		return p, err
	}
	// A custom profile being refreshed stays ready on its old digest.
	if state != StateReady && !(p.Kind == KindCustom && digest != nil && *digest != "") {
		return p, errNotReady
	}
	p.Image = ref
	if p.Kind == KindCustom {
		if digest == nil || *digest == "" {
			return p, errNotReady
		}
		p.Image = ImageAt(ref, *digest)
	}
	if len(inv) > 0 {
		var m Manifest
		if json.Unmarshal(inv, &m) == nil {
			p.Inventory = &m
		}
	}
	return p, nil
}

// ImageAt is repository@digest of a reference with a tag or a digest.
func ImageAt(ref, digest string) string {
	repo := ref
	if i := strings.Index(repo, "@"); i >= 0 {
		repo = repo[:i]
	}
	if i := strings.LastIndex(repo, ":"); i > strings.LastIndex(repo, "/") {
		repo = repo[:i]
	}
	return repo + "@" + digest
}

// SyncBuiltins writes the built-in profiles of the stand into the database at
// the start of the api (tech §5.2): the image, the manifest from the image of
// nabu-core, ready. Built-in profiles gone from the stand are removed, or
// marked failed while something is assigned to them. It returns the
// manifests of the profiles for the automatic «added» status of packages.
func (s *Service) SyncBuiltins(ctx context.Context) ([]Manifest, error) {
	if s.Cfg.Legacy() {
		return nil, nil
	}
	var out []Manifest
	names := make([]string, 0, len(s.Cfg.Profiles))
	for n := range s.Cfg.Profiles {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		m := s.builtinManifest(n)
		inv, _ := json.Marshal(m)
		if _, err := s.Pool.Exec(ctx, `INSERT INTO sandbox_profiles (name, kind, image_ref, state, inventory, base_version, checked_at, updated_by)
			VALUES ($1, 'builtin', $2, 'ready', $3, $4, now(), 'system')
			ON CONFLICT (name) DO UPDATE SET kind = 'builtin', image_ref = EXCLUDED.image_ref, image_digest = NULL,
				state = 'ready', error = NULL, pending_ref = NULL, inventory = EXCLUDED.inventory,
				base_version = EXCLUDED.base_version, checked_at = now(), updated_at = now()
			WHERE sandbox_profiles.kind = 'builtin'`, n, s.Cfg.Profiles[n], inv, s.Cfg.Version); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	rows, err := s.Pool.Query(ctx, `SELECT name FROM sandbox_profiles WHERE kind = 'builtin' AND NOT (name = ANY($1))`, names)
	if err != nil {
		return nil, err
	}
	gone, err := pgxCollect(rows)
	if err != nil {
		return nil, err
	}
	for _, n := range gone {
		used, err := s.AssignedTo(ctx, n)
		if err != nil {
			return nil, err
		}
		if used.Any() {
			_, err = s.Pool.Exec(ctx, `UPDATE sandbox_profiles SET state = 'error', error = 'sandbox_profile_removed', updated_at = now() WHERE name = $1`, n)
		} else {
			_, err = s.Pool.Exec(ctx, `DELETE FROM sandbox_profiles WHERE name = $1`, n)
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// builtinManifest reads the manifest of a built-in profile shipped in the
// image of nabu-core; without it (development) — the name only.
func (s *Service) builtinManifest(name string) Manifest {
	m := Manifest{Name: name, Kind: KindBuiltin, Version: s.Cfg.Version}
	if s.Cfg.ProfilesDir == "" {
		return m
	}
	b, err := os.ReadFile(filepath.Join(s.Cfg.ProfilesDir, name+".json"))
	if err != nil {
		slog.Warn("no manifest of a built-in sandbox profile", "profile", name, "err", err)
		return m
	}
	if err := json.Unmarshal(b, &m); err != nil {
		slog.Warn("bad manifest of a built-in sandbox profile", "profile", name, "err", err)
	}
	m.Name, m.Kind = name, KindBuiltin
	if m.Version == "" {
		m.Version = s.Cfg.Version
	}
	return m
}

// Usage is whom a profile is assigned to.
type Usage struct {
	Personal bool     `json:"personal"`
	Group    bool     `json:"group"`
	Services []string `json:"services"`
}

// Any reports any assignment.
func (u Usage) Any() bool { return u.Personal || u.Group || len(u.Services) > 0 }

// AssignedTo lists the assignments of a profile, the defaults of the stand
// included.
func (s *Service) AssignedTo(ctx context.Context, name string) (Usage, error) {
	u := Usage{Services: []string{}}
	for _, t := range []string{TargetPersonal, TargetGroup} {
		n, err := s.Assigned(ctx, Target{Kind: t})
		if err != nil {
			return u, err
		}
		if n == name {
			if t == TargetPersonal {
				u.Personal = true
			} else {
				u.Group = true
			}
		}
	}
	rows, err := s.Pool.Query(ctx, `SELECT name FROM service_agents
		WHERE config->>'workspace' = 'nabu' AND NOT coalesce((config->>'builtin')::boolean, false)
		  AND coalesce(nullif(config->>'workspaceProfile', ''), $2) = $1 ORDER BY name`, name, s.Cfg.Defaults[TargetService])
	if err != nil {
		return u, err
	}
	u.Services, err = pgxCollect(rows)
	if u.Services == nil {
		u.Services = []string{}
	}
	return u, err
}

// Assignments are the names of the profiles in use: personal, group and
// every service agent with a workspace in Nabu.
func (s *Service) Assignments(ctx context.Context) ([]string, error) {
	seen := map[string]bool{}
	for _, t := range []string{TargetPersonal, TargetGroup} {
		n, err := s.Assigned(ctx, Target{Kind: t})
		if err != nil {
			return nil, err
		}
		seen[n] = true
	}
	rows, err := s.Pool.Query(ctx, `SELECT DISTINCT coalesce(nullif(config->>'workspaceProfile', ''), $1) FROM service_agents
		WHERE config->>'workspace' = 'nabu' AND enabled`, s.Cfg.Defaults[TargetService])
	if err != nil {
		return nil, err
	}
	names, err := pgxCollect(rows)
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		seen[n] = true
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// Ready reports that a profile exists and can be assigned.
func (s *Service) Ready(ctx context.Context, name string) (bool, error) {
	_, err := s.load(ctx, name)
	if errors.Is(err, errNotReady) {
		return false, nil
	}
	return err == nil, err
}

// Manifests are the inventories of the ready profiles (the «available» status
// of a requested package).
func (s *Service) Manifests(ctx context.Context) ([]Manifest, error) {
	rows, err := s.Pool.Query(ctx, `SELECT name, kind, inventory, description FROM sandbox_profiles
		WHERE inventory IS NOT NULL AND (state = 'ready' OR (kind = 'custom' AND image_digest IS NOT NULL)) ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Manifest
	for rows.Next() {
		var (
			name, kind, desc string
			inv              []byte
		)
		if err := rows.Scan(&name, &kind, &inv, &desc); err != nil {
			return nil, err
		}
		var m Manifest
		if json.Unmarshal(inv, &m) != nil {
			continue
		}
		m.Name, m.Kind, m.Description = name, kind, desc
		out = append(out, m)
	}
	return out, rows.Err()
}

func pgxCollect(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close()
}) ([]string, error) {
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
