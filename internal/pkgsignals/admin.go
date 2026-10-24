package pkgsignals

import (
	"context"
	_ "embed"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/sandboxprofile"
)

// Popular packages for the «looks like» mark (R17): the 5000 most downloaded
// PyPI projects (hugovk/top-pypi-packages) and the 2000 most installed Debian
// packages (popcon). `make update-popular` refreshes them every quarter.
var (
	//go:embed popular/pypi.txt
	popularPyPI string
	//go:embed popular/debian.txt
	popularDebian string

	popularOnce sync.Once
	popular     map[string][]string
)

func popularOf(ecosystem string) []string {
	popularOnce.Do(func() {
		popular = map[string][]string{PyPI: strings.Fields(popularPyPI), Apt: strings.Fields(popularDebian)}
	})
	return popular[ecosystem]
}

// Lookalike is a popular package a name differs from by at most two edits
// (a substitution of a typosquatter), or "".
func Lookalike(ecosystem, name string) string {
	list := popularOf(ecosystem)
	for _, p := range list {
		if p == name {
			return ""
		}
	}
	for _, p := range list {
		if d := len(p) - len(name); d > 2 || d < -2 {
			continue
		}
		if distance(name, p) <= 2 {
			return p
		}
	}
	return ""
}

// distance is the optimal string alignment distance (Damerau–Levenshtein with
// adjacent transpositions).
func distance(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	d := make([][]int, len(ra)+1)
	for i := range d {
		d[i] = make([]int, len(rb)+1)
		d[i][0] = i
	}
	for j := range d[0] {
		d[0][j] = j
	}
	for i := 1; i <= len(ra); i++ {
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			d[i][j] = min(d[i-1][j]+1, d[i][j-1]+1, d[i-1][j-1]+cost)
			if i > 1 && j > 1 && ra[i-1] == rb[j-2] && ra[i-2] == rb[j-1] {
				d[i][j] = min(d[i][j], d[i-2][j-2]+1)
			}
		}
	}
	return d[len(ra)][len(rb)]
}

// Row is a package of the table of requests (R15). Users are counted, never
// named.
type Row struct {
	Ecosystem string         `json:"ecosystem"`
	Name      string         `json:"name"`
	Aliases   []Alias        `json:"aliases"`
	Users     int            `json:"users"`
	Signals   int            `json:"signals"`
	BySource  map[string]int `json:"bySource"`
	ByProfile map[string]int `json:"byProfile"`
	ByKind    map[string]int `json:"byKind"`
	Previous  int            `json:"previousSignals"`
	New       bool           `json:"new"`
	LastAt    time.Time      `json:"lastAt"`
	Status    string         `json:"status"`
	Profile   string         `json:"profile,omitempty"`
	Version   string         `json:"version,omitempty"`
	Note      string         `json:"note,omitempty"`
	Lookalike string         `json:"lookalike,omitempty"`
}

// Alias is a module or a command mapped to a package.
type Alias struct {
	Ecosystem string `json:"ecosystem"`
	Name      string `json:"name"`
	Builtin   bool   `json:"builtin"`
}

// Filter narrows the table.
type Filter struct {
	Days                      int
	Ecosystem, Status, Source string
	Query                     string
}

const signalsOf = `WITH s AS (
	SELECT coalesce(a.package_ecosystem, p.ecosystem) AS eco, coalesce(a.package_name, p.name) AS pkg, p.*
	FROM package_signals p LEFT JOIN package_aliases a ON a.ecosystem = p.ecosystem AND a.name = p.name
	WHERE p.day > current_date - $1::int AND p.day <= current_date - $2::int AND ($3 = '' OR p.source = $3))`

// Table is the packages of a period, by unique users (tech §7.7).
func (s *Service) Table(ctx context.Context, f Filter) ([]Row, error) {
	rows, err := s.Pool.Query(ctx, signalsOf+`
	SELECT s.eco, s.pkg, count(DISTINCT s.subject), count(*), max(s.created_at),
		count(*) FILTER (WHERE s.source = 'agent'), count(*) FILTER (WHERE s.source = 'auto'),
		count(*) FILTER (WHERE s.agent_kind = 'personal'), count(*) FILTER (WHERE s.agent_kind = 'group'),
		count(*) FILTER (WHERE s.agent_kind = 'service'),
		coalesce(st.status, 'new'), coalesce(st.profile, ''), coalesce(st.version, ''), coalesce(st.note, '')
	FROM s LEFT JOIN package_status st ON st.package_ecosystem = s.eco AND st.package_name = s.pkg
	WHERE ($4 = '' OR s.eco = $4 OR ($4 = 'python-module' AND s.ecosystem = 'python-module') OR ($4 = 'command' AND s.ecosystem = 'command'))
	  AND ($5 = '' OR coalesce(st.status, 'new') = $5) AND ($6 = '' OR s.pkg ILIKE '%' || $6 || '%')
	GROUP BY s.eco, s.pkg, st.status, st.profile, st.version, st.note
	ORDER BY count(DISTINCT s.subject) DESC, count(*) DESC, s.pkg LIMIT 500`,
		f.Days, 0, f.Source, f.Ecosystem, f.Status, f.Query)
	if err != nil {
		return nil, err
	}
	var out []Row
	idx := map[string]int{}
	for rows.Next() {
		var r Row
		var agent, auto, personal, group, service int
		if err := rows.Scan(&r.Ecosystem, &r.Name, &r.Users, &r.Signals, &r.LastAt, &agent, &auto, &personal, &group, &service,
			&r.Status, &r.Profile, &r.Version, &r.Note); err != nil {
			rows.Close()
			return nil, err
		}
		r.BySource = map[string]int{SourceAgent: agent, SourceAuto: auto}
		r.ByKind = map[string]int{sandboxprofile.TargetPersonal: personal, sandboxprofile.TargetGroup: group, sandboxprofile.TargetService: service}
		r.ByProfile = map[string]int{}
		r.Aliases = []Alias{}
		r.Lookalike = Lookalike(r.Ecosystem, r.Name)
		idx[r.Ecosystem+"\x00"+r.Name] = len(out)
		out = append(out, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return []Row{}, nil
	}
	// By profile, the previous period of the same size (the trend) and aliases.
	prof, err := s.Pool.Query(ctx, signalsOf+` SELECT eco, pkg, profile, count(*) FROM s GROUP BY eco, pkg, profile`, f.Days, 0, f.Source)
	if err != nil {
		return nil, err
	}
	for prof.Next() {
		var eco, pkg, p string
		var n int
		if prof.Scan(&eco, &pkg, &p, &n) == nil {
			if i, ok := idx[eco+"\x00"+pkg]; ok {
				out[i].ByProfile[p] = n
			}
		}
	}
	prof.Close()
	prev, err := s.Pool.Query(ctx, signalsOf+` SELECT eco, pkg, count(*) FROM s GROUP BY eco, pkg`, 2*f.Days, f.Days, f.Source)
	if err != nil {
		return nil, err
	}
	for prev.Next() {
		var eco, pkg string
		var n int
		if prev.Scan(&eco, &pkg, &n) == nil {
			if i, ok := idx[eco+"\x00"+pkg]; ok {
				out[i].Previous = n
			}
		}
	}
	prev.Close()
	for i := range out {
		out[i].New = out[i].Previous == 0
	}
	al, err := s.Pool.Query(ctx, `SELECT package_ecosystem, package_name, ecosystem, name, builtin FROM package_aliases ORDER BY name`)
	if err != nil {
		return nil, err
	}
	for al.Next() {
		var eco, pkg string
		var a Alias
		if al.Scan(&eco, &pkg, &a.Ecosystem, &a.Name, &a.Builtin) == nil {
			if i, ok := idx[eco+"\x00"+pkg]; ok {
				out[i].Aliases = append(out[i].Aliases, a)
			}
		}
	}
	al.Close()
	return out, nil
}

// Reason is a reason of an agent without its author.
type Reason struct {
	Text    string    `json:"text"`
	Profile string    `json:"profile"`
	At      time.Time `json:"at"`
}

// Change is a change of the status of a package.
type Change struct {
	Status  string    `json:"status"`
	Profile string    `json:"profile,omitempty"`
	Version string    `json:"version,omitempty"`
	Note    string    `json:"note,omitempty"`
	By      string    `json:"by,omitempty"`
	At      time.Time `json:"at"`
}

// Card is a package with its reasons, daily signals and history.
type Card struct {
	Row
	Reasons []Reason         `json:"reasons"`
	Daily   []map[string]any `json:"daily"`
	History []Change         `json:"history"`
}

// CardOf loads the card of a package for a period.
func (s *Service) CardOf(ctx context.Context, ecosystem, name string, days int) (*Card, error) {
	all, err := s.Table(ctx, Filter{Days: days})
	if err != nil {
		return nil, err
	}
	c := &Card{Reasons: []Reason{}, Daily: []map[string]any{}, History: []Change{}}
	found := false
	for _, r := range all {
		if r.Ecosystem == ecosystem && r.Name == name {
			c.Row, found = r, true
		}
	}
	if !found {
		c.Row = Row{Ecosystem: ecosystem, Name: name, Status: "new", Aliases: []Alias{}, BySource: map[string]int{},
			ByProfile: map[string]int{}, ByKind: map[string]int{}}
		_ = s.Pool.QueryRow(ctx, `SELECT status, coalesce(profile, ''), coalesce(version, ''), coalesce(note, '') FROM package_status
			WHERE package_ecosystem = $1 AND package_name = $2`, ecosystem, name).Scan(&c.Status, &c.Profile, &c.Version, &c.Note)
	}
	rs, err := s.Pool.Query(ctx, signalsOf+` SELECT reason, profile, created_at FROM s
		WHERE eco = $4 AND pkg = $5 AND reason IS NOT NULL ORDER BY created_at DESC LIMIT 5`, days, 0, "", ecosystem, name)
	if err != nil {
		return nil, err
	}
	for rs.Next() {
		var r Reason
		if rs.Scan(&r.Text, &r.Profile, &r.At) == nil {
			c.Reasons = append(c.Reasons, r)
		}
	}
	rs.Close()
	ds, err := s.Pool.Query(ctx, signalsOf+` SELECT day, count(*) FROM s WHERE eco = $4 AND pkg = $5 GROUP BY day ORDER BY day`,
		days, 0, "", ecosystem, name)
	if err != nil {
		return nil, err
	}
	for ds.Next() {
		var day time.Time
		var n int
		if ds.Scan(&day, &n) == nil {
			c.Daily = append(c.Daily, map[string]any{"day": day.Format("2006-01-02"), "signals": n})
		}
	}
	ds.Close()
	hs, err := s.Pool.Query(ctx, `SELECT status, coalesce(profile, ''), coalesce(version, ''), coalesce(note, ''), coalesce(updated_by, ''), created_at
		FROM package_status_log WHERE package_ecosystem = $1 AND package_name = $2 ORDER BY created_at DESC LIMIT 50`, ecosystem, name)
	if err != nil {
		return nil, err
	}
	for hs.Next() {
		var h Change
		if hs.Scan(&h.Status, &h.Profile, &h.Version, &h.Note, &h.By, &h.At) == nil {
			c.History = append(c.History, h)
		}
	}
	hs.Close()
	return c, nil
}

// SetStatus is a decision of the administrator (R16).
func (s *Service) SetStatus(ctx context.Context, actor, ecosystem, name, status, profile, version, note string) error {
	switch status {
	case StatusNew, StatusPlanned, StatusAdded:
	case StatusRejected:
		if strings.TrimSpace(note) == "" {
			return apperr.Unprocessable("sandbox_reject_note_required", "give a reason: the agent passes it to the user").With("field", "note")
		}
	default:
		return apperr.Unprocessable("sandbox_status_invalid", "status is new, planned, added or rejected").With("field", "status")
	}
	if r := []rune(note); len(r) > MaxReason {
		return apperr.Unprocessable("sandbox_status_invalid", "the note is up to 300 characters").With("field", "note")
	}
	return s.setStatus(ctx, ecosystem, name, Change{Status: status, Profile: profile, Version: version, Note: strings.TrimSpace(note), By: actor})
}

// Map maps a module or a command to a package (R13).
func (s *Service) Map(ctx context.Context, ecosystem, name, pkgEcosystem, pkgName string) error {
	if ecosystem != PythonModule && ecosystem != Command {
		return apperr.Unprocessable("sandbox_alias_invalid", "map a python-module or a command").With("field", "ecosystem")
	}
	n, ok1 := Normalize(ecosystem, name)
	p, ok2 := Normalize(pkgEcosystem, pkgName)
	if !ok1 || !ok2 || (pkgEcosystem != PyPI && pkgEcosystem != Apt && pkgEcosystem != Binary) {
		return apperr.Unprocessable("sandbox_alias_invalid", "not a valid module or package").With("field", "packageName")
	}
	tag, err := s.Pool.Exec(ctx, `INSERT INTO package_aliases (ecosystem, name, package_ecosystem, package_name) VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING`, ecosystem, n, pkgEcosystem, p)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return apperr.Conflict("sandbox_alias_exists", "the module is already mapped")
	}
	_, err = s.Pool.Exec(ctx, `INSERT INTO package_status (package_ecosystem, package_name, status) VALUES ($1, $2, 'new') ON CONFLICT DO NOTHING`, pkgEcosystem, p)
	return err
}

// Unmap removes a mapping of the administrator; built-in ones stay.
func (s *Service) Unmap(ctx context.Context, ecosystem, name string) error {
	var builtin bool
	err := s.Pool.QueryRow(ctx, `SELECT builtin FROM package_aliases WHERE ecosystem = $1 AND name = $2`, ecosystem, name).Scan(&builtin)
	if postgres.IsNoRows(err) {
		return apperr.NotFound("sandbox_alias_not_found", "no such mapping")
	}
	if err != nil {
		return err
	}
	if builtin {
		return apperr.Conflict("sandbox_alias_builtin", "built-in mappings stay")
	}
	_, err = s.Pool.Exec(ctx, `DELETE FROM package_aliases WHERE ecosystem = $1 AND name = $2`, ecosystem, name)
	return err
}

func period(r *http.Request) (int, error) {
	switch r.URL.Query().Get("period") {
	case "", "30d":
		return 30, nil
	case "7d":
		return 7, nil
	case "90d":
		return 90, nil
	}
	return 0, apperr.BadRequest("invalid_period", "period is 7d, 30d or 90d")
}

// AdminRoutes mounts /sandbox/packages and /sandbox/aliases (tech §7.7).
func (s *Service) AdminRoutes(r chi.Router) {
	actor := func(r *http.Request) string {
		if p, _ := httpx.MustPrincipal(r); p != nil {
			return p.UserID.String()
		}
		return ""
	}
	r.Get("/sandbox/packages", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		days, err := period(r)
		if err != nil {
			return err
		}
		q := r.URL.Query()
		l, err := s.Table(r.Context(), Filter{Days: days, Ecosystem: q.Get("ecosystem"), Status: q.Get("status"), Source: q.Get("source"), Query: q.Get("q")})
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusOK, map[string]any{"items": l})
		return nil
	}))
	r.Get("/sandbox/packages/{ecosystem}/{name}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		days, err := period(r)
		if err != nil {
			return err
		}
		c, err := s.CardOf(r.Context(), chi.URLParam(r, "ecosystem"), chi.URLParam(r, "name"), days)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusOK, c)
		return nil
	}))
	r.Put("/sandbox/packages/{ecosystem}/{name}/status", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in struct{ Status, Profile, Version, Note string }
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := s.SetStatus(r.Context(), actor(r), chi.URLParam(r, "ecosystem"), chi.URLParam(r, "name"),
			in.Status, in.Profile, in.Version, in.Note); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Post("/sandbox/aliases", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in struct{ Ecosystem, Name, PackageEcosystem, PackageName string }
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := s.Map(r.Context(), in.Ecosystem, in.Name, in.PackageEcosystem, in.PackageName); err != nil {
			return err
		}
		w.WriteHeader(http.StatusCreated)
		return nil
	}))
	r.Delete("/sandbox/aliases/{ecosystem}/{name}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		if err := s.Unmap(r.Context(), chi.URLParam(r, "ecosystem"), chi.URLParam(r, "name")); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
}
