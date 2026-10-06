// Package catalog is the catalog of skills and MCP servers (FTR.NAB.CMN-0001
// R11–R14, arch §8, tech §3.5, §4): administrators publish items, users
// connect them to their agent. An MCP item has an access mode: personal (the
// user's OAuth or personal token, the agent acts with the user's rights) or
// platform (a service credential of the administrator, read-only by default).
//
// Personal and platform credentials never reach the agent: sessions talk to
// catalog servers through the MCP proxy of Nabu (proxy.go), which adds the
// credentials and, for read-only items, hides and refuses tools that change
// data (CAT-04).
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/platform/storage"
)

// Source is where an item comes from.
type Source struct {
	Kind string `json:"kind"` // url (mcp) | upload | git (skill)
	URL  string `json:"url,omitempty"`
	Repo string `json:"repo,omitempty"` // https URL or owner/name on GitHub
	Path string `json:"path,omitempty"`
	Ref  string `json:"ref,omitempty"`
}

// PersonalAuth is how a user grants personal access.
type PersonalAuth struct {
	Kind         string   `json:"kind"` // oauth | token | delegation
	AuthorizeURL string   `json:"authorizeUrl,omitempty"`
	TokenURL     string   `json:"tokenUrl,omitempty"`
	ClientID     string   `json:"clientId,omitempty"`
	ClientSecret string   `json:"clientSecret,omitempty"` // input only; stored encrypted
	Scopes       []string `json:"scopes,omitempty"`
	// Header and Prefix place a personal token: Authorization: Bearer <token> by default.
	Header string `json:"header,omitempty"`
	Prefix string `json:"prefix,omitempty"`
	// Audience of the Nabu JWT for delegation to a product (FTR.HMR.CMN-0006 tech §3.6).
	Audience  string `json:"audience,omitempty"`
	HasSecret bool   `json:"hasSecret,omitempty"`
}

// Item is a catalog item as administrators see it (secrets as last4 only).
type Item struct {
	ID            uuid.UUID         `json:"id"`
	Type          string            `json:"type"` // mcp | skill
	Name          string            `json:"name"`
	Title         string            `json:"title"`
	Description   string            `json:"description"`
	Source        Source            `json:"source"`
	Mode          *string           `json:"mode"`
	PersonalAuth  *PersonalAuth     `json:"personalAuth"`
	PlatformAuth  map[string]string `json:"platformAuth"` // header → last4
	ReadOnly      bool              `json:"readOnly"`
	Exposure      string            `json:"exposure"`
	Published     bool              `json:"published"`
	InDevelopment bool              `json:"inDevelopment"`
	Status        *string           `json:"status"`
	StatusReason  *string           `json:"statusReason"`
	Tools         []agent.MCPTool   `json:"tools"`
	Skills        []string          `json:"skills"`
	SyncedAt      *time.Time        `json:"syncedAt"`
	CreatedAt     time.Time         `json:"createdAt"`
	snapshot      *string
}

// Service is the catalog.
type Service struct {
	Pool   *pgxpool.Pool
	Box    *crypto.Box
	S3     storage.Storage
	Signer *jwt.Signer
	Events events.Publisher
	// InternalURL is the internal API that sessions reach the proxy at.
	InternalURL string
	// PublicAPIURL: the OAuth callback of personal connections.
	PublicAPIURL, WebURL string
	// CheckMCP lists the tools of a server through the operator.
	CheckMCP func(ctx context.Context, req agent.MCPCheckRequest) (*agent.MCPCheckResponse, error)
	// GitToken reads private skill repositories (optional).
	GitToken string
}

var nameRe = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

const itemCols = `id, type, name, COALESCE(title,''), COALESCE(description,''), source, mode, personal_auth, personal_auth_secret_enc IS NOT NULL,
	platform_auth_enc, read_only, exposure, published, in_development, status, status_reason, tools, skills, skills_snapshot, synced_at, created_at`

func (s *Service) scan(row pgx.Row) (*Item, error) {
	var it Item
	var src, pa, tools, skills []byte
	var hasSecret bool
	var platEnc []byte
	if err := row.Scan(&it.ID, &it.Type, &it.Name, &it.Title, &it.Description, &src, &it.Mode, &pa, &hasSecret, &platEnc, &it.ReadOnly,
		&it.Exposure, &it.Published, &it.InDevelopment, &it.Status, &it.StatusReason, &tools, &skills, &it.snapshot, &it.SyncedAt, &it.CreatedAt); err != nil {
		return nil, err
	}
	_ = json.Unmarshal(src, &it.Source)
	if len(pa) > 0 {
		it.PersonalAuth = &PersonalAuth{}
		_ = json.Unmarshal(pa, it.PersonalAuth)
		it.PersonalAuth.HasSecret = hasSecret
		it.PersonalAuth.ClientSecret = ""
	}
	if len(tools) > 0 {
		_ = json.Unmarshal(tools, &it.Tools)
	}
	if len(skills) > 0 {
		_ = json.Unmarshal(skills, &it.Skills)
	}
	if it.Tools == nil {
		it.Tools = []agent.MCPTool{}
	}
	if it.Skills == nil {
		it.Skills = []string{}
	}
	it.PlatformAuth = map[string]string{}
	if len(platEnc) > 0 {
		if h, err := s.platformHeaders(platEnc); err == nil {
			for k, v := range h {
				it.PlatformAuth[k] = "…" + last4(v)
			}
		}
	}
	return &it, nil
}

func last4(s string) string {
	if len(s) <= 4 {
		return s
	}
	return s[len(s)-4:]
}

func (s *Service) platformHeaders(enc []byte) (map[string]string, error) {
	if len(enc) == 0 {
		return map[string]string{}, nil
	}
	pt, err := s.Box.Open(enc)
	if err != nil {
		return nil, err
	}
	h := map[string]string{}
	return h, json.Unmarshal([]byte(pt), &h)
}

// List lists all items (administration).
func (s *Service) List(ctx context.Context) ([]Item, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+itemCols+` FROM catalog_items ORDER BY type, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Item{}
	for rows.Next() {
		it, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *it)
	}
	return out, rows.Err()
}

// Get loads an item.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (*Item, error) {
	it, err := s.scan(s.Pool.QueryRow(ctx, `SELECT `+itemCols+` FROM catalog_items WHERE id = $1`, id))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "catalog item not found")
	}
	return it, err
}

// ByName loads an item by name or nil.
func (s *Service) ByName(ctx context.Context, name string) (*Item, error) {
	it, err := s.scan(s.Pool.QueryRow(ctx, `SELECT `+itemCols+` FROM catalog_items WHERE name = $1`, name))
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	return it, err
}

// Input is POST /catalog and PATCH /catalog/{id} (absent fields keep values).
type Input struct {
	Type          string             `json:"type"`
	Name          string             `json:"name"`
	Title         *string            `json:"title"`
	Description   *string            `json:"description"`
	Source        *Source            `json:"source"`
	Mode          *string            `json:"mode"`
	PersonalAuth  *PersonalAuth      `json:"personalAuth"`
	PlatformAuth  *map[string]string `json:"platformAuth"` // header → value; "" keeps the stored value
	ReadOnly      *bool              `json:"readOnly"`
	Exposure      *string            `json:"exposure"`
	Published     *bool              `json:"published"`
	InDevelopment *bool              `json:"inDevelopment"`
}

func validURL(u string) bool {
	p, err := url.Parse(u)
	return err == nil && (p.Scheme == "https" || p.Scheme == "http") && p.Host != ""
}

func (s *Service) validate(it *Item) error {
	switch it.Type {
	case "mcp":
		if it.Source.Kind != "url" || !validURL(it.Source.URL) {
			return apperr.Unprocessable("invalid_source", "an MCP server needs an http(s) URL").With("field", "source")
		}
		if it.Mode == nil || (*it.Mode != "personal" && *it.Mode != "platform") {
			return apperr.Unprocessable("invalid_mode", "the mode is personal or platform").With("field", "mode")
		}
		if *it.Mode == "personal" {
			pa := it.PersonalAuth
			if pa == nil {
				return apperr.Unprocessable("invalid_auth", "personal access needs personalAuth").With("field", "personalAuth")
			}
			switch pa.Kind {
			case "token", "delegation":
			case "oauth":
				if !validURL(pa.AuthorizeURL) || !validURL(pa.TokenURL) || pa.ClientID == "" {
					return apperr.Unprocessable("invalid_auth", "OAuth needs authorizeUrl, tokenUrl and clientId").With("field", "personalAuth")
				}
			default:
				return apperr.Unprocessable("invalid_auth", "personalAuth.kind is oauth, token or delegation").With("field", "personalAuth")
			}
		}
		if it.Exposure != "direct" && it.Exposure != "deferred" {
			return apperr.Unprocessable("invalid_exposure", "exposure is direct or deferred").With("field", "exposure")
		}
	case "skill":
		switch it.Source.Kind {
		case "upload":
		case "git":
			if it.Source.Repo == "" {
				return apperr.Unprocessable("invalid_source", "a git source needs repo").With("field", "source")
			}
		default:
			return apperr.Unprocessable("invalid_source", "a skill source is upload or git").With("field", "source")
		}
		it.Mode = nil
	default:
		return apperr.Unprocessable("invalid_type", "type is mcp or skill").With("field", "type")
	}
	if !nameRe.MatchString(it.Name) || len(it.Name) > 64 || it.Name == "nabu" {
		return apperr.Unprocessable("invalid_name", "the name is lower-case words with dashes").With("field", "name")
	}
	return nil
}

// Save creates (id nil) or updates an item.
func (s *Service) Save(ctx context.Context, id *uuid.UUID, in Input) (*Item, error) {
	var it *Item
	var platEnc []byte
	var paSecret []byte
	if id == nil {
		it = &Item{Type: in.Type, Name: strings.TrimSpace(in.Name), ReadOnly: true, Exposure: "deferred"}
	} else {
		var err error
		if it, err = s.Get(ctx, *id); err != nil {
			return nil, err
		}
		_ = s.Pool.QueryRow(ctx, `SELECT platform_auth_enc, personal_auth_secret_enc FROM catalog_items WHERE id = $1`, *id).Scan(&platEnc, &paSecret)
		if in.Name != "" && in.Name != it.Name {
			return nil, apperr.Unprocessable("invalid_name", "the name cannot change").With("field", "name")
		}
	}
	if in.Title != nil {
		it.Title = strings.TrimSpace(*in.Title)
	}
	if in.Description != nil {
		it.Description = strings.TrimSpace(*in.Description)
	}
	if in.Source != nil {
		it.Source = *in.Source
	}
	if in.Mode != nil {
		m := *in.Mode
		it.Mode = &m
	}
	if in.PersonalAuth != nil {
		pa := *in.PersonalAuth
		if pa.ClientSecret != "" {
			enc, err := s.Box.Seal(pa.ClientSecret)
			if err != nil {
				return nil, err
			}
			paSecret = enc
		}
		pa.ClientSecret, pa.HasSecret = "", false
		it.PersonalAuth = &pa
	}
	if in.ReadOnly != nil {
		it.ReadOnly = *in.ReadOnly
	}
	if in.Exposure != nil {
		it.Exposure = *in.Exposure
	}
	if in.Published != nil {
		it.Published = *in.Published
	}
	if in.InDevelopment != nil {
		it.InDevelopment = *in.InDevelopment
	}
	if in.PlatformAuth != nil {
		old, _ := s.platformHeaders(platEnc)
		next := map[string]string{}
		for k, v := range *in.PlatformAuth {
			k = strings.TrimSpace(k)
			if k == "" {
				continue
			}
			if v == "" {
				v = old[k]
			}
			next[k] = v
		}
		b, _ := json.Marshal(next)
		enc, err := s.Box.Seal(string(b))
		if err != nil {
			return nil, err
		}
		platEnc = enc
		if len(next) == 0 {
			platEnc = nil
		}
	}
	if it.Exposure == "" {
		it.Exposure = "deferred"
	}
	if err := s.validate(it); err != nil {
		return nil, err
	}
	src, _ := json.Marshal(it.Source)
	var pa []byte
	if it.PersonalAuth != nil && it.Mode != nil && *it.Mode == "personal" {
		pa, _ = json.Marshal(it.PersonalAuth)
	}
	var err error
	if id == nil {
		var nid uuid.UUID
		err = s.Pool.QueryRow(ctx, `INSERT INTO catalog_items (type, name, title, description, source, mode, personal_auth, personal_auth_secret_enc,
			platform_auth_enc, read_only, exposure, published, in_development) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING id`,
			it.Type, it.Name, it.Title, it.Description, src, it.Mode, pa, paSecret, platEnc, it.ReadOnly, it.Exposure, it.Published, it.InDevelopment).Scan(&nid)
		if postgres.IsUniqueViolation(err) {
			return nil, apperr.Conflict("name_taken", "an item with this name exists").With("field", "name")
		}
		id = &nid
	} else {
		_, err = s.Pool.Exec(ctx, `UPDATE catalog_items SET title = $2, description = $3, source = $4, mode = $5, personal_auth = $6,
			personal_auth_secret_enc = $7, platform_auth_enc = $8, read_only = $9, exposure = $10, published = $11, in_development = $12, updated_at = now()
			WHERE id = $1`, *id, it.Title, it.Description, src, it.Mode, pa, paSecret, platEnc, it.ReadOnly, it.Exposure, it.Published, it.InDevelopment)
	}
	if err != nil {
		return nil, err
	}
	s.changed(ctx)
	return s.Get(ctx, *id)
}

// Delete removes an item and the connections of users.
func (s *Service) Delete(ctx context.Context, id uuid.UUID) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM catalog_items WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return apperr.NotFound("not_found", "catalog item not found")
	}
	s.changed(ctx)
	return nil
}

func (s *Service) changed(ctx context.Context) {
	if s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: events.ConnectionsChanged, Data: map[string]any{}})
	}
}

// Check lists the tools of an MCP item through the operator with platform
// credentials (or none) and stores them. A delegation item is checked on
// behalf of the administrator (email): the product refuses calls without a
// Nabu JWT.
func (s *Service) Check(ctx context.Context, id uuid.UUID, email string) (*Item, error) {
	it, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if it.Type != "mcp" {
		return nil, apperr.Unprocessable("not_mcp", "only MCP servers are checked")
	}
	if s.CheckMCP == nil {
		return nil, apperr.Unavailable("agent_unavailable", "the agent operator is not reachable")
	}
	var platEnc []byte
	_ = s.Pool.QueryRow(ctx, `SELECT platform_auth_enc FROM catalog_items WHERE id = $1`, id).Scan(&platEnc)
	headers, err := s.platformHeaders(platEnc)
	if err != nil {
		return nil, err
	}
	if (it.Mode == nil || *it.Mode != "platform") && it.PersonalAuth != nil && it.PersonalAuth.Kind == "delegation" && email != "" {
		headers = s.delegationHeaders(it, email)
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	res, err := s.CheckMCP(ctx, agent.MCPCheckRequest{Server: agent.MCPServer{Name: it.Name, URL: it.Source.URL, HeaderNames: names, Exposure: "direct"}, Headers: headers})
	if err != nil {
		return nil, apperr.Unavailable("agent_unavailable", err.Error())
	}
	status, reason := "ok", ""
	if !res.OK {
		status, reason = "error", res.Error
	}
	tools, _ := json.Marshal(res.Tools)
	_, err = s.Pool.Exec(ctx, `UPDATE catalog_items SET status = $2, status_reason = NULLIF($3,''), tools = CASE WHEN $2 = 'ok' THEN $4 ELSE tools END, updated_at = now() WHERE id = $1`,
		id, status, reason, tools)
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, id)
}

// ─── user view (tech §3.5) ──────────────────────────────────────────

// UserItem is a published item as a user sees it.
type UserItem struct {
	ID            uuid.UUID  `json:"id"`
	Type          string     `json:"type"`
	Name          string     `json:"name"`
	Title         string     `json:"title"`
	Description   string     `json:"description"`
	Mode          *string    `json:"mode"`
	AuthKind      string     `json:"authKind,omitempty"`
	ReadOnly      bool       `json:"readOnly"`
	Status        string     `json:"status"` // available | in_development | unavailable
	Connected     bool       `json:"connected"`
	ConnectedAt   *time.Time `json:"connectedAt"`
	InDevelopment bool       `json:"inDevelopment"`
}

// ForUser lists published items with the user's state (CAT-08: unpublished items disappear).
func (s *Service) ForUser(ctx context.Context, uid uuid.UUID) ([]UserItem, error) {
	rows, err := s.Pool.Query(ctx, `SELECT c.id, c.type, c.name, COALESCE(c.title,''), COALESCE(c.description,''), c.mode,
		COALESCE(c.personal_auth->>'kind',''), c.read_only, c.in_development, c.status, u.connected_at
		FROM catalog_items c LEFT JOIN user_connections u ON u.item_id = c.id AND u.user_id = $1
		WHERE c.published ORDER BY c.type, COALESCE(c.title, c.name)`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UserItem{}
	for rows.Next() {
		var u UserItem
		var status *string
		if err := rows.Scan(&u.ID, &u.Type, &u.Name, &u.Title, &u.Description, &u.Mode, &u.AuthKind, &u.ReadOnly, &u.InDevelopment, &status, &u.ConnectedAt); err != nil {
			return nil, err
		}
		u.Connected = u.ConnectedAt != nil
		u.Status = "available"
		if u.InDevelopment {
			u.Status = "in_development"
		} else if status != nil && *status == "error" {
			u.Status = "unavailable"
		}
		if u.Title == "" {
			u.Title = u.Name
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ErrUnavailable is catalog_item_unavailable (CAT-08).
func ErrUnavailable() error {
	return apperr.Conflict("catalog_item_unavailable", "the catalog item is unpublished or not configured")
}

// ConnectResult is the answer of POST /catalog/{id}/connect.
type ConnectResult struct {
	Connected    bool   `json:"connected"`
	AuthorizeURL string `json:"authorizeUrl,omitempty"`
}

// Connect connects an item for a user: OAuth returns the authorize URL; a
// personal token is stored encrypted; platform items and skills are enabled.
func (s *Service) Connect(ctx context.Context, uid, id uuid.UUID, token string) (*ConnectResult, error) {
	it, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if !it.Published || it.InDevelopment {
		return nil, ErrUnavailable()
	}
	if it.Type == "mcp" && it.Mode != nil && *it.Mode == "personal" {
		switch it.PersonalAuth.Kind {
		case "oauth":
			return s.startOAuth(ctx, uid, it)
		case "token":
			token = strings.TrimSpace(token)
			if len(token) < 4 {
				return nil, apperr.Unprocessable("token_required", "a personal token is required").With("field", "token")
			}
			enc, err := s.Box.Seal(token)
			if err != nil {
				return nil, err
			}
			if err := s.saveConnection(ctx, uid, id, enc, nil, nil); err != nil {
				return nil, err
			}
			return &ConnectResult{Connected: true}, nil
		}
	}
	if err := s.saveConnection(ctx, uid, id, nil, nil, nil); err != nil {
		return nil, err
	}
	return &ConnectResult{Connected: true}, nil
}

func (s *Service) saveConnection(ctx context.Context, uid, id uuid.UUID, cred, refresh []byte, exp *time.Time) error {
	_, err := s.Pool.Exec(ctx, `INSERT INTO user_connections (user_id, item_id, credentials_enc, refresh_enc, expires_at) VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (user_id, item_id) DO UPDATE SET credentials_enc = EXCLUDED.credentials_enc, refresh_enc = EXCLUDED.refresh_enc,
		expires_at = EXCLUDED.expires_at, connected_at = now()`, uid, id, cred, refresh, exp)
	if err == nil && s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: events.ConnectionsChanged, UserID: &uid, Data: map[string]any{"itemId": id}})
	}
	return err
}

// Disconnect revokes a personal access or disables an item (CAT-03).
func (s *Service) Disconnect(ctx context.Context, uid, id uuid.UUID) error {
	if _, err := s.Pool.Exec(ctx, `DELETE FROM user_connections WHERE user_id = $1 AND item_id = $2`, uid, id); err != nil {
		return err
	}
	if s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: events.ConnectionsChanged, UserID: &uid, Data: map[string]any{"itemId": id}})
	}
	return nil
}

// ─── sessions ───────────────────────────────────────────────────────

// SessionMCP lists the MCP servers of a user's session: the connected
// published items reached through the proxy, and the skills to load.
type SessionMCP struct {
	Servers []agent.MCPServer
	Headers map[string]map[string]string // proxy token per server
	Skills  []string
	// SkillSnapshots maps a skill to its snapshot hash.
	SkillSnapshots map[string]string
}

// ProxyToken is the token a session presents to the MCP proxy for one item.
func (s *Service) ProxyToken(uid uuid.UUID, item uuid.UUID, ch, conv string, ttl time.Duration) string {
	return s.Signer.Issue(jwt.Claims{Audience: jwt.AudMCP, Subject: "proxy:" + item.String(), User: uid.String(), Channel: ch, Conversation: conv}, ttl)
}

// ForSession resolves the user's connected items (CAT-01: connections take
// effect in the next session).
func (s *Service) ForSession(ctx context.Context, uid uuid.UUID, ch, conv string, ttl time.Duration) (*SessionMCP, error) {
	rows, err := s.Pool.Query(ctx, `SELECT c.id, c.type, c.name, COALESCE(c.title,''), COALESCE(c.description,''), c.exposure, c.skills, c.skills_snapshot
		FROM catalog_items c JOIN user_connections u ON u.item_id = c.id AND u.user_id = $1
		WHERE c.published AND NOT c.in_development ORDER BY c.name`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := &SessionMCP{Headers: map[string]map[string]string{}, SkillSnapshots: map[string]string{}}
	for rows.Next() {
		var id uuid.UUID
		var typ, name, title, desc, exposure string
		var skills []byte
		var snap *string
		if err := rows.Scan(&id, &typ, &name, &title, &desc, &exposure, &skills, &snap); err != nil {
			return nil, err
		}
		if typ == "mcp" {
			d := title
			if desc != "" {
				d = strings.TrimSpace(title + ": " + desc)
			}
			out.Servers = append(out.Servers, agent.MCPServer{Name: name, URL: s.InternalURL + "/internal/v1/mcp-proxy/" + id.String(),
				HeaderNames: []string{"Authorization"}, Exposure: exposure, Description: d})
			out.Headers[name] = map[string]string{"Authorization": "Bearer " + s.ProxyToken(uid, id, ch, conv, ttl)}
			continue
		}
		var names []string
		_ = json.Unmarshal(skills, &names)
		for _, n := range names {
			out.Skills = append(out.Skills, n)
			if snap != nil {
				out.SkillSnapshots[n] = *snap
			}
		}
	}
	return out, rows.Err()
}

// ForServiceAgent resolves platform MCP items and skills by name for a
// service agent (R15: only platform items).
func (s *Service) ForServiceAgent(ctx context.Context, mcpNames, skillNames []string, runID string, ttl time.Duration) (*SessionMCP, error) {
	out := &SessionMCP{Headers: map[string]map[string]string{}, SkillSnapshots: map[string]string{}}
	for _, n := range mcpNames {
		it, err := s.ByName(ctx, n)
		if err != nil {
			return nil, err
		}
		if it == nil || it.Type != "mcp" || it.Mode == nil || *it.Mode != "platform" || !it.Published {
			continue
		}
		tok := s.Signer.Issue(jwt.Claims{Audience: jwt.AudMCP, Subject: "proxy:" + it.ID.String(), Run: runID, Channel: "run"}, ttl)
		out.Servers = append(out.Servers, agent.MCPServer{Name: it.Name, URL: s.InternalURL + "/internal/v1/mcp-proxy/" + it.ID.String(),
			HeaderNames: []string{"Authorization"}, Exposure: it.Exposure, Description: it.Title})
		out.Headers[it.Name] = map[string]string{"Authorization": "Bearer " + tok}
	}
	if len(skillNames) == 0 {
		return out, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT skills, skills_snapshot FROM catalog_items WHERE type = 'skill' AND skills_snapshot IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	want := map[string]bool{}
	for _, n := range skillNames {
		want[n] = true
	}
	for rows.Next() {
		var skills []byte
		var snap string
		if err := rows.Scan(&skills, &snap); err != nil {
			return nil, err
		}
		var names []string
		_ = json.Unmarshal(skills, &names)
		for _, n := range names {
			if want[n] {
				out.Skills = append(out.Skills, n)
				out.SkillSnapshots[n] = snap
			}
		}
	}
	return out, rows.Err()
}

// Bundle reads a skills snapshot from S3.
func (s *Service) Bundle(ctx context.Context, hash string) ([]byte, error) {
	return storage.ReadAll(ctx, s.S3, "skills/"+strings.TrimPrefix(hash, "sha256:")+".tar.gz")
}

// errNoCredential: a personal item the user connected has no usable credential.
var errNoCredential = errors.New("no credential")

// StarterCatalog seeds the starter catalog of R13 once, unpublished and
// unconfigured: the administrator fills in the addresses and credentials and
// publishes (CAT-05). Hammurapi is not seeded: it appears only when an
// administrator adds it (R26b, R32a).
func (s *Service) StarterCatalog(ctx context.Context) error {
	var done bool
	_ = s.Pool.QueryRow(ctx, `SELECT true FROM settings WHERE key = 'starter_catalog'`).Scan(&done)
	if done {
		return nil
	}
	type seed struct {
		name, title, mode, auth string
		readOnly, dev           bool
	}
	seeds := []seed{
		{"confluence", "Confluence", "personal", "oauth", false, false},
		{"jira", "Jira", "personal", "oauth", false, false},
		{"figma", "Figma", "personal", "oauth", false, false},
		{"vk-workspace", "VK WorkSpace", "personal", "token", false, true},
		{"grafana", "Grafana", "platform", "", true, false},
		{"victoriametrics", "VictoriaMetrics", "platform", "", true, false},
		{"victorialogs", "VictoriaLogs", "platform", "", true, false},
		{"backstage", "Backstage", "platform", "", true, false},
		{"kubernetes", "Kubernetes", "platform", "", true, false},
		{"postgresql", "PostgreSQL", "platform", "", true, false},
		{"clickhouse", "ClickHouse", "platform", "", true, false},
		{"greenplum", "Greenplum", "platform", "", true, false},
		{"web-search", "Web search", "platform", "", false, false},
	}
	for _, sd := range seeds {
		src, _ := json.Marshal(Source{Kind: "url", URL: "https://mcp.example.invalid/" + sd.name})
		var pa []byte
		if sd.mode == "personal" {
			pa, _ = json.Marshal(PersonalAuth{Kind: sd.auth})
		}
		if _, err := s.Pool.Exec(ctx, `INSERT INTO catalog_items (type, name, title, source, mode, personal_auth, read_only, published, in_development, status, status_reason)
			VALUES ('mcp',$1,$2,$3,$4,$5,$6,false,$7,'unconfigured','set the address and credentials, then publish') ON CONFLICT (name) DO NOTHING`,
			sd.name, sd.title, src, sd.mode, pa, sd.readOnly, sd.dev); err != nil {
			return fmt.Errorf("starter catalog %s: %w", sd.name, err)
		}
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO settings (key, value) VALUES ('starter_catalog', 'true') ON CONFLICT DO NOTHING`)
	return err
}
