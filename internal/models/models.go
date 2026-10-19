// Package models is the model access layer of Nabu (FTR.NAB.CMN-0001 R19–R21,
// tech §4; FTR.NAB.CHAT-0001 R7–R15): connections to providers — DeepSeek,
// GigaChat, Z.AI or any OpenAI-compatible endpoint such as LiteLLM — the
// models of a connection with their capabilities and the sources of them, the
// default and available models of personal agents, and the resolution of a
// model choice into what a harness session needs.
package models

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/access"
	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/models/gigachat"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Connection types (R15).
const (
	TypeDeepSeek         = "deepseek"
	TypeGigaChat         = "gigachat"
	TypeZAI              = "zai"
	TypeOpenAICompatible = "openai_compatible"
)

// Base addresses of the presets.
const (
	DeepSeekURL  = "https://api.deepseek.com"
	ZAIURL       = "https://api.z.ai/api/paas/v4"
	ZAICodingURL = "https://api.z.ai/api/coding/paas/v4"
)

// DeepSeekModels are the models of the DeepSeek preset in the directory.
var DeepSeekModels = func() []agent.ModelDef {
	var out []agent.ModelDef
	for _, k := range ofType(TypeDeepSeek) {
		out = append(out, build(TypeDeepSeek, k.def.ID, nil, nil))
	}
	return out
}()

// NewModel is a model the provider lists and the connection does not have
// yet (R10): it is marked, never added by itself.
type NewModel struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	FirstSeenAt time.Time `json:"firstSeenAt"`
}

// Connection is a model connection as the API shows it (never the key).
type Connection struct {
	ID           uuid.UUID         `json:"id"`
	Name         string            `json:"name"`
	Type         string            `json:"type"`
	BaseURL      string            `json:"baseUrl"`
	Options      map[string]string `json:"options"`
	Models       []agent.ModelDef  `json:"models"`
	Discovered   []NewModel        `json:"discovered"`
	NewCount     int               `json:"newCount"`
	KeyLast4     string            `json:"keyLast4"`
	Enabled      bool              `json:"enabled"`
	Status       string            `json:"status"`
	StatusReason *string           `json:"statusReason"`
	CheckedAt    *time.Time        `json:"checkedAt"`
	RefreshedAt  *time.Time        `json:"refreshedAt"`
	CreatedAt    time.Time         `json:"createdAt"`
	api          string
}

// Choice is a model of a connection with a reasoning level.
type Choice struct {
	ConnectionID uuid.UUID `json:"connectionId"`
	Model        string    `json:"model"`
	Thinking     string    `json:"thinking,omitempty"`
}

// PersonalModels are the default and the selectable models of personal agents.
type PersonalModels struct {
	Default   *Choice  `json:"default"`
	Available []Choice `json:"available"`
}

// Service manages connections and resolves models.
type Service struct {
	Pool *pgxpool.Pool
	Box  *crypto.Box
	// Check runs a check through the agent operator (nil: checks unavailable).
	Check func(ctx context.Context, req agent.LLMCheckRequest) (*agent.LLMCheckResponse, error)
	// Embed asks a model for one vector (nil: models of embeddings are not
	// recognized by a check).
	Embed func(ctx context.Context, baseURL, key, model string) error
	// GigaChat is the client of GigaChat; Signer and InternalURL make the
	// address and the token of the bridge Pi talks to (R13). Without them
	// GigaChat connections cannot run.
	GigaChat    *gigachat.Client
	Signer      *jwt.Signer
	InternalURL string
	// HTTP lists the models of providers (nil: a client with a 30 s timeout).
	HTTP *http.Client
	// AutoRefresh asks the provider for the description of the models in the
	// background after a connection is created (R7).
	AutoRefresh bool
	// Access is the access of a user by groups (FTR.NAB.CMN-0007); nil keeps
	// the global list of available models.
	Access func(ctx context.Context, uid uuid.UUID) (*access.Access, error)
}

const connSelect = `SELECT id, name, type, base_url, api, models, options, discovered, COALESCE(key_last4,''), enabled, status, status_reason,
	checked_at, refreshed_at, created_at FROM model_connections`

func scanConn(row pgx.Row) (*Connection, error) {
	var c Connection
	var models, opts, disc []byte
	if err := row.Scan(&c.ID, &c.Name, &c.Type, &c.BaseURL, &c.api, &models, &opts, &disc, &c.KeyLast4, &c.Enabled, &c.Status, &c.StatusReason,
		&c.CheckedAt, &c.RefreshedAt, &c.CreatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(models, &c.Models); err != nil {
		return nil, err
	}
	_ = json.Unmarshal(opts, &c.Options)
	_ = json.Unmarshal(disc, &c.Discovered)
	if c.Options == nil {
		c.Options = map[string]string{}
	}
	if c.Discovered == nil {
		c.Discovered = []NewModel{}
	}
	c.NewCount = len(c.Discovered)
	if !c.Enabled {
		c.Status = "disabled"
	}
	return &c, nil
}

// List lists the connections.
func (s *Service) List(ctx context.Context) ([]Connection, error) {
	rows, err := s.Pool.Query(ctx, connSelect+` ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Connection{}
	for rows.Next() {
		c, err := scanConn(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

// Get loads a connection or nil.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (*Connection, error) {
	c, err := scanConn(s.Pool.QueryRow(ctx, connSelect+` WHERE id = $1`, id))
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	return c, err
}

// Input is the body of POST /model-connections, of a check of a draft and of
// a discovery.
type Input struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	BaseURL string            `json:"baseUrl"`
	APIKey  string            `json:"apiKey"`
	Options map[string]string `json:"options"`
	Models  []ModelRef        `json:"models"`
	Defs    []agent.ModelDef  `json:"modelDefs,omitempty"` // full definitions (import)
	// ImportRef makes a repeated import update the same connection.
	ImportRef string `json:"-"`
}

// Refs makes model references of ids.
func Refs(ids ...string) []ModelRef {
	out := make([]ModelRef, 0, len(ids))
	for _, id := range ids {
		out = append(out, ModelRef{ID: id})
	}
	return out
}

// normalize validates the connection fields of an input (without the models).
func (in *Input) normalize() error {
	in.Name = strings.TrimSpace(in.Name)
	in.APIKey = strings.TrimSpace(in.APIKey)
	if in.Options == nil {
		in.Options = map[string]string{}
	}
	switch in.Type {
	case TypeDeepSeek:
		if in.BaseURL == "" {
			in.BaseURL = DeepSeekURL
		}
		if in.Name == "" {
			in.Name = "DeepSeek"
		}
		in.Options = map[string]string{}
	case TypeGigaChat:
		if in.BaseURL == "" {
			in.BaseURL = gigachat.DefaultAPIURL
		}
		if in.Name == "" {
			in.Name = "GigaChat"
		}
		scope := in.Options["scope"]
		if scope == "" {
			scope = gigachat.DefaultScope
		}
		if !gigachat.ValidScope(scope) {
			return apperr.Unprocessable("invalid_scope", "unknown scope of GigaChat").With("field", "scope")
		}
		opts := map[string]string{"scope": scope}
		if a := strings.TrimSpace(in.Options["authUrl"]); a != "" {
			if u, err := url.Parse(a); err != nil || u.Scheme != "https" {
				return apperr.Unprocessable("invalid_url", "the OAuth address must be https").With("field", "authUrl")
			}
			opts["authUrl"] = a
		}
		in.Options = opts
	case TypeZAI:
		if in.Name == "" {
			in.Name = "Z.AI"
		}
		ep := in.Options["endpoint"]
		if ep == "" {
			ep = "general"
		}
		switch ep {
		case "general":
			in.BaseURL = ZAIURL
		case "coding":
			in.BaseURL = ZAICodingURL
		default:
			return apperr.Unprocessable("invalid_endpoint", "the endpoint is general or coding").With("field", "endpoint")
		}
		in.Options = map[string]string{"endpoint": ep}
	case TypeOpenAICompatible:
		if in.Name == "" {
			in.Name = "LiteLLM"
		}
		in.Options = map[string]string{}
	default:
		return apperr.Unprocessable("invalid_type", "unknown connection type").With("field", "type")
	}
	u, err := url.Parse(in.BaseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return apperr.Unprocessable("invalid_url", "the base URL must be http(s)").With("field", "baseUrl")
	}
	in.BaseURL = strings.TrimRight(in.BaseURL, "/")
	if len(in.Name) > 80 {
		return apperr.Unprocessable("invalid_name", "the name is too long").With("field", "name")
	}
	return nil
}

// Normalize validates the input and builds the model definitions from the
// directory and the edits (no network: the provider's description comes with
// a refresh).
func (in *Input) Normalize() ([]agent.ModelDef, error) {
	if err := in.normalize(); err != nil {
		return nil, err
	}
	if len(in.Defs) > 0 {
		for i := range in.Defs {
			if in.Defs[i].Sources == nil {
				in.Defs[i].Tools = true // an imported model worked as an agent
				in.Defs[i].Sources = map[string]string{}
			}
		}
		return in.Defs, nil
	}
	refs := in.Models
	if len(refs) == 0 && in.Type != TypeOpenAICompatible {
		for _, k := range ofType(in.Type) {
			if !k.def.Embeddings {
				refs = append(refs, ModelRef{ID: k.def.ID})
			}
		}
	}
	return describe(in.Type, refs, nil)
}

// describe builds the models of refs, keeping what old says of them.
func describe(typ string, refs []ModelRef, old []agent.ModelDef) ([]agent.ModelDef, error) {
	var defs []agent.ModelDef
	seen := map[string]bool{}
	for _, r := range refs {
		id := normalizeID(r.ID)
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		var d agent.ModelDef
		if i := slices.IndexFunc(old, func(m agent.ModelDef) bool { return m.ID == id }); i >= 0 {
			d = old[i]
		} else {
			d = build(typ, id, nil, nil)
		}
		defs = append(defs, edit(typ, d, r.Overrides))
	}
	if len(defs) == 0 {
		return nil, apperr.Unprocessable("invalid_models", "at least one model is required").With("field", "models")
	}
	return defs, nil
}

func last4(key string) string {
	if len(key) <= 4 {
		return key
	}
	return key[len(key)-4:]
}

// Create creates a connection (or, with ImportRef, updates the imported one).
func (s *Service) Create(ctx context.Context, in Input) (*Connection, error) {
	defs, err := in.Normalize()
	if err != nil {
		return nil, err
	}
	if len(in.APIKey) < 8 {
		return nil, apperr.Unprocessable("invalid_key", "the API key is required").With("field", "apiKey")
	}
	enc, err := s.Box.Seal(in.APIKey)
	if err != nil {
		return nil, err
	}
	b, _ := json.Marshal(defs)
	ob, _ := json.Marshal(in.Options)
	var ref *string
	if in.ImportRef != "" {
		ref = &in.ImportRef
	}
	var id uuid.UUID
	err = s.Pool.QueryRow(ctx, `INSERT INTO model_connections (name, type, base_url, api_key_enc, key_last4, models, options, import_ref)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (import_ref) DO UPDATE SET base_url = EXCLUDED.base_url, api_key_enc = EXCLUDED.api_key_enc,
			key_last4 = EXCLUDED.key_last4, models = EXCLUDED.models, updated_at = now()
		RETURNING id`, in.Name, in.Type, in.BaseURL, enc, last4(in.APIKey), b, ob, ref).Scan(&id)
	if postgres.IsUniqueViolation(err) {
		return nil, apperr.Conflict("name_taken", "a connection with this name exists").With("field", "name")
	}
	if err != nil {
		return nil, err
	}
	if s.AutoRefresh {
		go func() {
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			defer cancel()
			if _, err := s.Refresh(rctx, id); err != nil {
				slog.Warn("refresh the models of a new connection", "connection", id, "err", err)
			}
		}()
	}
	return s.Get(ctx, id)
}

// Patch is PATCH /model-connections/{id}.
type Patch struct {
	Name    *string     `json:"name"`
	Enabled *bool       `json:"enabled"`
	Models  *[]ModelRef `json:"models"`
}

// Update changes name, state or models (with edits of their capabilities).
func (s *Service) Update(ctx context.Context, id uuid.UUID, p Patch) (*Connection, error) {
	c, err := s.Get(ctx, id)
	if err != nil || c == nil {
		return nil, notFound(err)
	}
	var models, disc []byte
	if p.Models != nil {
		defs, err := describe(c.Type, *p.Models, c.Models)
		if err != nil {
			return nil, err
		}
		models, _ = json.Marshal(defs)
		// an added new model is no longer new
		left := slices.DeleteFunc(slices.Clone(c.Discovered), func(n NewModel) bool {
			return slices.ContainsFunc(defs, func(d agent.ModelDef) bool { return d.ID == n.ID })
		})
		disc, _ = json.Marshal(left)
	}
	if p.Name != nil && strings.TrimSpace(*p.Name) == "" {
		return nil, apperr.Unprocessable("invalid_name", "the name is required").With("field", "name")
	}
	_, err = s.Pool.Exec(ctx, `UPDATE model_connections SET name = COALESCE($2, name), enabled = COALESCE($3, enabled),
		models = COALESCE($4, models), discovered = COALESCE($5, discovered), updated_at = now() WHERE id = $1`, id, p.Name, p.Enabled, models, disc)
	if postgres.IsUniqueViolation(err) {
		return nil, apperr.Conflict("name_taken", "a connection with this name exists").With("field", "name")
	}
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, id)
}

// ReplaceKey stores a new key.
func (s *Service) ReplaceKey(ctx context.Context, id uuid.UUID, key string) (*Connection, error) {
	key = strings.TrimSpace(key)
	if len(key) < 8 {
		return nil, apperr.Unprocessable("invalid_key", "the API key is required").With("field", "apiKey")
	}
	enc, err := s.Box.Seal(key)
	if err != nil {
		return nil, err
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE model_connections SET api_key_enc = $2, key_last4 = $3, status = 'unchecked', status_reason = NULL, updated_at = now() WHERE id = $1`,
		id, enc, last4(key))
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, notFound(nil)
	}
	return s.Get(ctx, id)
}

// Delete removes a connection that no service agent uses.
func (s *Service) Delete(ctx context.Context, id uuid.UUID) error {
	var n int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM service_agents WHERE config->'model'->>'connectionId' = $1::text`, id).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return apperr.Conflict("connection_in_use", "service agents use this connection").With("agents", n)
	}
	tag, err := s.Pool.Exec(ctx, `DELETE FROM model_connections WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return notFound(nil)
	}
	return nil
}

func notFound(err error) error {
	if err != nil {
		return err
	}
	return apperr.NotFound("not_found", "connection not found")
}

func (s *Service) key(ctx context.Context, id uuid.UUID) (string, error) {
	var enc []byte
	if err := s.Pool.QueryRow(ctx, `SELECT api_key_enc FROM model_connections WHERE id = $1`, id).Scan(&enc); err != nil {
		return "", err
	}
	key, err := s.Box.Open(enc)
	if err != nil {
		return "", fmt.Errorf("decrypt the key of the connection %s: %w", id, err)
	}
	return key, nil
}

// bridgeTTL is the life of a bridge token: as long as an agent session lives.
const bridgeTTL = 24 * time.Hour

// endpoint is the address and the key Pi uses for a connection: GigaChat goes
// through the bridge of api with a token of the connection (R13, arch §3.5).
func (s *Service) endpoint(ctx context.Context, c *Connection) (string, string, error) {
	if c.Type == TypeGigaChat {
		if s.Signer == nil || s.InternalURL == "" {
			return "", "", apperr.Unavailable("gigachat_unavailable", "GigaChat is not available in this deployment")
		}
		tok := s.Signer.Issue(jwt.Claims{Audience: jwt.AudLLMBridge, Subject: c.ID.String()}, bridgeTTL)
		return s.InternalURL + gigachat.Prefix + "/" + c.ID.String() + "/v1", tok, nil
	}
	key, err := s.key(ctx, c.ID)
	return c.BaseURL, key, err
}

// VerifyBridge admits a bridge token of the connection (GC-07).
func (s *Service) VerifyBridge(token string, conn uuid.UUID) bool {
	if s.Signer == nil {
		return false
	}
	cl, err := s.Signer.Verify(token, jwt.AudLLMBridge)
	return err == nil && cl.Subject == conn.String()
}

// BridgeConn loads an enabled GigaChat connection with its key for the bridge.
func (s *Service) BridgeConn(ctx context.Context, id uuid.UUID) (*gigachat.Conn, error) {
	c, err := s.Get(ctx, id)
	if err != nil || c == nil || !c.Enabled || c.Type != TypeGigaChat {
		return nil, err
	}
	key, err := s.key(ctx, id)
	if err != nil {
		return nil, err
	}
	gc := gigachatConn(c, key)
	return &gc, nil
}

// spec builds the model spec of a connection with the key.
func (s *Service) spec(ctx context.Context, c *Connection, model, thinking string) (agent.ModelSpec, string, error) {
	var def *agent.ModelDef
	for i := range c.Models {
		if c.Models[i].ID == model {
			def = &c.Models[i]
		}
	}
	if def == nil {
		return agent.ModelSpec{}, "", apperr.Conflict("agent_not_configured", fmt.Sprintf("the model %s is not in the connection %s", model, c.Name))
	}
	base, key, err := s.endpoint(ctx, c)
	if err != nil {
		return agent.ModelSpec{}, "", err
	}
	if thinking == "" || !def.SupportsThinking(thinking) {
		thinking = "off"
	}
	return agent.ModelSpec{Provider: "nabu-" + strings.ReplaceAll(c.ID.String(), "-", "")[:12], ConnectionID: c.ID.String(),
		API: c.api, BaseURL: base, ModelID: model, Thinking: thinking, Models: c.Models}, key, nil
}

// ErrNotConfigured is agent_not_configured (MOD-06).
func ErrNotConfigured() error {
	return apperr.Conflict("agent_not_configured", "no model connection or default model; an administrator configures them in Administration → Model connections")
}

// Resolve returns the session model of a choice.
func (s *Service) Resolve(ctx context.Context, ch Choice) (agent.ModelSpec, string, error) {
	c, err := s.Get(ctx, ch.ConnectionID)
	if err != nil {
		return agent.ModelSpec{}, "", err
	}
	if c == nil || !c.Enabled {
		return agent.ModelSpec{}, "", ErrNotConfigured()
	}
	return s.spec(ctx, c, ch.Model, ch.Thinking)
}

// Def returns the description of a model of a connection.
func (s *Service) Def(ctx context.Context, conn uuid.UUID, model string) (*agent.ModelDef, error) {
	c, err := s.Get(ctx, conn)
	if err != nil || c == nil {
		return nil, err
	}
	for i := range c.Models {
		if c.Models[i].ID == model {
			return &c.Models[i], nil
		}
	}
	return nil, nil
}

// ResolvePersonal resolves the model of a personal agent: the user's choice
// when it is still available, otherwise the default (R19).
func (s *Service) ResolvePersonal(ctx context.Context, conn *uuid.UUID, model *string) (agent.ModelSpec, string, error) {
	pm, err := s.Personal(ctx)
	if err != nil {
		return agent.ModelSpec{}, "", err
	}
	if conn != nil && model != nil {
		for _, a := range pm.Available {
			if a.ConnectionID == *conn && a.Model == *model {
				th := ""
				if pm.Default != nil && pm.Default.ConnectionID == a.ConnectionID && pm.Default.Model == a.Model {
					th = pm.Default.Thinking
				}
				if spec, key, err := s.Resolve(ctx, Choice{ConnectionID: a.ConnectionID, Model: a.Model, Thinking: th}); err == nil {
					return spec, key, nil
				}
			}
		}
	}
	if pm.Default == nil {
		return agent.ModelSpec{}, "", ErrNotConfigured()
	}
	return s.Resolve(ctx, *pm.Default)
}

const personalKey = "personal_models"

// allGrants are the models granted to «all» (FTR.NAB.CMN-0007 R2): the
// available models of the global setting once groups are on.
func (s *Service) allGrants(ctx context.Context) ([]Choice, error) {
	rows, err := s.Pool.Query(ctx, `SELECT ag.connection_id, ag.model FROM access_grants ag JOIN user_groups g ON g.id = ag.group_id
		WHERE g.builtin = 'all' AND ag.kind = 'model' ORDER BY ag.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Choice{}
	for rows.Next() {
		var c Choice
		if err := rows.Scan(&c.ConnectionID, &c.Model); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// PersonalView is GET /personal-models: with groups the available models are
// those of «all» (OLD-03).
func (s *Service) PersonalView(ctx context.Context) (PersonalModels, error) {
	pm, err := s.Personal(ctx)
	if err != nil || s.Access == nil {
		return pm, err
	}
	pm.Available, err = s.allGrants(ctx)
	return pm, err
}

// Personal reads the personal model settings.
func (s *Service) Personal(ctx context.Context) (PersonalModels, error) {
	var pm PersonalModels
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT value FROM settings WHERE key = $1`, personalKey).Scan(&raw)
	if postgres.IsNoRows(err) {
		return PersonalModels{Available: []Choice{}}, nil
	}
	if err != nil {
		return pm, err
	}
	if err := json.Unmarshal(raw, &pm); err != nil {
		return pm, err
	}
	if pm.Available == nil {
		pm.Available = []Choice{}
	}
	return pm, nil
}

// PutPersonal validates and stores the personal model settings.
func (s *Service) PutPersonal(ctx context.Context, pm PersonalModels) (PersonalModels, error) {
	check := func(ch Choice) error {
		c, err := s.Get(ctx, ch.ConnectionID)
		if err != nil {
			return err
		}
		if c == nil {
			return apperr.Unprocessable("invalid_model", "unknown connection")
		}
		for _, m := range c.Models {
			if m.ID == ch.Model {
				if !Usable(m) { // FTR.NAB.CHAT-0001 R8, MDL-06
					return apperr.Unprocessable("model_no_tools", "the model cannot call tools").With("model", m.ID)
				}
				if ch.Thinking != "" && !m.SupportsThinking(ch.Thinking) {
					return apperr.Unprocessable("invalid_thinking", "the model does not support this reasoning level")
				}
				return nil
			}
		}
		return apperr.Unprocessable("invalid_model", "the model is not in the connection")
	}
	if pm.Default != nil {
		if err := check(*pm.Default); err != nil {
			return pm, err
		}
	}
	found := pm.Default == nil
	for _, a := range pm.Available {
		if err := check(a); err != nil {
			return pm, err
		}
		if pm.Default != nil && a.ConnectionID == pm.Default.ConnectionID && a.Model == pm.Default.Model {
			found = true
		}
	}
	if !found {
		pm.Available = append([]Choice{{ConnectionID: pm.Default.ConnectionID, Model: pm.Default.Model}}, pm.Available...)
	}
	if pm.Available == nil {
		pm.Available = []Choice{}
	}
	b, _ := json.Marshal(pm)
	_, err := s.Pool.Exec(ctx, `INSERT INTO settings (key, value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, personalKey, b)
	return pm, err
}

// Caps are the capabilities of a model as the user sees them (R16).
type Caps struct {
	Image         bool  `json:"image"`
	Tools         bool  `json:"tools"`
	Reasoning     bool  `json:"reasoning"`
	ContextWindow int64 `json:"contextWindow"`
}

// CapsOf returns the capabilities of a description.
func CapsOf(d agent.ModelDef) Caps {
	return Caps{Image: d.HasInput("image"), Tools: d.Tools, Reasoning: d.Reasoning, ContextWindow: d.ContextWindow}
}

// AvailableModel is a selectable model as the user sees it.
type AvailableModel struct {
	ConnectionID uuid.UUID `json:"connectionId"`
	Model        string    `json:"model"`
	Name         string    `json:"name"`
	Connection   string    `json:"connection"`
	Caps         Caps      `json:"caps"`
}

// AvailableForUsers lists the models a user may choose (R19, MOD-03): models
// of enabled connections that a personal agent can run on.
func (s *Service) AvailableForUsers(ctx context.Context) ([]AvailableModel, error) {
	pm, err := s.Personal(ctx)
	if err != nil {
		return nil, err
	}
	out := []AvailableModel{}
	conns := map[uuid.UUID]*Connection{}
	for _, a := range pm.Available {
		c, ok := conns[a.ConnectionID]
		if !ok {
			if c, err = s.Get(ctx, a.ConnectionID); err != nil {
				return nil, err
			}
			conns[a.ConnectionID] = c
		}
		if c == nil || !c.Enabled {
			continue
		}
		for _, m := range c.Models {
			if m.ID == a.Model && Usable(m) {
				name := m.Name
				if name == "" {
					name = m.ID
				}
				out = append(out, AvailableModel{ConnectionID: a.ConnectionID, Model: a.Model, Name: name, Connection: c.Name, Caps: CapsOf(m)})
			}
		}
	}
	return out, nil
}

// AvailableFor lists the models the user may choose: the access of the
// user's groups (FTR.NAB.CMN-0007 R8), or the global list without groups.
func (s *Service) AvailableFor(ctx context.Context, uid uuid.UUID) ([]AvailableModel, error) {
	if s.Access == nil {
		return s.AvailableForUsers(ctx)
	}
	a, err := s.Access(ctx, uid)
	if err != nil {
		return nil, err
	}
	return s.availableOf(ctx, a)
}

func (s *Service) availableOf(ctx context.Context, a *access.Access) ([]AvailableModel, error) {
	out := []AvailableModel{}
	conns := map[uuid.UUID]*Connection{}
	for _, m := range a.Models {
		c, ok := conns[m.ConnectionID]
		if !ok {
			var err error
			if c, err = s.Get(ctx, m.ConnectionID); err != nil {
				return nil, err
			}
			conns[m.ConnectionID] = c
		}
		if c == nil {
			continue
		}
		for _, d := range c.Models {
			if d.ID == m.Model {
				out = append(out, AvailableModel{ConnectionID: m.ConnectionID, Model: m.Model, Name: m.Name, Connection: m.Connection, Caps: CapsOf(d)})
			}
		}
	}
	return out, nil
}

// DefaultFor is the default model of the user (FTR.NAB.CMN-0007 R8): what the
// agent runs on when the user did not choose; nil — no model.
func (s *Service) DefaultFor(ctx context.Context, uid uuid.UUID) (*AvailableModel, error) {
	var ch *Choice
	if s.Access != nil {
		a, err := s.Access(ctx, uid)
		if err != nil {
			return nil, err
		}
		if a.DefaultModel == nil {
			return nil, nil
		}
		if a.DefaultModel.Source == "user" { // the default without the user's choice
			cp := *a
			cp.DefaultModel = nil
			if d := s.defaultWithoutChoice(ctx, uid, &cp); d != nil {
				ch = d
			}
		} else {
			ch = &Choice{ConnectionID: a.DefaultModel.ConnectionID, Model: a.DefaultModel.Model}
		}
	} else if pm, err := s.Personal(ctx); err == nil && pm.Default != nil {
		ch = pm.Default
	}
	if ch == nil {
		return nil, nil
	}
	d, err := s.Def(ctx, ch.ConnectionID, ch.Model)
	if err != nil || d == nil {
		return nil, err
	}
	c, _ := s.Get(ctx, ch.ConnectionID)
	name, conn := d.Name, ""
	if name == "" {
		name = d.ID
	}
	if c != nil {
		conn = c.Name
	}
	return &AvailableModel{ConnectionID: ch.ConnectionID, Model: d.ID, Name: name, Connection: conn, Caps: CapsOf(*d)}, nil
}

// defaultWithoutChoice recomputes the default of the access without the
// user's own choice.
func (s *Service) defaultWithoutChoice(ctx context.Context, uid uuid.UUID, a *access.Access) *Choice {
	var gid uuid.UUID
	var conn uuid.UUID
	var model string
	err := s.Pool.QueryRow(ctx, `SELECT g.id, ag.connection_id, ag.model FROM access_grants ag JOIN user_groups g ON g.id = ag.group_id
		WHERE ag.is_default AND (g.builtin = 'all' OR g.id IN (SELECT group_id FROM user_group_members WHERE user_id = $1))
		ORDER BY g.position LIMIT 1`, uid).Scan(&gid, &conn, &model)
	if err == nil && a.HasModel(conn, model) {
		return &Choice{ConnectionID: conn, Model: model}
	}
	if pm, err := s.Personal(ctx); err == nil && pm.Default != nil && a.HasModel(pm.Default.ConnectionID, pm.Default.Model) {
		return pm.Default
	}
	if len(a.Models) > 0 {
		first := a.Models[0]
		for _, m := range a.Models[1:] {
			if m.Name < first.Name {
				first = m
			}
		}
		return &Choice{ConnectionID: first.ConnectionID, Model: first.Model}
	}
	return nil
}

// ErrNoModels is no_models_granted (FTR.NAB.CMN-0007 R8, RES-03).
func ErrNoModels() error {
	return apperr.Conflict("no_models_granted", "no model is available to you; ask an administrator")
}

// ResolveUser resolves the model of the personal agent of a user (or of the
// owner of a group agent): the choice when it is granted, else the default.
func (s *Service) ResolveUser(ctx context.Context, uid uuid.UUID, conn *uuid.UUID, model *string) (agent.ModelSpec, string, error) {
	if s.Access == nil {
		return s.ResolvePersonal(ctx, conn, model)
	}
	a, err := s.Access(ctx, uid)
	if err != nil {
		return agent.ModelSpec{}, "", err
	}
	if a.DefaultModel == nil {
		return agent.ModelSpec{}, "", ErrNoModels()
	}
	ch := Choice{ConnectionID: a.DefaultModel.ConnectionID, Model: a.DefaultModel.Model}
	if conn != nil && model != nil && a.HasModel(*conn, *model) { // the model of a group agent
		ch = Choice{ConnectionID: *conn, Model: *model}
	}
	// the level of reasoning is the one of the global default for that model
	if pm, err := s.Personal(ctx); err == nil && pm.Default != nil && pm.Default.ConnectionID == ch.ConnectionID && pm.Default.Model == ch.Model {
		ch.Thinking = pm.Default.Thinking
	}
	return s.Resolve(ctx, ch)
}

// ModelInfo tells whether a model of a connection exists and can run a
// personal agent (the check of the grants of a group).
func (s *Service) ModelInfo(ctx context.Context, conn uuid.UUID, model string) (bool, bool) {
	d, err := s.Def(ctx, conn, model)
	if err != nil || d == nil {
		return false, false
	}
	return true, Usable(*d)
}

// RecordResult marks the connection by the outcome of a run.
func (s *Service) RecordResult(ctx context.Context, connID string, class agent.ErrorClass) {
	id, err := uuid.Parse(connID)
	if err != nil {
		return
	}
	if class == "" {
		_, _ = s.Pool.Exec(ctx, `UPDATE model_connections SET status = 'ok', status_reason = NULL WHERE id = $1 AND status <> 'ok'`, id)
		return
	}
	if class.ConnectionProblem() {
		_, _ = s.Pool.Exec(ctx, `UPDATE model_connections SET status = $2, status_reason = $2, checked_at = now() WHERE id = $1`, id, string(class))
	}
}

// CheckConnection checks every model of a connection and probes images and
// tools (FTR.NAB.CHAT-0001 R9). A probe that confirms a capability of a
// model described by the defaults becomes its source; one that does not
// confirm it is reported and changes nothing.
func (s *Service) CheckConnection(ctx context.Context, id uuid.UUID) (*agent.LLMCheckResponse, error) {
	c, err := s.Get(ctx, id)
	if err != nil || c == nil {
		return nil, notFound(err)
	}
	var chat []agent.ModelDef
	for _, m := range c.Models {
		if !m.Embeddings {
			chat = append(chat, m)
		}
	}
	if len(c.Models) == 0 {
		return &agent.LLMCheckResponse{Results: []agent.LLMCheckResult{}}, nil
	}
	first := c.Models[0].ID
	if len(chat) > 0 {
		first = chat[0].ID
	}
	spec, key, err := s.spec(ctx, c, first, "off")
	if err != nil {
		return nil, err
	}
	res, err := s.runCheck(ctx, spec, key, []string{agent.ProbeImage, agent.ProbeTools})
	if err != nil {
		return nil, err
	}
	status, reason := "ok", ""
	for _, r := range res.Results {
		if !r.OK {
			status, reason = "error", string(r.ErrorClass)
			if r.ErrorClass.ConnectionProblem() {
				status = string(r.ErrorClass)
			}
		}
	}
	defs := slices.Clone(c.Models)
	changed := false
	for _, r := range res.Results {
		for i := range defs {
			if defs[i].ID != r.Model || defs[i].Sources == nil {
				continue
			}
			if r.Caps[agent.ProbeTools] == agent.ProbeConfirmed && defs[i].Sources[FTools] == SrcDefault {
				defs[i].Sources[FTools], defs[i].Tools, changed = SrcProbe, true, true
			}
		}
	}
	if changed {
		b, _ := json.Marshal(defs)
		_, _ = s.Pool.Exec(ctx, `UPDATE model_connections SET models = $2 WHERE id = $1`, id, b)
	}
	_, _ = s.Pool.Exec(ctx, `UPDATE model_connections SET status = $2, status_reason = NULLIF($3,''), checked_at = now() WHERE id = $1`, id, status, reason)
	return res, nil
}

// CheckDraft checks a connection before it is saved. GigaChat answers only
// through the bridge of a saved connection: a draft is checked by its list
// of models.
func (s *Service) CheckDraft(ctx context.Context, in Input) (*agent.LLMCheckResponse, error) {
	defs, err := in.Normalize()
	if err != nil {
		return nil, err
	}
	if in.Type == TypeGigaChat {
		conn := &Connection{ID: uuid.New(), Type: in.Type, BaseURL: in.BaseURL, Options: in.Options}
		start := time.Now()
		if _, err := s.list(ctx, conn, in.APIKey); err != nil {
			e := listErr(err)
			out := &agent.LLMCheckResponse{}
			for _, d := range defs {
				out.Results = append(out.Results, agent.LLMCheckResult{Model: d.ID, ErrorClass: agent.ErrAuth, Message: e.Error()})
			}
			return out, nil
		}
		out := &agent.LLMCheckResponse{}
		for _, d := range defs {
			out.Results = append(out.Results, agent.LLMCheckResult{Model: d.ID, OK: true, LatencyMs: time.Since(start).Milliseconds()})
		}
		return out, nil
	}
	spec := agent.ModelSpec{Provider: "nabu-check", ConnectionID: "draft", API: "openai-completions", BaseURL: in.BaseURL, ModelID: defs[0].ID, Thinking: "off", Models: defs}
	return s.runCheck(ctx, spec, in.APIKey, nil)
}

func (s *Service) runCheck(ctx context.Context, spec agent.ModelSpec, key string, probes []string) (*agent.LLMCheckResponse, error) {
	if s.Check == nil {
		return nil, apperr.Unavailable("agent_unavailable", "the agent operator is not reachable")
	}
	res, err := s.Check(ctx, agent.LLMCheckRequest{Model: spec, Secrets: agent.Secrets{LLMKey: key}, Probes: probes})
	if err != nil {
		slog.WarnContext(ctx, "model check failed", "err", err)
		return nil, apperr.Unavailable("agent_unavailable", "the agent operator is not reachable: "+err.Error())
	}
	s.checkEmbeddings(ctx, spec.BaseURL, key, res)
	return res, nil
}

// checkEmbeddings gives a second chance to the models that failed the check by
// a chat request: a model of embeddings refuses to chat (the provider answers
// as if the key were wrong) but is a working model of a connection
// (FTR.NAB.CMN-0003 R20).
func (s *Service) checkEmbeddings(ctx context.Context, baseURL, key string, res *agent.LLMCheckResponse) {
	if s.Embed == nil || res == nil {
		return
	}
	for i, r := range res.Results {
		if r.OK {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		start := time.Now()
		err := s.Embed(cctx, baseURL, key, r.Model)
		cancel()
		if err == nil {
			res.Results[i] = agent.LLMCheckResult{Model: r.Model, OK: true, LatencyMs: time.Since(start).Milliseconds(), Embeddings: true}
		}
	}
}

// Discover lists the models of a draft connection with their capabilities
// (POST /model-connections/discover, R7).
func (s *Service) Discover(ctx context.Context, in Input) (Discovered, error) {
	if err := in.normalize(); err != nil {
		return Discovered{}, err
	}
	if len(in.APIKey) < 8 {
		return Discovered{}, apperr.Unprocessable("invalid_key", "the API key is required").With("field", "apiKey")
	}
	return s.discover(ctx, &Connection{ID: uuid.New(), Type: in.Type, BaseURL: in.BaseURL, Options: in.Options}, in.APIKey, nil)
}

// Refresh asks the provider again (R10): new models are marked, models the
// provider no longer lists are marked gone, capabilities without an edit are
// described again.
func (s *Service) Refresh(ctx context.Context, id uuid.UUID) (*Connection, error) {
	c, err := s.Get(ctx, id)
	if err != nil || c == nil {
		return nil, notFound(err)
	}
	key, err := s.key(ctx, id)
	if err != nil {
		return nil, err
	}
	d, err := s.discover(ctx, c, key, c.Models)
	if err != nil {
		metrics.ModelsRefresh.WithLabelValues(c.Type, "error").Inc()
		return nil, err
	}
	fresh := map[string]agent.ModelDef{}
	for _, m := range d.Models {
		fresh[m.ID] = m
		for _, a := range m.Aliases {
			fresh[a] = m
		}
	}
	defs := make([]agent.ModelDef, 0, len(c.Models))
	for _, m := range c.Models {
		f, ok := fresh[m.ID]
		switch {
		case ok && f.ID == m.ID:
			f.Gone = false
			defs = append(defs, f)
		case ok:
			// the connection uses an alias of a listed model: it keeps its id and its edits
			m.Gone = false
			defs = append(defs, build(c.Type, m.ID, nil, &m))
		case d.Listed:
			m.Gone = true
			defs = append(defs, m)
		default:
			defs = append(defs, build(c.Type, m.ID, nil, &m))
		}
	}
	prev := map[string]time.Time{}
	for _, n := range c.Discovered {
		prev[n.ID] = n.FirstSeenAt
	}
	disc := []NewModel{}
	if d.Listed {
		for _, m := range d.Models {
			if slices.ContainsFunc(defs, func(x agent.ModelDef) bool { return x.ID == m.ID || slices.Contains(m.Aliases, x.ID) }) {
				continue
			}
			first, ok := prev[m.ID]
			if !ok {
				first = time.Now().UTC()
			}
			disc = append(disc, NewModel{ID: m.ID, Name: m.Name, FirstSeenAt: first})
		}
	}
	mb, _ := json.Marshal(defs)
	db, _ := json.Marshal(disc)
	if _, err := s.Pool.Exec(ctx, `UPDATE model_connections SET models = $2, discovered = $3, refreshed_at = now() WHERE id = $1`, id, mb, db); err != nil {
		return nil, err
	}
	metrics.ModelsRefresh.WithLabelValues(c.Type, "ok").Inc()
	return s.Get(ctx, id)
}

// RefreshAll refreshes every enabled connection (the daily job of the worker).
func (s *Service) RefreshAll(ctx context.Context) {
	list, err := s.List(ctx)
	if err != nil {
		slog.WarnContext(ctx, "refresh models", "err", err)
		return
	}
	for _, c := range list {
		if !c.Enabled {
			continue
		}
		// MDL-10: a connection refreshed by another worker within the hour is skipped
		tag, err := s.Pool.Exec(ctx, `UPDATE model_connections SET refreshed_at = now() WHERE id = $1
			AND (refreshed_at IS NULL OR refreshed_at < now() - interval '1 hour')`, c.ID)
		if err != nil || tag.RowsAffected() == 0 {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, time.Minute)
		if _, err := s.Refresh(cctx, c.ID); err != nil {
			slog.WarnContext(ctx, "refresh models", "connection", c.Name, "err", err)
		}
		cancel()
	}
}

// Bootstrap creates the first connection from the deployment's key when
// there is none, and makes its first model the default of personal agents.
func (s *Service) Bootstrap(ctx context.Context, typ, baseURL, key string) error {
	if key == "" {
		return nil
	}
	var n int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM model_connections`).Scan(&n); err != nil || n > 0 {
		return err
	}
	c, err := s.Create(ctx, Input{Type: typ, BaseURL: baseURL, APIKey: key, ImportRef: "bootstrap"})
	if err != nil {
		return err
	}
	pm, err := s.Personal(ctx)
	if err != nil || pm.Default != nil || len(c.Models) == 0 {
		return err
	}
	ch := Choice{ConnectionID: c.ID, Model: c.Models[0].ID, Thinking: "off"}
	avail := []Choice{}
	for _, m := range c.Models {
		if Usable(m) {
			avail = append(avail, Choice{ConnectionID: c.ID, Model: m.ID})
		}
	}
	_, err = s.PutPersonal(ctx, PersonalModels{Default: &ch, Available: avail})
	slog.Info("bootstrap model connection created", "connection", c.Name)
	return err
}

// ─── HTTP ───────────────────────────────────────────────────────────

// typesView lists the types of connections for the form (R15).
func typesView() []map[string]any {
	dir := func(typ string) []agent.ModelDef {
		out := []agent.ModelDef{}
		for _, k := range ofType(typ) {
			out = append(out, build(typ, k.def.ID, nil, nil))
		}
		return out
	}
	return []map[string]any{
		{"type": TypeDeepSeek, "baseUrl": DeepSeekURL, "models": dir(TypeDeepSeek)},
		{"type": TypeGigaChat, "baseUrl": gigachat.DefaultAPIURL, "scopes": gigachat.Scopes, "defaultScope": gigachat.DefaultScope, "models": dir(TypeGigaChat)},
		{"type": TypeZAI, "endpoints": map[string]string{"general": ZAIURL, "coding": ZAICodingURL}, "models": dir(TypeZAI)},
		{"type": TypeOpenAICompatible},
	}
}

// AdminRoutes mounts /model-connections and /personal-models (tech §4).
func (s *Service) AdminRoutes(r chi.Router) {
	r.Get("/model-connections", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		list, err := s.List(r.Context())
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": list, "types": typesView()})
		return nil
	}))
	r.Post("/model-connections", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in Input
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		in.Defs, in.ImportRef = nil, ""
		c, err := s.Create(r.Context(), in)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, c)
		return nil
	}))
	r.Post("/model-connections/discover", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in Input
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		in.Defs = nil
		d, err := s.Discover(r.Context(), in)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, d)
		return nil
	}))
	r.Post("/model-connections/check", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in Input
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		in.Defs = nil
		res, err := s.CheckDraft(r.Context(), in)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, res)
		return nil
	}))
	r.Patch("/model-connections/{id}", withID(func(w http.ResponseWriter, r *http.Request, id uuid.UUID) error {
		var p Patch
		if err := httpx.Decode(r, &p); err != nil {
			return err
		}
		c, err := s.Update(r.Context(), id, p)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, c)
		return nil
	}))
	r.Put("/model-connections/{id}/key", withID(func(w http.ResponseWriter, r *http.Request, id uuid.UUID) error {
		var in struct {
			APIKey string `json:"apiKey"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		c, err := s.ReplaceKey(r.Context(), id, in.APIKey)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]string{"keyLast4": c.KeyLast4})
		return nil
	}))
	r.Delete("/model-connections/{id}", withID(func(w http.ResponseWriter, r *http.Request, id uuid.UUID) error {
		if err := s.Delete(r.Context(), id); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Post("/model-connections/{id}/check", withID(func(w http.ResponseWriter, r *http.Request, id uuid.UUID) error {
		res, err := s.CheckConnection(r.Context(), id)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, res)
		return nil
	}))
	r.Post("/model-connections/{id}/refresh", withID(func(w http.ResponseWriter, r *http.Request, id uuid.UUID) error {
		c, err := s.Refresh(r.Context(), id)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, c)
		return nil
	}))
	r.Get("/personal-models", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		pm, err := s.PersonalView(r.Context())
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, pm)
		return nil
	}))
	r.Put("/personal-models", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var pm PersonalModels
		if err := httpx.Decode(r, &pm); err != nil {
			return err
		}
		if s.Access != nil { // FTR.NAB.CMN-0007 OLD-02: the available models are granted to groups
			if pm.Available != nil {
				cur, err := s.PersonalView(r.Context())
				if err != nil {
					return err
				}
				if !sameChoices(pm.Available, cur.Available) {
					return apperr.Unprocessable("use_groups", "the available models are granted to groups in Administration → Groups")
				}
			}
			old, err := s.Personal(r.Context())
			if err != nil {
				return err
			}
			pm.Available = old.Available
		}
		out, err := s.PutPersonal(r.Context(), pm)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, out)
		return nil
	}))
}

func sameChoices(a, b []Choice) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ConnectionID != b[i].ConnectionID || a[i].Model != b[i].Model {
			return false
		}
	}
	return true
}

func withID(fn func(w http.ResponseWriter, r *http.Request, id uuid.UUID) error) http.HandlerFunc {
	return httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			return apperr.NotFound("not_found", "not found")
		}
		return fn(w, r, id)
	})
}
