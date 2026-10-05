// Package models is the model access layer of Nabu (FTR.NAB.CMN-0001 R19–R21,
// tech §4): connections to providers — directly (DeepSeek first) or to any
// OpenAI-compatible endpoint such as LiteLLM — the default and available models
// of personal agents, and the resolution of a model choice into what a
// harness session needs.
package models

import (
	"context"
	"encoding/json"

	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/crypto"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// Connection types.
const (
	TypeDeepSeek         = "deepseek"
	TypeOpenAICompatible = "openai_compatible"
)

func strp(s string) *string { return &s }

// deepseekCompat follows DeepSeek's official configuration for Pi.
var deepseekCompat = map[string]any{
	"supportsStore": false, "supportsDeveloperRole": false, "maxTokensField": "max_tokens",
	"requiresReasoningContentOnAssistantMessages": true, "thinkingFormat": "deepseek",
}

var deepseekLevels = map[string]*string{"minimal": nil, "low": nil, "medium": nil, "high": strp("high"), "xhigh": strp("max")}

// DeepSeekModels are the models of the DeepSeek preset (prices: USD per million tokens).
var DeepSeekModels = []agent.ModelDef{
	{ID: "deepseek-v4-flash", Name: "DeepSeek V4 Flash", ContextWindow: 1_000_000, MaxTokens: 384_000, Reasoning: true,
		Input: []string{"text"}, ThinkingLevelMap: deepseekLevels, Compat: deepseekCompat,
		Cost: agent.Cost{Input: 0.14, Output: 0.28, CacheRead: 0.028}},
	{ID: "deepseek-v4-pro", Name: "DeepSeek V4 Pro", ContextWindow: 1_000_000, MaxTokens: 384_000, Reasoning: true,
		Input: []string{"text"}, ThinkingLevelMap: deepseekLevels, Compat: deepseekCompat,
		Cost: agent.Cost{Input: 1.74, Output: 3.48, CacheRead: 0.145}},
}

// Connection is a model connection as the API shows it (never the key).
type Connection struct {
	ID           uuid.UUID        `json:"id"`
	Name         string           `json:"name"`
	Type         string           `json:"type"`
	BaseURL      string           `json:"baseUrl"`
	Models       []agent.ModelDef `json:"models"`
	KeyLast4     string           `json:"keyLast4"`
	Enabled      bool             `json:"enabled"`
	Status       string           `json:"status"`
	StatusReason *string          `json:"statusReason"`
	CheckedAt    *time.Time       `json:"checkedAt"`
	CreatedAt    time.Time        `json:"createdAt"`
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
}

const connSelect = `SELECT id, name, type, base_url, api, models, COALESCE(key_last4,''), enabled, status, status_reason, checked_at, created_at FROM model_connections`

func scanConn(row pgx.Row) (*Connection, error) {
	var c Connection
	var models []byte
	if err := row.Scan(&c.ID, &c.Name, &c.Type, &c.BaseURL, &c.api, &models, &c.KeyLast4, &c.Enabled, &c.Status, &c.StatusReason, &c.CheckedAt, &c.CreatedAt); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(models, &c.Models); err != nil {
		return nil, err
	}
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

// Input is the body of POST /model-connections and of a check of a draft.
type Input struct {
	Name    string           `json:"name"`
	Type    string           `json:"type"`
	BaseURL string           `json:"baseUrl"`
	APIKey  string           `json:"apiKey"`
	Models  []string         `json:"models"`
	Defs    []agent.ModelDef `json:"modelDefs,omitempty"` // full definitions (import)
	// ImportRef makes a repeated import update the same connection.
	ImportRef string `json:"-"`
}

// Normalize validates the input and builds the model definitions.
func (in *Input) Normalize() ([]agent.ModelDef, error) {
	in.Name = strings.TrimSpace(in.Name)
	in.APIKey = strings.TrimSpace(in.APIKey)
	switch in.Type {
	case TypeDeepSeek:
		if in.BaseURL == "" {
			in.BaseURL = "https://api.deepseek.com"
		}
		if in.Name == "" {
			in.Name = "DeepSeek"
		}
	case TypeOpenAICompatible:
		if in.Name == "" {
			in.Name = "LiteLLM"
		}
	default:
		return nil, apperr.Unprocessable("invalid_type", "unknown connection type").With("field", "type")
	}
	u, err := url.Parse(in.BaseURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, apperr.Unprocessable("invalid_url", "the base URL must be http(s)").With("field", "baseUrl")
	}
	in.BaseURL = strings.TrimRight(in.BaseURL, "/")
	if len(in.Name) > 80 {
		return nil, apperr.Unprocessable("invalid_name", "the name is too long").With("field", "name")
	}
	if len(in.Defs) > 0 {
		return in.Defs, nil
	}
	var defs []agent.ModelDef
	if in.Type == TypeDeepSeek {
		byID := map[string]agent.ModelDef{}
		for _, m := range DeepSeekModels {
			byID[m.ID] = m
		}
		if len(in.Models) == 0 {
			return append([]agent.ModelDef(nil), DeepSeekModels...), nil
		}
		for _, id := range in.Models {
			m, ok := byID[id]
			if !ok {
				return nil, apperr.Unprocessable("invalid_models", "the models must be from the type's list").With("field", "models")
			}
			defs = append(defs, m)
		}
		return defs, nil
	}
	for _, id := range in.Models {
		if id = strings.TrimSpace(id); id != "" {
			defs = append(defs, agent.ModelDef{ID: id, Name: id, ContextWindow: 128_000, MaxTokens: 16_000, Input: []string{"text"}})
		}
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
	var ref *string
	if in.ImportRef != "" {
		ref = &in.ImportRef
	}
	var id uuid.UUID
	err = s.Pool.QueryRow(ctx, `INSERT INTO model_connections (name, type, base_url, api_key_enc, key_last4, models, import_ref)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (import_ref) DO UPDATE SET base_url = EXCLUDED.base_url, api_key_enc = EXCLUDED.api_key_enc,
			key_last4 = EXCLUDED.key_last4, models = EXCLUDED.models, updated_at = now()
		RETURNING id`, in.Name, in.Type, in.BaseURL, enc, last4(in.APIKey), b, ref).Scan(&id)
	if postgres.IsUniqueViolation(err) {
		return nil, apperr.Conflict("name_taken", "a connection with this name exists").With("field", "name")
	}
	if err != nil {
		return nil, err
	}
	return s.Get(ctx, id)
}

// Patch is PATCH /model-connections/{id}.
type Patch struct {
	Name    *string   `json:"name"`
	Enabled *bool     `json:"enabled"`
	Models  *[]string `json:"models"`
}

// Update changes name, state or models.
func (s *Service) Update(ctx context.Context, id uuid.UUID, p Patch) (*Connection, error) {
	c, err := s.Get(ctx, id)
	if err != nil || c == nil {
		return nil, notFound(err)
	}
	var models []byte
	if p.Models != nil {
		in := Input{Type: c.Type, BaseURL: c.BaseURL, Models: *p.Models}
		defs, err := in.Normalize()
		if err != nil {
			return nil, err
		}
		models, _ = json.Marshal(defs)
	}
	if p.Name != nil && strings.TrimSpace(*p.Name) == "" {
		return nil, apperr.Unprocessable("invalid_name", "the name is required").With("field", "name")
	}
	_, err = s.Pool.Exec(ctx, `UPDATE model_connections SET name = COALESCE($2, name), enabled = COALESCE($3, enabled),
		models = COALESCE($4, models), updated_at = now() WHERE id = $1`, id, p.Name, p.Enabled, models)
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

// Spec builds the model spec of a connection with the key.
func (s *Service) spec(ctx context.Context, c *Connection, model, thinking string) (agent.ModelSpec, string, error) {
	var enc []byte
	if err := s.Pool.QueryRow(ctx, `SELECT api_key_enc FROM model_connections WHERE id = $1`, c.ID).Scan(&enc); err != nil {
		return agent.ModelSpec{}, "", err
	}
	key, err := s.Box.Open(enc)
	if err != nil {
		return agent.ModelSpec{}, "", fmt.Errorf("decrypt the key of %s: %w", c.Name, err)
	}
	var def *agent.ModelDef
	for i := range c.Models {
		if c.Models[i].ID == model {
			def = &c.Models[i]
		}
	}
	if def == nil {
		return agent.ModelSpec{}, "", apperr.Conflict("agent_not_configured", fmt.Sprintf("the model %s is not in the connection %s", model, c.Name))
	}
	if thinking == "" || !def.SupportsThinking(thinking) {
		thinking = "off"
	}
	return agent.ModelSpec{Provider: "nabu-" + strings.ReplaceAll(c.ID.String(), "-", "")[:12], ConnectionID: c.ID.String(),
		API: c.api, BaseURL: c.BaseURL, ModelID: model, Thinking: thinking, Models: c.Models}, key, nil
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
				return s.Resolve(ctx, Choice{ConnectionID: a.ConnectionID, Model: a.Model, Thinking: th})
			}
		}
	}
	if pm.Default == nil {
		return agent.ModelSpec{}, "", ErrNotConfigured()
	}
	return s.Resolve(ctx, *pm.Default)
}

const personalKey = "personal_models"

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

// AvailableModel is a selectable model as the user sees it.
type AvailableModel struct {
	ConnectionID uuid.UUID `json:"connectionId"`
	Model        string    `json:"model"`
	Name         string    `json:"name"`
	Connection   string    `json:"connection"`
}

// AvailableForUsers lists the models a user may choose (R19, MOD-03).
func (s *Service) AvailableForUsers(ctx context.Context) ([]AvailableModel, error) {
	pm, err := s.Personal(ctx)
	if err != nil {
		return nil, err
	}
	out := []AvailableModel{}
	for _, a := range pm.Available {
		c, err := s.Get(ctx, a.ConnectionID)
		if err != nil {
			return nil, err
		}
		if c == nil || !c.Enabled {
			continue
		}
		name := a.Model
		for _, m := range c.Models {
			if m.ID == a.Model && m.Name != "" {
				name = m.Name
			}
		}
		out = append(out, AvailableModel{ConnectionID: a.ConnectionID, Model: a.Model, Name: name, Connection: c.Name})
	}
	return out, nil
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

// CheckConnection checks every model of a connection.
func (s *Service) CheckConnection(ctx context.Context, id uuid.UUID) (*agent.LLMCheckResponse, error) {
	c, err := s.Get(ctx, id)
	if err != nil || c == nil {
		return nil, notFound(err)
	}
	if len(c.Models) == 0 {
		return &agent.LLMCheckResponse{Results: []agent.LLMCheckResult{}}, nil
	}
	spec, key, err := s.spec(ctx, c, c.Models[0].ID, "off")
	if err != nil {
		return nil, err
	}
	res, err := s.runCheck(ctx, spec, key)
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
	_, _ = s.Pool.Exec(ctx, `UPDATE model_connections SET status = $2, status_reason = NULLIF($3,''), checked_at = now() WHERE id = $1`, id, status, reason)
	return res, nil
}

// CheckDraft checks a connection before it is saved.
func (s *Service) CheckDraft(ctx context.Context, in Input) (*agent.LLMCheckResponse, error) {
	defs, err := in.Normalize()
	if err != nil {
		return nil, err
	}
	api := "openai-completions"
	spec := agent.ModelSpec{Provider: "nabu-check", ConnectionID: "draft", API: api, BaseURL: in.BaseURL, ModelID: defs[0].ID, Thinking: "off", Models: defs}
	return s.runCheck(ctx, spec, in.APIKey)
}

func (s *Service) runCheck(ctx context.Context, spec agent.ModelSpec, key string) (*agent.LLMCheckResponse, error) {
	if s.Check == nil {
		return nil, apperr.Unavailable("agent_unavailable", "the agent operator is not reachable")
	}
	res, err := s.Check(ctx, agent.LLMCheckRequest{Model: spec, Secrets: agent.Secrets{LLMKey: key}})
	if err != nil {
		slog.WarnContext(ctx, "model check failed", "err", err)
		return nil, apperr.Unavailable("agent_unavailable", "the agent operator is not reachable: "+err.Error())
	}
	return res, nil
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
		avail = append(avail, Choice{ConnectionID: c.ID, Model: m.ID})
	}
	_, err = s.PutPersonal(ctx, PersonalModels{Default: &ch, Available: avail})
	slog.Info("bootstrap model connection created", "connection", c.Name)
	return err
}

// ─── HTTP ───────────────────────────────────────────────────────────

// AdminRoutes mounts /model-connections and /personal-models (tech §4).
func (s *Service) AdminRoutes(r chi.Router) {
	r.Get("/model-connections", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		list, err := s.List(r.Context())
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": list, "types": []map[string]any{
			{"type": TypeDeepSeek, "baseUrl": "https://api.deepseek.com", "models": DeepSeekModels},
			{"type": TypeOpenAICompatible},
		}})
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
	r.Get("/personal-models", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		pm, err := s.Personal(r.Context())
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
		out, err := s.PutPersonal(r.Context(), pm)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, out)
		return nil
	}))
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
