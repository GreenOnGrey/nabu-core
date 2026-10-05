// Package clients keeps the service clients of Nabu (FTR.NAB.CMN-0001 R17,
// R1a; arch §7; tech §4, §5): products such as Hammurapi that call service
// agents and, with the delegation right, act on behalf of users. A client
// exchanges its client_id and secret for a 15-minute token at /oauth/token.
package clients

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
)

// TokenTTL is the lifetime of a client token (tech §5).
const TokenTTL = 15 * time.Minute

// SecretOverlap is how long the old secret works after a reissue (SVC-09).
const SecretOverlap = 24 * time.Hour

// Client is a service client as administrators see it.
type Client struct {
	ID                   uuid.UUID      `json:"id"`
	Name                 string         `json:"name"`
	URL                  string         `json:"url"`
	ClientID             string         `json:"clientId"`
	Agents               []string       `json:"agents"`
	CanDelegate          bool           `json:"canDelegate"`
	CanImport            bool           `json:"canImport"`
	Limits               map[string]any `json:"limits"`
	Enabled              bool           `json:"enabled"`
	PrevSecretExpiresAt  *time.Time     `json:"prevSecretExpiresAt"`
	CreatedAt            time.Time      `json:"createdAt"`
	secretHash, prevHash []byte
}

// Service manages clients.
type Service struct {
	Pool   *pgxpool.Pool
	Signer *jwt.Signer
	now    func() time.Time
}

func (s *Service) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

const cols = `id, name, COALESCE(url,''), client_id, agents, can_delegate, can_import, limits, enabled, prev_secret_expires_at, created_at, secret_hash, prev_secret_hash`

func scan(row pgx.Row) (*Client, error) {
	var c Client
	var limits []byte
	if err := row.Scan(&c.ID, &c.Name, &c.URL, &c.ClientID, &c.Agents, &c.CanDelegate, &c.CanImport, &limits, &c.Enabled,
		&c.PrevSecretExpiresAt, &c.CreatedAt, &c.secretHash, &c.prevHash); err != nil {
		return nil, err
	}
	_ = json.Unmarshal(limits, &c.Limits)
	if c.Limits == nil {
		c.Limits = map[string]any{}
	}
	if c.Agents == nil {
		c.Agents = []string{}
	}
	return &c, nil
}

func hash(secret string) []byte {
	h := sha256.Sum256([]byte(secret))
	return h[:]
}

func newSecret() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return "nabu_" + hex.EncodeToString(b)
}

// List lists clients.
func (s *Service) List(ctx context.Context) ([]Client, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+cols+` FROM service_clients ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Client{}
	for rows.Next() {
		c, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

// Get loads a client.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (*Client, error) {
	c, err := scan(s.Pool.QueryRow(ctx, `SELECT `+cols+` FROM service_clients WHERE id = $1`, id))
	if postgres.IsNoRows(err) {
		return nil, apperr.NotFound("not_found", "client not found")
	}
	return c, err
}

// Load is the auth.ClientLoader: an enabled client for a request.
func (s *Service) Load(ctx context.Context, id uuid.UUID) (*httpx.Client, error) {
	c, err := scan(s.Pool.QueryRow(ctx, `SELECT `+cols+` FROM service_clients WHERE id = $1 AND enabled`, id))
	if postgres.IsNoRows(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &httpx.Client{ID: c.ID, Name: c.Name, Agents: c.Agents, CanDelegate: c.CanDelegate, CanImport: c.CanImport}, nil
}

var nameRe = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// Input is POST /clients and PATCH /clients/{id}.
type Input struct {
	Name        string          `json:"name"`
	URL         *string         `json:"url"`
	Agents      *[]string       `json:"agents"`
	CanDelegate *bool           `json:"canDelegate"`
	CanImport   *bool           `json:"canImport"`
	Limits      *map[string]any `json:"limits"`
	Enabled     *bool           `json:"enabled"`
}

// Created is a new client with its secret, shown once.
type Created struct {
	Client
	Secret string `json:"secret"`
}

// Create registers a client.
func (s *Service) Create(ctx context.Context, in Input) (*Created, error) {
	in.Name = strings.TrimSpace(in.Name)
	if !nameRe.MatchString(in.Name) {
		return nil, apperr.Unprocessable("invalid_name", "the name is lower-case words with dashes").With("field", "name")
	}
	secret := newSecret()
	agents := []string{}
	if in.Agents != nil {
		agents = *in.Agents
	}
	limits := map[string]any{}
	if in.Limits != nil {
		limits = *in.Limits
	}
	lb, _ := json.Marshal(limits)
	var id uuid.UUID
	err := s.Pool.QueryRow(ctx, `INSERT INTO service_clients (name, url, client_id, secret_hash, agents, can_delegate, can_import, limits)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id`, in.Name, deref(in.URL), in.Name, hash(secret), agents,
		in.CanDelegate != nil && *in.CanDelegate, in.CanImport != nil && *in.CanImport, lb).Scan(&id)
	if postgres.IsUniqueViolation(err) {
		return nil, apperr.Conflict("name_taken", "a client with this name exists").With("field", "name")
	}
	if err != nil {
		return nil, err
	}
	c, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return &Created{Client: *c, Secret: secret}, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Update changes rights and limits.
func (s *Service) Update(ctx context.Context, id uuid.UUID, in Input) (*Client, error) {
	var lb []byte
	if in.Limits != nil {
		lb, _ = json.Marshal(*in.Limits)
	}
	tag, err := s.Pool.Exec(ctx, `UPDATE service_clients SET url = COALESCE($2, url), agents = COALESCE($3, agents),
		can_delegate = COALESCE($4, can_delegate), can_import = COALESCE($5, can_import), limits = COALESCE($6, limits),
		enabled = COALESCE($7, enabled) WHERE id = $1`, id, in.URL, in.Agents, in.CanDelegate, in.CanImport, lb, in.Enabled)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, apperr.NotFound("not_found", "client not found")
	}
	return s.Get(ctx, id)
}

// Reissue issues a new secret; the old one works for 24 hours (SVC-09).
func (s *Service) Reissue(ctx context.Context, id uuid.UUID) (*Created, error) {
	secret := newSecret()
	tag, err := s.Pool.Exec(ctx, `UPDATE service_clients SET prev_secret_hash = secret_hash, prev_secret_expires_at = $3, secret_hash = $2 WHERE id = $1`,
		id, hash(secret), s.clock().Add(SecretOverlap))
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, apperr.NotFound("not_found", "client not found")
	}
	c, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return &Created{Client: *c, Secret: secret}, nil
}

// Authenticate checks client credentials.
func (s *Service) Authenticate(ctx context.Context, clientID, secret string) (*Client, error) {
	c, err := scan(s.Pool.QueryRow(ctx, `SELECT `+cols+` FROM service_clients WHERE client_id = $1 AND enabled`, clientID))
	if err != nil {
		return nil, apperr.Unauthorized("invalid_client", "invalid client credentials")
	}
	h := hash(secret)
	if subtle.ConstantTimeCompare(h, c.secretHash) == 1 {
		return c, nil
	}
	if len(c.prevHash) > 0 && c.PrevSecretExpiresAt != nil && s.clock().Before(*c.PrevSecretExpiresAt) && subtle.ConstantTimeCompare(h, c.prevHash) == 1 {
		return c, nil
	}
	return nil, apperr.Unauthorized("invalid_client", "invalid client credentials")
}

// Token issues a client token: sub client:<id>, scope — allowed agents,
// delegate and import (tech §5).
func (s *Service) Token(c *Client) (string, []string) {
	scope := append([]string{}, c.Agents...)
	if c.CanDelegate {
		scope = append(scope, "delegate")
	}
	if c.CanImport {
		scope = append(scope, "import")
	}
	return s.Signer.Issue(jwt.Claims{Audience: jwt.AudClient, Subject: "client:" + c.ID.String(), Scope: scope}, TokenTTL), scope
}

// TokenRoute mounts POST /oauth/token (grant_type=client_credentials, form
// or JSON; credentials in the body or with HTTP Basic).
func (s *Service) TokenRoute(r chi.Router) {
	r.Post("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		var grant, id, secret string
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			var in struct {
				GrantType    string `json:"grant_type"`
				ClientID     string `json:"client_id"`
				ClientSecret string `json:"client_secret"`
			}
			_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&in)
			grant, id, secret = in.GrantType, in.ClientID, in.ClientSecret
		} else {
			_ = r.ParseForm()
			grant, id, secret = r.PostForm.Get("grant_type"), r.PostForm.Get("client_id"), r.PostForm.Get("client_secret")
		}
		if u, p, ok := r.BasicAuth(); ok {
			id, secret = u, p
		}
		w.Header().Set("Cache-Control", "no-store")
		if grant != "client_credentials" {
			httpx.JSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_grant_type"})
			return
		}
		c, err := s.Authenticate(r.Context(), id, secret)
		if err != nil {
			httpx.JSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_client"})
			return
		}
		tok, scope := s.Token(c)
		httpx.JSON(w, 200, map[string]any{"access_token": tok, "token_type": "Bearer", "expires_in": int(TokenTTL.Seconds()), "scope": strings.Join(scope, " ")})
	})
}

// AdminRoutes mounts /clients (tech §4).
func (s *Service) AdminRoutes(r chi.Router) {
	r.Get("/clients", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		l, err := s.List(r.Context())
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Post("/clients", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in Input
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		c, err := s.Create(r.Context(), in)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, c)
		return nil
	}))
	r.Patch("/clients/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in Input
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		c, err := s.Update(r.Context(), id, in)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, c)
		return nil
	}))
	r.Post("/clients/{id}/secret", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		c, err := s.Reissue(r.Context(), id)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, c)
		return nil
	}))
}
