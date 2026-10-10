// Package httpx contains HTTP helpers shared by all slices: JSON encoding,
// error mapping, cursor pagination, request context and middleware.
package httpx

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
)

// JSON writes v with the given status.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v != nil {
		_ = json.NewEncoder(w).Encode(v)
	}
}

// NoContent writes 204.
func NoContent(w http.ResponseWriter) { w.WriteHeader(http.StatusNoContent) }

// Error writes err as {"error": {...}}. Unknown errors become 500 and are logged.
func Error(w http.ResponseWriter, r *http.Request, err error) {
	if e, ok := apperr.As(err); ok {
		if ra := e.Details["retryAfter"]; ra != nil {
			w.Header().Set("Retry-After", strconv.Itoa(toInt(ra)))
		}
		JSON(w, e.Status, map[string]any{"error": e})
		return
	}
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) {
		JSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": apperr.TooLarge("too_large", "request body too large")})
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	slog.ErrorContext(r.Context(), "request failed", "err", err, "path", r.URL.Path)
	JSON(w, http.StatusInternalServerError, map[string]any{"error": apperr.New(500, "internal", "internal error")})
}

func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	}
	return 10
}

// Decode reads a JSON body into v (max 1 MiB).
func Decode(r *http.Request, v any) error {
	return DecodeMax(r, v, 1<<20)
}

// DecodeMax reads a JSON body of at most max bytes into v.
func DecodeMax(r *http.Request, v any, max int64) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, max))
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return apperr.TooLarge("too_large", "request body too large")
		}
		return apperr.BadRequest("invalid_json", "invalid JSON body: "+err.Error())
	}
	return nil
}

// Handler adapts a handler that returns an error.
func Handler(fn func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			Error(w, r, err)
		}
	}
}

// Bearer returns the bearer token of the request or "".
func Bearer(r *http.Request) string {
	if t, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		return strings.TrimSpace(t)
	}
	return ""
}

// ─── Pagination ─────────────────────────────────────────────────────

// Cursor is the decoded keyset position: a timestamp and a tiebreaker id.
type Cursor struct {
	T  time.Time `json:"t"`
	ID string    `json:"i"`
}

// Page holds pagination parameters.
type Page struct {
	Cursor *Cursor
	Limit  int
}

// ParsePage reads ?cursor=&limit= (default 50, max 200).
func ParsePage(r *http.Request) (Page, error) {
	p := Page{Limit: 50}
	if l := r.URL.Query().Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n <= 0 {
			return p, apperr.BadRequest("invalid_limit", "limit must be a positive integer")
		}
		if n > 200 {
			n = 200
		}
		p.Limit = n
	}
	if c := r.URL.Query().Get("cursor"); c != "" {
		raw, err := base64.RawURLEncoding.DecodeString(c)
		if err != nil {
			return p, apperr.BadRequest("invalid_cursor", "invalid cursor")
		}
		var cur Cursor
		if err := json.Unmarshal(raw, &cur); err != nil {
			return p, apperr.BadRequest("invalid_cursor", "invalid cursor")
		}
		p.Cursor = &cur
	}
	return p, nil
}

// EncodeCursor builds an opaque cursor.
func EncodeCursor(t time.Time, id string) string {
	b, _ := json.Marshal(Cursor{T: t, ID: id})
	return base64.RawURLEncoding.EncodeToString(b)
}

// List is a paginated response.
type List[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"nextCursor"`
}

// NewList trims a result fetched with limit+1 rows and computes the next cursor.
func NewList[T any](items []T, limit int, key func(T) (time.Time, string)) List[T] {
	l := List[T]{Items: items}
	if l.Items == nil {
		l.Items = []T{}
	}
	if len(items) > limit {
		l.Items = items[:limit]
		t, id := key(items[limit-1])
		c := EncodeCursor(t, id)
		l.NextCursor = &c
	}
	return l
}

// ─── Request context ────────────────────────────────────────────────

type ctxKey int

const (
	principalKey ctxKey = iota
	sessionKey
	clientKey
)

// Session is the authenticated browser session.
type Session struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	CSRFToken string
}

// Client is the authenticated service client of a /client/v1 request.
type Client struct {
	ID          uuid.UUID
	Name        string
	Agents      []string
	CanDelegate bool
	CanImport   bool
	// FTR.NAB.CMN-0002 R22: rights users:archive and users:restore.
	CanArchive bool
	CanRestore bool
}

// WithPrincipal stores the principal.
func WithPrincipal(ctx context.Context, p *domain.Principal) context.Context {
	return context.WithValue(ctx, principalKey, p)
}

// PrincipalFrom returns the principal or nil.
func PrincipalFrom(ctx context.Context) *domain.Principal {
	p, _ := ctx.Value(principalKey).(*domain.Principal)
	return p
}

// MustPrincipal returns the principal or an unauthenticated error.
func MustPrincipal(r *http.Request) (*domain.Principal, error) {
	p := PrincipalFrom(r.Context())
	if p == nil {
		return nil, apperr.ErrNoSession
	}
	return p, nil
}

// WithSession stores the session.
func WithSession(ctx context.Context, s *Session) context.Context {
	return context.WithValue(ctx, sessionKey, s)
}

// SessionFrom returns the session or nil.
func SessionFrom(ctx context.Context) *Session {
	s, _ := ctx.Value(sessionKey).(*Session)
	return s
}

// WithClient stores the service client.
func WithClient(ctx context.Context, c *Client) context.Context {
	return context.WithValue(ctx, clientKey, c)
}

// ClientFrom returns the service client or nil.
func ClientFrom(ctx context.Context) *Client {
	c, _ := ctx.Value(clientKey).(*Client)
	return c
}

// ParamUUID reads a UUID URL parameter.
func ParamUUID(r *http.Request, name string) (uuid.UUID, error) {
	id, err := uuid.Parse(chi.URLParam(r, name))
	if err != nil {
		return uuid.Nil, apperr.NotFound("not_found", "not found")
	}
	return id, nil
}
