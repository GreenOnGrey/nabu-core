// Package gigachat connects Nabu to GigaChat (FTR.NAB.CHAT-0001 R13, arch
// §3.5, tech §6): the access token of a connection obtained by its
// authorization key, the list of models, the files of images, and the bridge —
// an OpenAI-compatible endpoint in `api` through which Pi talks to GigaChat.
// The authorization key never leaves `api`.
package gigachat

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// Defaults of a connection.
const (
	DefaultAuthURL = "https://ngw.devices.sberbank.ru:9443/api/v2/oauth"
	DefaultAPIURL  = "https://gigachat.devices.sberbank.ru/api/v1"
	DefaultScope   = "GIGACHAT_API_CORP"
)

// Scopes of the authorization key.
var Scopes = []string{"GIGACHAT_API_PERS", "GIGACHAT_API_B2B", "GIGACHAT_API_CORP"}

// ValidScope reports whether s is a scope of GigaChat.
func ValidScope(s string) bool {
	for _, v := range Scopes {
		if v == s {
			return true
		}
	}
	return false
}

// rootCA is the Russian Trusted Root CA of the Ministry of Digital
// Development (SHA-256 D2:6D:2D:02:31:B7:C3:9F:92:CC:73:85:12:BA:54:10:35:19:E4:40:5D:68:B5:BD:70:3E:97:88:CA:8E:CF:31,
// until 2032-02-27): the GigaChat endpoints are signed by it. It is trusted
// for GigaChat only, next to the system roots.
//
//go:embed russian_trusted_root_ca.pem
var rootCA []byte

// Conn is a GigaChat connection as the client needs it.
type Conn struct {
	ID      uuid.UUID
	AuthKey string // the authorization key (Basic)
	Scope   string
	AuthURL string
	APIURL  string
}

func (c Conn) authURL() string {
	if c.AuthURL != "" {
		return c.AuthURL
	}
	return DefaultAuthURL
}

func (c Conn) apiURL() string {
	if c.APIURL != "" {
		return strings.TrimRight(c.APIURL, "/")
	}
	return DefaultAPIURL
}

// Client talks to GigaChat and keeps the access tokens of the connections.
type Client struct {
	HTTP *http.Client
	now  func() time.Time

	mu     sync.Mutex
	tokens map[tokenKey]token
}

type tokenKey struct {
	conn  uuid.UUID
	key   string
	scope string
}

type token struct {
	value   string
	expires time.Time
}

// New creates a client. caFile adds a PEM file to the trusted roots of
// GigaChat (GIGACHAT_CA_FILE); empty keeps the embedded root.
func New(caFile string) (*Client, error) {
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	pool.AppendCertsFromPEM(rootCA)
	if caFile != "" {
		b, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("GIGACHAT_CA_FILE: %w", err)
		}
		if !pool.AppendCertsFromPEM(b) {
			return nil, errors.New("GIGACHAT_CA_FILE: no certificates")
		}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return &Client{HTTP: &http.Client{Transport: otelhttp.NewTransport(tr), Timeout: 0}}, nil
}

func (c *Client) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// Error is an answer of GigaChat with a status other than 2xx.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("gigachat %d: %s", e.Status, e.Message) }

func readError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var e struct {
		Message string `json:"message"`
		Error   any    `json:"error"`
	}
	msg := strings.TrimSpace(string(raw))
	if json.Unmarshal(raw, &e) == nil && e.Message != "" {
		msg = e.Message
	}
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return &Error{Status: resp.StatusCode, Message: msg}
}

// Token returns the access token of a connection: cached until a minute
// before it expires (the token lives 30 minutes).
func (c *Client) Token(ctx context.Context, conn Conn) (string, error) {
	k := tokenKey{conn.ID, conn.AuthKey, conn.Scope}
	c.mu.Lock()
	if t, ok := c.tokens[k]; ok && c.clock().Before(t.expires.Add(-time.Minute)) {
		c.mu.Unlock()
		return t.value, nil
	}
	c.mu.Unlock()
	scope := conn.Scope
	if scope == "" {
		scope = DefaultScope
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, conn.authURL(), strings.NewReader(url.Values{"scope": {scope}}.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Basic "+conn.AuthKey)
	req.Header.Set("RqUID", uuid.NewString())
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		metrics.GigaChatTokens.WithLabelValues("error").Inc()
		return "", fmt.Errorf("gigachat oauth: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		metrics.GigaChatTokens.WithLabelValues("rejected").Inc()
		return "", readError(resp)
	}
	metrics.GigaChatTokens.WithLabelValues("ok").Inc()
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresAt   int64  `json:"expires_at"` // milliseconds
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return "", fmt.Errorf("gigachat oauth: %w", err)
	}
	if out.AccessToken == "" {
		return "", errors.New("gigachat oauth: no access token")
	}
	exp := time.UnixMilli(out.ExpiresAt)
	if out.ExpiresAt == 0 {
		exp = c.clock().Add(30 * time.Minute)
	}
	c.mu.Lock()
	if c.tokens == nil {
		c.tokens = map[tokenKey]token{}
	}
	c.tokens[k] = token{value: out.AccessToken, expires: exp}
	c.mu.Unlock()
	return out.AccessToken, nil
}

// Invalidate forgets the token of a connection (GigaChat answered 401).
func (c *Client) Invalidate(conn Conn) {
	c.mu.Lock()
	delete(c.tokens, tokenKey{conn.ID, conn.AuthKey, conn.Scope})
	c.mu.Unlock()
}

// do sends a request with the access token; a 401 gets one retry with a new
// token (GC-03).
func (c *Client) do(ctx context.Context, conn Conn, build func(token string) (*http.Request, error)) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		tok, err := c.Token(ctx, conn)
		if err != nil {
			return nil, err
		}
		req, err := build(tok)
		if err != nil {
			return nil, err
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			return nil, fmt.Errorf("gigachat: %w", err)
		}
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 {
			resp.Body.Close()
			c.Invalidate(conn)
			continue
		}
		return resp, nil
	}
}

// Model is a model of GigaChat's list.
type Model struct {
	ID   string `json:"id"`
	Type string `json:"type"` // chat | embedder
}

// Models lists the models of the connection.
func (c *Client) Models(ctx context.Context, conn Conn) ([]Model, error) {
	resp, err := c.do(ctx, conn, func(tok string) (*http.Request, error) {
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, conn.apiURL()+"/models", nil)
		if err == nil {
			r.Header.Set("Authorization", "Bearer "+tok)
			r.Header.Set("Accept", "application/json")
		}
		return r, err
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, readError(resp)
	}
	var out struct {
		Data []Model `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&out); err != nil {
		return nil, fmt.Errorf("gigachat models: %w", err)
	}
	return out.Data, nil
}

// upload stores an image for one request and returns its id.
func (c *Client) upload(ctx context.Context, conn Conn, mime string, data []byte) (string, error) {
	resp, err := c.do(ctx, conn, func(tok string) (*http.Request, error) {
		var body bytes.Buffer
		mw := multipart.NewWriter(&body)
		ext := "png"
		if i := strings.IndexByte(mime, '/'); i > 0 {
			ext = mime[i+1:]
		}
		h := make(map[string][]string)
		h["Content-Disposition"] = []string{fmt.Sprintf(`form-data; name="file"; filename="image.%s"`, ext)}
		h["Content-Type"] = []string{mime}
		fw, err := mw.CreatePart(h)
		if err != nil {
			return nil, err
		}
		if _, err := fw.Write(data); err != nil {
			return nil, err
		}
		_ = mw.WriteField("purpose", "general")
		if err := mw.Close(); err != nil {
			return nil, err
		}
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, conn.apiURL()+"/files", &body)
		if err == nil {
			r.Header.Set("Authorization", "Bearer "+tok)
			r.Header.Set("Content-Type", mw.FormDataContentType())
		}
		return r, err
	})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return "", readError(resp)
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil || out.ID == "" {
		return "", errors.New("gigachat files: no id")
	}
	return out.ID, nil
}

// remove deletes uploaded files; errors are ignored.
func (c *Client) remove(ctx context.Context, conn Conn, ids []string) {
	for _, id := range ids {
		resp, err := c.do(ctx, conn, func(tok string) (*http.Request, error) {
			r, err := http.NewRequestWithContext(ctx, http.MethodPost, conn.apiURL()+"/files/"+url.PathEscape(id)+"/delete", nil)
			if err == nil {
				r.Header.Set("Authorization", "Bearer "+tok)
			}
			return r, err
		})
		if err == nil {
			resp.Body.Close()
		}
	}
}
