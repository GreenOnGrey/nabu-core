package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// OAuth of MCP (FTR.NAB.ADM-0001 R1–R3, tech §4.1): the administrator gives
// only the address of the server. Nabu finds its authorization server by the
// metadata of the protected resource (RFC 9728) and of the authorization
// server (RFC 8414), registers itself as a client (RFC 7591) with the
// callback of the item, and names the resource in the authorization and the
// token requests (RFC 8707). The connection of a user and the refresh of
// tokens are those of the plain OAuth kind.

// KindMCPOAuth is the kind of personal access discovered from the server.
const KindMCPOAuth = "mcp_oauth"

// AuthError is why the discovery or the registration failed; the item stays
// unpublished until the next successful save (design §3.1).
type AuthError struct {
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

const (
	codeDiscoveryFailed      = "mcp_oauth_discovery_failed"
	codeRegistrationUnsupp   = "mcp_oauth_registration_unsupported"
	codeRegistrationFailed   = "mcp_oauth_registration_failed"
	mcpOAuthMaxBody          = 64 << 10
	mcpOAuthTimeout          = 10 * time.Second
	mcpOAuthClientName       = "Nabu"
	authMethodNone           = "none"
	authMethodClientSecretPo = "client_secret_post"
)

// oauthKind reports whether a personal access is an OAuth one: plain or of MCP.
func oauthKind(kind string) bool { return kind == "oauth" || kind == KindMCPOAuth }

type mcpOAuthMeta struct {
	Issuer, AuthorizeURL, TokenURL, RegistrationURL, Resource string
	AuthMethods, Scopes                                       []string
}

type discoveryError struct{ code, detail string }

func (e *discoveryError) Error() string { return e.code + ": " + e.detail }

func discoveryErr(code, format string, a ...any) error {
	return &discoveryError{code: code, detail: fmt.Sprintf(format, a...)}
}

// oauthClient is the HTTP client of the discovery: a short timeout and no
// redirect to another host (tech §4.1, step 4).
func (s *Service) oauthClient() *http.Client {
	base := s.HTTP
	if base == nil {
		base = http.DefaultClient
	}
	c := *base
	c.Timeout = mcpOAuthTimeout
	c.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 || req.URL.Host != via[0].URL.Host || req.URL.Scheme != "https" {
			return errors.New("redirect to another host")
		}
		return nil
	}
	return &c
}

// privateHost reports whether a host is a loopback, private or link-local
// address or localhost: an authorization server named by the metadata must
// not point the API at the internal network (SSRF, arch §6).
func privateHost(host string) bool {
	h := strings.ToLower(host)
	if h == "localhost" || strings.HasSuffix(h, ".localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(h, "[]"))
	return ip != nil && (ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified())
}

// resolvesPrivate reports whether a name resolves to an internal address, or
// does not resolve: another host than the MCP server is trusted only when it
// is public (SSRF, arch §6).
func (s *Service) resolvesPrivate(ctx context.Context, host string) bool {
	if net.ParseIP(strings.Trim(host, "[]")) != nil {
		return false // a literal is checked by privateHost
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil || len(addrs) == 0 {
		return true
	}
	for _, a := range addrs {
		if privateHost(a.IP.String()) {
			return true
		}
	}
	return false
}

func (s *Service) getJSON(ctx context.Context, c *http.Client, u string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, mcpOAuthMaxBody))
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return resp.StatusCode, json.Unmarshal(b, out)
}

var resourceMetadataRe = regexp.MustCompile(`resource_metadata="([^"]+)"`)

// discoverMCPOAuth finds the authorization server of an MCP server (tech
// §4.1, steps 1–2).
func (s *Service) discoverMCPOAuth(ctx context.Context, mcpURL string) (*mcpOAuthMeta, error) {
	mu, err := url.Parse(mcpURL)
	if err != nil || mu.Scheme != "https" || mu.Host == "" {
		return nil, discoveryErr(codeDiscoveryFailed, "the address of the server must be https")
	}
	c := s.oauthClient()
	// 1. the challenge of the server names the metadata of the resource
	var candidates []string
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{
		"protocolVersion": mcp.ProtocolVersion, "capabilities": map[string]any{}, "clientInfo": map[string]string{"name": "nabu", "version": "1.0.0"}}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, mcpURL, bytes.NewReader(body))
	if err != nil {
		return nil, discoveryErr(codeDiscoveryFailed, "%v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if resp, err := c.Do(req); err == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, mcpOAuthMaxBody))
		resp.Body.Close()
		if m := resourceMetadataRe.FindStringSubmatch(resp.Header.Get("WWW-Authenticate")); m != nil {
			candidates = append(candidates, m[1])
		}
	}
	origin := mu.Scheme + "://" + mu.Host
	if p := strings.TrimRight(mu.Path, "/"); p != "" {
		candidates = append(candidates, origin+"/.well-known/oauth-protected-resource"+p)
	}
	candidates = append(candidates, origin+"/.well-known/oauth-protected-resource")
	var res struct {
		Resource             string   `json:"resource"`
		AuthorizationServers []string `json:"authorization_servers"`
		ScopesSupported      []string `json:"scopes_supported"`
	}
	found := false
	for _, cand := range candidates {
		cu, err := url.Parse(cand)
		if err != nil || cu.Scheme != "https" || cu.Host != mu.Host {
			continue // the metadata of the resource lives on the server itself
		}
		if _, err := s.getJSON(ctx, c, cand, &res); err == nil && len(res.AuthorizationServers) > 0 {
			found = true
			break
		}
	}
	if !found {
		return nil, discoveryErr(codeDiscoveryFailed, "the server gives no metadata of a protected resource")
	}
	resource := strings.TrimRight(res.Resource, "/")
	if resource == "" {
		resource = strings.TrimRight(mcpURL, "/")
	}
	if resource != strings.TrimRight(mcpURL, "/") {
		return nil, discoveryErr(codeDiscoveryFailed, "the metadata names another resource: %s", res.Resource)
	}
	// 2. the metadata of the authorization server
	asu, err := url.Parse(res.AuthorizationServers[0])
	if err != nil || asu.Scheme != "https" || asu.Host == "" || (asu.Host != mu.Host && (privateHost(asu.Hostname()) || s.resolvesPrivate(ctx, asu.Hostname()))) {
		return nil, discoveryErr(codeDiscoveryFailed, "the authorization server %s is not a public https address", res.AuthorizationServers[0])
	}
	asBase := strings.TrimRight(asu.String(), "/")
	asOrigin := asu.Scheme + "://" + asu.Host
	asPath := strings.TrimRight(asu.Path, "/")
	var as struct {
		Issuer                string   `json:"issuer"`
		AuthorizationEndpoint string   `json:"authorization_endpoint"`
		TokenEndpoint         string   `json:"token_endpoint"`
		RegistrationEndpoint  string   `json:"registration_endpoint"`
		CodeChallengeMethods  []string `json:"code_challenge_methods_supported"`
		AuthMethods           []string `json:"token_endpoint_auth_methods_supported"`
	}
	ok := false
	for _, cand := range []string{asOrigin + "/.well-known/oauth-authorization-server" + asPath, asBase + "/.well-known/openid-configuration"} {
		if _, err := s.getJSON(ctx, c, cand, &as); err == nil && as.AuthorizationEndpoint != "" {
			ok = true
			break
		}
	}
	if !ok {
		return nil, discoveryErr(codeDiscoveryFailed, "the authorization server %s gives no metadata", asBase)
	}
	for _, e := range []string{as.AuthorizationEndpoint, as.TokenEndpoint, as.RegistrationEndpoint} {
		if e == "" {
			continue
		}
		if eu, err := url.Parse(e); err != nil || eu.Scheme != "https" || eu.Host != asu.Host {
			return nil, discoveryErr(codeDiscoveryFailed, "the endpoint %s is not on the authorization server", e)
		}
	}
	if as.TokenEndpoint == "" {
		return nil, discoveryErr(codeDiscoveryFailed, "the authorization server names no token endpoint")
	}
	if as.RegistrationEndpoint == "" || !slices.Contains(as.CodeChallengeMethods, "S256") {
		return nil, discoveryErr(codeRegistrationUnsupp, "the server does not support the automatic registration of a client")
	}
	issuer := as.Issuer
	if issuer == "" {
		issuer = asBase
	}
	return &mcpOAuthMeta{Issuer: issuer, AuthorizeURL: as.AuthorizationEndpoint, TokenURL: as.TokenEndpoint,
		RegistrationURL: as.RegistrationEndpoint, Resource: resource, AuthMethods: as.AuthMethods, Scopes: res.ScopesSupported}, nil
}

// registerClient registers Nabu at the authorization server (tech §4.1, step 3).
func (s *Service) registerClient(ctx context.Context, meta *mcpOAuthMeta, redirect string) (clientID, secret string, err error) {
	method := authMethodNone
	if len(meta.AuthMethods) > 0 && !slices.Contains(meta.AuthMethods, authMethodNone) {
		method = authMethodClientSecretPo
	}
	body, _ := json.Marshal(map[string]any{"client_name": mcpOAuthClientName, "redirect_uris": []string{redirect},
		"grant_types": []string{"authorization_code", "refresh_token"}, "response_types": []string{"code"},
		"token_endpoint_auth_method": method})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, meta.RegistrationURL, bytes.NewReader(body))
	if err != nil {
		return "", "", discoveryErr(codeRegistrationFailed, "%v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := s.oauthClient().Do(req)
	if err != nil {
		return "", "", discoveryErr(codeRegistrationFailed, "the authorization server is not reachable: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, mcpOAuthMaxBody))
	var out struct {
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		Error        string `json:"error"`
		ErrorDesc    string `json:"error_description"`
	}
	_ = json.Unmarshal(b, &out)
	if resp.StatusCode/100 != 2 || out.ClientID == "" {
		return "", "", discoveryErr(codeRegistrationFailed, "HTTP %d %s %s", resp.StatusCode, out.Error, out.ErrorDesc)
	}
	return out.ClientID, out.ClientSecret, nil
}

// setupMCPOAuth discovers and registers the client of an item and stores the
// result in personal_auth; a failure is stored in personalAuth.error and the
// item is unpublished (R1). It returns the failure, if any.
func (s *Service) setupMCPOAuth(ctx context.Context, id uuid.UUID, mcpURL string) (*AuthError, error) {
	pa := PersonalAuth{Kind: KindMCPOAuth}
	var secretEnc []byte
	meta, err := s.discoverMCPOAuth(ctx, mcpURL)
	if err == nil {
		var clientID, secret string
		if clientID, secret, err = s.registerClient(ctx, meta, s.CallbackURL(id)); err == nil {
			now := time.Now().UTC()
			pa.Issuer, pa.AuthorizeURL, pa.TokenURL, pa.Resource = meta.Issuer, meta.AuthorizeURL, meta.TokenURL, meta.Resource
			pa.ClientID, pa.Scopes, pa.RegisteredAt = clientID, meta.Scopes, &now
			if secret != "" {
				if secretEnc, err = s.Box.Seal(secret); err != nil {
					return nil, err
				}
			}
		}
	}
	var failure *AuthError
	var de *discoveryError
	if errors.As(err, &de) {
		failure = &AuthError{Code: de.code, Detail: de.detail}
		pa.Error = failure
		metrics.MCPOAuthRegister.WithLabelValues(de.code).Inc()
	} else if err != nil {
		return nil, err
	} else {
		metrics.MCPOAuthRegister.WithLabelValues("ok").Inc()
	}
	b, _ := json.Marshal(pa)
	q := `UPDATE catalog_items SET personal_auth = $2, personal_auth_secret_enc = $3, updated_at = now() WHERE id = $1`
	if failure != nil {
		q = `UPDATE catalog_items SET personal_auth = $2, personal_auth_secret_enc = $3, published = false, updated_at = now() WHERE id = $1`
	}
	if _, err := s.Pool.Exec(ctx, q, id, b, secretEnc); err != nil {
		return nil, err
	}
	return failure, nil
}

// ReregisterResult is the answer of POST /catalog/{id}/oauth/register.
type ReregisterResult struct {
	ClientID          string     `json:"clientId"`
	RegisteredAt      *time.Time `json:"registeredAt"`
	DisconnectedUsers int        `json:"disconnectedUsers"`
}

// Reregister registers the client of an item again (R3): the tokens of the
// users were issued to the old client, so their connections are removed;
// the collection of each pauses with source_access at its next run.
func (s *Service) Reregister(ctx context.Context, id uuid.UUID) (*ReregisterResult, error) {
	it, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if it.PersonalAuth == nil || it.PersonalAuth.Kind != KindMCPOAuth {
		return nil, apperr.Conflict("mcp_oauth_only", "only an item with the OAuth of MCP registers its client")
	}
	// the previous registration stays when the new one fails: the users keep working
	meta, err := s.discoverMCPOAuth(ctx, it.Source.URL)
	var clientID, secret string
	if err == nil {
		clientID, secret, err = s.registerClient(ctx, meta, s.CallbackURL(id))
	}
	var de *discoveryError
	if errors.As(err, &de) {
		metrics.MCPOAuthRegister.WithLabelValues(de.code).Inc()
		if de.code == codeRegistrationFailed {
			return nil, apperr.New(http.StatusBadGateway, de.code, de.detail)
		}
		return nil, apperr.Unprocessable(de.code, de.detail)
	} else if err != nil {
		return nil, err
	}
	metrics.MCPOAuthRegister.WithLabelValues("ok").Inc()
	now := time.Now().UTC()
	pa := PersonalAuth{Kind: KindMCPOAuth, Issuer: meta.Issuer, AuthorizeURL: meta.AuthorizeURL, TokenURL: meta.TokenURL,
		Resource: meta.Resource, ClientID: clientID, Scopes: meta.Scopes, RegisteredAt: &now}
	var secretEnc []byte
	if secret != "" {
		if secretEnc, err = s.Box.Seal(secret); err != nil {
			return nil, err
		}
	}
	b, _ := json.Marshal(pa)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `UPDATE catalog_items SET personal_auth = $2, personal_auth_secret_enc = $3, updated_at = now() WHERE id = $1`, id, b, secretEnc); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `DELETE FROM user_connections WHERE item_id = $1`, id)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM oauth_states WHERE item_id = $1`, id); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	if s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: events.ConnectionsChanged, Data: map[string]any{"itemId": id}})
	}
	return &ReregisterResult{ClientID: clientID, RegisteredAt: &now, DisconnectedUsers: int(tag.RowsAffected())}, nil
}
