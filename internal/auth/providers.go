// Package auth implements sign-in through the provider of the deployment —
// OIDC (authorization code with PKCE) or GitHub (OAuth with an organization
// restriction) — browser sessions with CSRF protection, and authentication
// of service clients with delegation (FTR.NAB.CMN-0001 R1, R1a; arch §7;
// tech §2). The same model is used by Hammurapi (FTR.HMR.CMN-0006 arch §3).
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/users"
)

// Provider is a sign-in provider.
type Provider interface {
	// Kind is oidc or github; Label is the name on the sign-in button.
	Kind() string
	Label() string
	// AuthURL is where the browser goes; verifier is the PKCE verifier.
	AuthURL(ctx context.Context, state, verifier, redirectURL string) (string, error)
	// Exchange completes the flow and returns the verified identity.
	Exchange(ctx context.Context, code, verifier, redirectURL string) (users.Identity, error)
}

// ErrDenied is a sign-in refused by the rules (no verified email, not a member).
type ErrDenied struct{ Reason string }

func (e *ErrDenied) Error() string { return "sign-in denied: " + e.Reason }

var httpClient = &http.Client{Timeout: 20 * time.Second}

func getJSON(ctx context.Context, u, bearer string, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return resp.StatusCode, fmt.Errorf("GET %s: %d %s", u, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return resp.StatusCode, json.Unmarshal(b, out)
}

func postForm(ctx context.Context, u string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("POST %s: %d %s", u, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return json.Unmarshal(b, out)
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	Error       string `json:"error"`
	ErrorDesc   string `json:"error_description"`
}

// ─── GitHub ─────────────────────────────────────────────────────────

// GitHub signs in with a GitHub OAuth App. The email is the primary verified
// address from /user/emails; membership of AllowedOrg is required when set.
type GitHub struct {
	BaseURL, APIURL  string // https://github.com, https://api.github.com
	ClientID, Secret string
	AllowedOrg       string
}

// Kind implements Provider.
func (g *GitHub) Kind() string { return "github" }

// Label implements Provider.
func (g *GitHub) Label() string { return "GitHub" }

// AuthURL implements Provider.
func (g *GitHub) AuthURL(_ context.Context, state, _, redirectURL string) (string, error) {
	q := url.Values{"client_id": {g.ClientID}, "redirect_uri": {redirectURL}, "state": {state},
		"scope": {"read:user user:email read:org"}, "allow_signup": {"false"}}
	return g.BaseURL + "/login/oauth/authorize?" + q.Encode(), nil
}

// Exchange implements Provider.
func (g *GitHub) Exchange(ctx context.Context, code, _, redirectURL string) (users.Identity, error) {
	var id users.Identity
	var tok tokenResponse
	if err := postForm(ctx, g.BaseURL+"/login/oauth/access_token", url.Values{"client_id": {g.ClientID},
		"client_secret": {g.Secret}, "code": {code}, "redirect_uri": {redirectURL}}, &tok); err != nil {
		return id, err
	}
	if tok.AccessToken == "" {
		return id, fmt.Errorf("github: %s %s", tok.Error, tok.ErrorDesc)
	}
	var u struct {
		ID        int64  `json:"id"`
		Login     string `json:"login"`
		Name      string `json:"name"`
		AvatarURL string `json:"avatar_url"`
	}
	if _, err := getJSON(ctx, g.APIURL+"/user", tok.AccessToken, &u); err != nil {
		return id, err
	}
	var emails []struct {
		Email    string `json:"email"`
		Primary  bool   `json:"primary"`
		Verified bool   `json:"verified"`
	}
	if _, err := getJSON(ctx, g.APIURL+"/user/emails", tok.AccessToken, &emails); err != nil {
		return id, err
	}
	for _, e := range emails {
		if e.Primary && e.Verified {
			id.Email = e.Email
		}
	}
	if id.Email == "" {
		return id, &ErrDenied{Reason: "no_verified_email"}
	}
	if g.AllowedOrg != "" {
		var m struct {
			State string `json:"state"`
		}
		status, err := getJSON(ctx, g.APIURL+"/user/memberships/orgs/"+url.PathEscape(g.AllowedOrg), tok.AccessToken, &m)
		if status == http.StatusNotFound || status == http.StatusForbidden || (err == nil && m.State != "active") {
			return id, &ErrDenied{Reason: "not_org_member"} // AUTH-04
		}
		if err != nil {
			return id, err
		}
	}
	name := u.Name
	if name == "" {
		name = u.Login
	}
	id.Issuer, id.Subject, id.Name, id.Avatar = "github", fmt.Sprint(u.ID), name, u.AvatarURL
	return id, nil
}

// ─── OIDC ───────────────────────────────────────────────────────────

// OIDC signs in with any OpenID Connect provider (Keycloak first): the
// authorization code flow with PKCE (S256). The identity comes from the
// userinfo endpoint over the access token received from the token endpoint;
// email_verified must be true (AUTH-02).
type OIDC struct {
	Issuer, ClientID, Secret, Scopes, Name string

	mu   sync.Mutex
	meta *oidcMeta
}

type oidcMeta struct {
	Authorization string `json:"authorization_endpoint"`
	Token         string `json:"token_endpoint"`
	UserInfo      string `json:"userinfo_endpoint"`
}

func (o *OIDC) discover(ctx context.Context) (*oidcMeta, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.meta != nil {
		return o.meta, nil
	}
	var m oidcMeta
	if _, err := getJSON(ctx, o.Issuer+"/.well-known/openid-configuration", "", &m); err != nil {
		return nil, err
	}
	if m.Authorization == "" || m.Token == "" || m.UserInfo == "" {
		return nil, errors.New("oidc: incomplete discovery document")
	}
	o.meta = &m
	return o.meta, nil
}

// Kind implements Provider.
func (o *OIDC) Kind() string { return "oidc" }

// Label implements Provider.
func (o *OIDC) Label() string { return o.Name }

// Challenge is the PKCE S256 challenge of a verifier.
func Challenge(verifier string) string {
	h := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h[:])
}

// AuthURL implements Provider.
func (o *OIDC) AuthURL(ctx context.Context, state, verifier, redirectURL string) (string, error) {
	m, err := o.discover(ctx)
	if err != nil {
		return "", err
	}
	q := url.Values{"response_type": {"code"}, "client_id": {o.ClientID}, "redirect_uri": {redirectURL},
		"scope": {o.Scopes}, "state": {state}, "code_challenge": {Challenge(verifier)}, "code_challenge_method": {"S256"}}
	sep := "?"
	if strings.Contains(m.Authorization, "?") {
		sep = "&"
	}
	return m.Authorization + sep + q.Encode(), nil
}

// UserInfo is the part of the userinfo answer Nabu uses.
type UserInfo struct {
	Subject       string `json:"sub"`
	Email         string `json:"email"`
	EmailVerified any    `json:"email_verified"`
	Name          string `json:"name"`
	PreferredName string `json:"preferred_username"`
	Picture       string `json:"picture"`
}

// Identity validates the userinfo: a subject and a verified email (AUTH-02).
func (u UserInfo) Identity(issuer string) (users.Identity, error) {
	verified := false
	switch v := u.EmailVerified.(type) {
	case bool:
		verified = v
	case string:
		verified = v == "true"
	}
	if u.Subject == "" {
		return users.Identity{}, errors.New("oidc: userinfo has no sub")
	}
	if u.Email == "" || !verified {
		return users.Identity{}, &ErrDenied{Reason: "email_not_verified"}
	}
	name := u.Name
	if name == "" {
		name = u.PreferredName
	}
	return users.Identity{Issuer: issuer, Subject: u.Subject, Email: u.Email, Name: name, Avatar: u.Picture}, nil
}

// Exchange implements Provider.
func (o *OIDC) Exchange(ctx context.Context, code, verifier, redirectURL string) (users.Identity, error) {
	m, err := o.discover(ctx)
	if err != nil {
		return users.Identity{}, err
	}
	var tok tokenResponse
	if err := postForm(ctx, m.Token, url.Values{"grant_type": {"authorization_code"}, "code": {code},
		"redirect_uri": {redirectURL}, "client_id": {o.ClientID}, "client_secret": {o.Secret}, "code_verifier": {verifier}}, &tok); err != nil {
		return users.Identity{}, err
	}
	if tok.AccessToken == "" {
		return users.Identity{}, fmt.Errorf("oidc: %s %s", tok.Error, tok.ErrorDesc)
	}
	var ui UserInfo
	if _, err := getJSON(ctx, m.UserInfo, tok.AccessToken, &ui); err != nil {
		return users.Identity{}, err
	}
	return ui.Identity(o.Issuer)
}
