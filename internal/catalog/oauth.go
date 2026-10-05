package catalog

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
)

// Personal OAuth (arch §8): Nabu is the OAuth client of Atlassian, Figma and
// others; tokens are stored encrypted and refreshed.

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// CallbackURL is the redirect URI to register at the provider.
func (s *Service) CallbackURL(id uuid.UUID) string {
	return s.PublicAPIURL + "/api/v1/oauth/callback/" + id.String()
}

func (s *Service) startOAuth(ctx context.Context, uid uuid.UUID, it *Item) (*ConnectResult, error) {
	pa := it.PersonalAuth
	state, verifier := randHex(16), randHex(32)
	if _, err := s.Pool.Exec(ctx, `INSERT INTO oauth_states (state, user_id, item_id, verifier, expires_at) VALUES ($1,$2,$3,$4,$5)`,
		state, uid, it.ID, verifier, time.Now().Add(10*time.Minute)); err != nil {
		return nil, err
	}
	h := sha256.Sum256([]byte(verifier))
	q := url.Values{"response_type": {"code"}, "client_id": {pa.ClientID}, "redirect_uri": {s.CallbackURL(it.ID)},
		"state": {state}, "code_challenge": {base64.RawURLEncoding.EncodeToString(h[:])}, "code_challenge_method": {"S256"}}
	if len(pa.Scopes) > 0 {
		q.Set("scope", strings.Join(pa.Scopes, " "))
	}
	sep := "?"
	if strings.Contains(pa.AuthorizeURL, "?") {
		sep = "&"
	}
	return &ConnectResult{AuthorizeURL: pa.AuthorizeURL + sep + q.Encode()}, nil
}

type oauthToken struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	Error        string `json:"error"`
	ErrorDesc    string `json:"error_description"`
}

func (s *Service) tokenRequest(ctx context.Context, id uuid.UUID, pa *PersonalAuth, form url.Values) (*oauthToken, error) {
	var secretEnc []byte
	_ = s.Pool.QueryRow(ctx, `SELECT personal_auth_secret_enc FROM catalog_items WHERE id = $1`, id).Scan(&secretEnc)
	form.Set("client_id", pa.ClientID)
	if len(secretEnc) > 0 {
		sec, err := s.Box.Open(secretEnc)
		if err != nil {
			return nil, err
		}
		form.Set("client_secret", sec)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pa.TokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var t oauthToken
	if err := json.Unmarshal(b, &t); err != nil || resp.StatusCode/100 != 2 || t.AccessToken == "" {
		return nil, fmt.Errorf("token endpoint %d: %s %s", resp.StatusCode, t.Error, t.ErrorDesc)
	}
	return &t, nil
}

func (s *Service) storeToken(ctx context.Context, uid, id uuid.UUID, t *oauthToken, keepRefresh []byte) error {
	acc, err := s.Box.Seal(t.AccessToken)
	if err != nil {
		return err
	}
	ref := keepRefresh
	if t.RefreshToken != "" {
		if ref, err = s.Box.Seal(t.RefreshToken); err != nil {
			return err
		}
	}
	var exp *time.Time
	if t.ExpiresIn > 0 {
		e := time.Now().Add(time.Duration(t.ExpiresIn) * time.Second)
		exp = &e
	}
	return s.saveConnection(ctx, uid, id, acc, ref, exp)
}

// Callback completes a personal OAuth connection and returns where the
// browser goes (the Connections page with the result).
func (s *Service) Callback(ctx context.Context, id uuid.UUID, state, code, oauthErr string) string {
	back := strings.TrimRight(s.WebURL, "/") + "/connections"
	var uid uuid.UUID
	var verifier string
	err := s.Pool.QueryRow(ctx, `DELETE FROM oauth_states WHERE state = $1 AND item_id = $2 AND expires_at > now() RETURNING user_id, verifier`, state, id).
		Scan(&uid, &verifier)
	if err != nil {
		return back + "?oauth=invalid_state"
	}
	if oauthErr != "" {
		return back + "?oauth=denied"
	}
	it, err := s.Get(ctx, id)
	if err != nil || it.PersonalAuth == nil {
		return back + "?oauth=failed"
	}
	t, err := s.tokenRequest(ctx, id, it.PersonalAuth, url.Values{"grant_type": {"authorization_code"}, "code": {code},
		"redirect_uri": {s.CallbackURL(id)}, "code_verifier": {verifier}})
	if err != nil {
		return back + "?oauth=failed"
	}
	if err := s.storeToken(ctx, uid, id, t, nil); err != nil {
		return back + "?oauth=failed"
	}
	return back + "?oauth=connected&item=" + url.QueryEscape(it.Name)
}

// userToken returns a valid personal credential, refreshing OAuth tokens.
func (s *Service) userToken(ctx context.Context, uid uuid.UUID, it *Item) (string, error) {
	var cred, ref []byte
	var exp *time.Time
	err := s.Pool.QueryRow(ctx, `SELECT credentials_enc, refresh_enc, expires_at FROM user_connections WHERE user_id = $1 AND item_id = $2`, uid, it.ID).
		Scan(&cred, &ref, &exp)
	if err != nil || len(cred) == 0 {
		return "", errNoCredential
	}
	if exp == nil || time.Until(*exp) > 2*time.Minute || len(ref) == 0 || it.PersonalAuth.Kind != "oauth" {
		return s.Box.Open(cred)
	}
	rt, err := s.Box.Open(ref)
	if err != nil {
		return "", err
	}
	t, err := s.tokenRequest(ctx, it.ID, it.PersonalAuth, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {rt}})
	if err != nil {
		return "", apperr.Unauthorized("reconnect_required", "the access expired; connect the item again")
	}
	if err := s.storeToken(ctx, uid, it.ID, t, ref); err != nil {
		return "", err
	}
	return t.AccessToken, nil
}
