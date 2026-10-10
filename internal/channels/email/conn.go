package email

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/emersion/go-imap/v2/imapclient"
)

// Credentials sign in to the mailbox: an app password (Yandex 360, VK
// WorkMail) or a Google service account with domain-wide delegation.
type Credentials struct {
	Settings    Settings
	AppPassword string
	Google      *GoogleToken
	// tlsConfig replaces the TLS configuration in tests of this package.
	tlsConfig *tls.Config
}

// ─── Google: a service account token for the mailbox (tech §5.1) ──────

// GoogleScope is the OAuth scope of IMAP and SMTP.
const GoogleScope = "https://mail.google.com/"

// GoogleToken issues access tokens of a service account acting as the mailbox.
type GoogleToken struct {
	Email    string // client_email
	Key      *rsa.PrivateKey
	TokenURI string
	Subject  string // the mailbox
	HTTP     *http.Client

	mu      sync.Mutex
	token   string
	expires time.Time
}

// NewGoogleToken reads the JSON key of a service account.
func NewGoogleToken(serviceAccountJSON, mailbox string) (*GoogleToken, error) {
	var sa struct {
		Type        string `json:"type"`
		ClientEmail string `json:"client_email"`
		PrivateKey  string `json:"private_key"`
		TokenURI    string `json:"token_uri"`
	}
	if err := json.Unmarshal([]byte(serviceAccountJSON), &sa); err != nil {
		return nil, errors.New("the key of the service account is not JSON")
	}
	block, _ := pem.Decode([]byte(sa.PrivateKey))
	if block == nil || sa.ClientEmail == "" {
		return nil, errors.New("the key of the service account has no private_key or client_email")
	}
	k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, errors.New("the private key of the service account is not PKCS#8")
	}
	rk, ok := k.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("the private key of the service account is not RSA")
	}
	if sa.TokenURI == "" {
		sa.TokenURI = "https://oauth2.googleapis.com/token"
	}
	return &GoogleToken{Email: sa.ClientEmail, Key: rk, TokenURI: sa.TokenURI, Subject: mailbox,
		HTTP: &http.Client{Timeout: 20 * time.Second}}, nil
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// Token returns an access token, refreshed 5 minutes before it expires.
func (g *GoogleToken) Token(ctx context.Context) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.token != "" && time.Until(g.expires) > 5*time.Minute {
		return g.token, nil
	}
	now := time.Now()
	hdr, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{"iss": g.Email, "sub": g.Subject, "scope": GoogleScope, "aud": g.TokenURI,
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	unsigned := b64(hdr) + "." + b64(claims)
	sum := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(nil, g.Key, crypto.SHA256, sum[:])
	if err != nil {
		return "", err
	}
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {unsigned + "." + b64(sig)}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.TokenURI, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := g.HTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("google token: %w", err)
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&out)
	if out.AccessToken == "" {
		return "", fmt.Errorf("google token: %d %s %s (is domain-wide delegation for %s granted?)", resp.StatusCode, out.Error, out.Description, GoogleScope)
	}
	g.token, g.expires = out.AccessToken, now.Add(time.Duration(out.ExpiresIn)*time.Second)
	return g.token, nil
}

// xoauth2 is the SASL XOAUTH2 mechanism of Google.
type xoauth2 struct{ user, token string }

func (x *xoauth2) Start() (string, []byte, error) {
	return "XOAUTH2", []byte("user=" + x.user + "\x01auth=Bearer " + x.token + "\x01\x01"), nil
}

func (x *xoauth2) Next([]byte) ([]byte, error) { return []byte{}, nil }

// ─── IMAP ───────────────────────────────────────────────────────────

// DialIMAP connects over TLS and signs in.
func (c Credentials) DialIMAP(ctx context.Context, handler *imapclient.UnilateralDataHandler) (*imapclient.Client, error) {
	addr := net.JoinHostPort(c.Settings.IMAP.Host, strconv.Itoa(c.Settings.IMAP.Port))
	tlsCfg := c.tlsConfig
	if tlsCfg == nil {
		tlsCfg = &tls.Config{ServerName: c.Settings.IMAP.Host, MinVersion: tls.VersionTLS12}
	}
	cl, err := imapclient.DialTLS(addr, &imapclient.Options{UnilateralDataHandler: handler, TLSConfig: tlsCfg})
	if err != nil {
		return nil, fmt.Errorf("imap %s: %w", addr, err)
	}
	if c.Google != nil {
		tok, err := c.Google.Token(ctx)
		if err != nil {
			cl.Close()
			return nil, err
		}
		if err := cl.Authenticate(&xoauth2{user: c.Settings.Mailbox, token: tok}); err != nil {
			cl.Close()
			return nil, fmt.Errorf("imap sign-in: %w", err)
		}
		return cl, nil
	}
	if err := cl.Login(c.Settings.Mailbox, c.AppPassword).Wait(); err != nil {
		cl.Close()
		return nil, fmt.Errorf("imap sign-in: %w", err)
	}
	return cl, nil
}

// ─── SMTP (tech §5.3) ───────────────────────────────────────────────

type xoauth2SMTP struct{ user, token string }

func (x xoauth2SMTP) Start(*smtp.ServerInfo) (string, []byte, error) {
	return "XOAUTH2", []byte("user=" + x.user + "\x01auth=Bearer " + x.token + "\x01\x01"), nil
}

func (x xoauth2SMTP) Next(_ []byte, more bool) ([]byte, error) {
	if more {
		return []byte{}, nil
	}
	return nil, nil
}

// SMTP sends one letter: implicit TLS on 465, STARTTLS otherwise.
func (c Credentials) SMTP(ctx context.Context, from string, to []string, msg []byte) error {
	s := c.Settings.SMTP
	addr := net.JoinHostPort(s.Host, strconv.Itoa(s.Port))
	tlsCfg := &tls.Config{ServerName: s.Host, MinVersion: tls.VersionTLS12}
	d := &net.Dialer{Timeout: 30 * time.Second}
	var conn net.Conn
	var err error
	if s.Port == 465 {
		conn, err = (&tls.Dialer{NetDialer: d, Config: tlsCfg}).DialContext(ctx, "tcp", addr)
	} else {
		conn, err = d.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return fmt.Errorf("smtp %s: %w", addr, err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(2 * time.Minute))
	}
	cl, err := smtp.NewClient(conn, s.Host)
	if err != nil {
		conn.Close()
		return fmt.Errorf("smtp %s: %w", addr, err)
	}
	defer cl.Close()
	if s.Port != 465 {
		if err := cl.StartTLS(tlsCfg); err != nil {
			return fmt.Errorf("smtp starttls: %w", err)
		}
	}
	auth := smtp.PlainAuth("", c.Settings.Mailbox, c.AppPassword, s.Host)
	if c.Google != nil {
		tok, err := c.Google.Token(ctx)
		if err != nil {
			return err
		}
		auth = xoauth2SMTP{user: c.Settings.Mailbox, token: tok}
	}
	if err := cl.Auth(auth); err != nil {
		return fmt.Errorf("smtp sign-in: %w", err)
	}
	if msg == nil { // the check of the channel: sign in only
		return cl.Quit()
	}
	if err := cl.Mail(from); err != nil {
		return err
	}
	for _, r := range to {
		if err := cl.Rcpt(r); err != nil {
			return err
		}
	}
	w, err := cl.Data()
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, bytes.NewReader(msg)); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return cl.Quit()
}

// Check signs in to IMAP and SMTP (POST /channels/email/check, ML-03, ML-04).
func (c Credentials) Check(ctx context.Context) error {
	if c.Settings.Mailbox == "" {
		return errors.New("the mailbox of the bot is not set")
	}
	cl, err := c.DialIMAP(ctx, nil)
	if err != nil {
		return err
	}
	_ = cl.Logout().Wait()
	cl.Close()
	return c.SMTP(ctx, "", nil, nil)
}
