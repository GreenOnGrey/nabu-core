package auth

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/logging"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

// Cookie names. They differ from Hammurapi's: both products share the base
// domain on the stand.
const (
	SessionCookie = "nabu_session"
	CSRFCookie    = "nabu_csrf"
	stateCookie   = "nabu_oauth"
	// DelegationHeader carries the email of the user a client acts for (R1a).
	DelegationHeader = "Nabu-On-Behalf-Of"
)

// Handlers serves sign-in and authenticates requests.
type Handlers struct {
	Provider        Provider
	Users           *users.Repo
	PublicAPIURL    string
	WebURL          string // where the browser returns after sign-in
	CookieDomain    string
	Secure          bool
	BootstrapAdmins map[string]bool
	// OnSignIn runs after a successful sign-in (statistics, logs).
	OnSignIn func(ctx context.Context, u *users.User, created bool)
	// OnArchived records the sign-in of an archived user as a restore request
	// (FTR.NAB.CMN-0002 R19, AR-05); id is set when the identity is new.
	OnArchived func(ctx context.Context, u *users.User, id *users.Identity)
}

// RedirectURL is the callback registered at the provider.
func (h *Handlers) RedirectURL() string { return h.PublicAPIURL + "/auth/callback" }

// Routes mounts /auth/login, /auth/callback and /auth/logout.
func (h *Handlers) Routes(r chi.Router) {
	r.Get("/auth/login", h.login)
	r.Get("/auth/callback", h.callback)
	r.With(h.Authenticate).Post("/auth/logout", httpx.Handler(h.logout))
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (h *Handlers) web(path string) string { return strings.TrimRight(h.WebURL, "/") + path }

func (h *Handlers) login(w http.ResponseWriter, r *http.Request) {
	state, verifier := randomHex(16), randomHex(32)
	u, err := h.Provider.AuthURL(r.Context(), state, verifier, h.RedirectURL())
	if err != nil {
		slog.ErrorContext(r.Context(), "sign-in provider unavailable", "err", err)
		http.Redirect(w, r, h.web("/login?error=provider"), http.StatusFound)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: stateCookie, Value: state + "." + verifier, Path: "/auth", HttpOnly: true,
		Secure: h.Secure, SameSite: http.SameSiteLaxMode, MaxAge: 600})
	http.Redirect(w, r, u, http.StatusFound)
}

func (h *Handlers) callback(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie(stateCookie)
	q := r.URL.Query()
	state, verifier, _ := strings.Cut(func() string {
		if c != nil {
			return c.Value
		}
		return ""
	}(), ".")
	if err != nil || q.Get("state") == "" || subtle.ConstantTimeCompare([]byte(state), []byte(q.Get("state"))) != 1 {
		http.Redirect(w, r, h.web("/login?error=state"), http.StatusFound)
		return
	}
	http.SetCookie(w, &http.Cookie{Name: stateCookie, Path: "/auth", MaxAge: -1})
	if q.Get("error") != "" {
		http.Redirect(w, r, h.web("/login?error=denied"), http.StatusFound)
		return
	}
	id, err := h.Provider.Exchange(r.Context(), q.Get("code"), verifier, h.RedirectURL())
	if err != nil {
		var d *ErrDenied
		if errors.As(err, &d) {
			slog.InfoContext(r.Context(), "sign-in denied", "reason", d.Reason)
			http.Redirect(w, r, h.web("/login?error="+d.Reason), http.StatusFound)
			return
		}
		slog.ErrorContext(r.Context(), "sign-in failed", "err", err)
		http.Redirect(w, r, h.web("/login?error=failed"), http.StatusFound)
		return
	}
	u, created, err := h.Users.SignIn(r.Context(), id, h.BootstrapAdmins[domain.NormalizeEmail(id.Email)])
	if err != nil {
		slog.ErrorContext(r.Context(), "sign-in failed", "err", err)
		http.Redirect(w, r, h.web("/login?error=failed"), http.StatusFound)
		return
	}
	if u.Status == "blocked" {
		http.Redirect(w, r, h.web("/login?error=blocked"), http.StatusFound)
		return
	}
	if u.Status == "archived" { // the sign-in stays closed until an administrator restores the account
		if h.OnArchived != nil {
			var ni *users.Identity
			if u.NewIdentity {
				ni = &id
			}
			h.OnArchived(r.Context(), u, ni)
		}
		slog.InfoContext(r.Context(), "sign-in of an archived account", "user_id", u.ID, "new_identity", u.NewIdentity)
		http.Redirect(w, r, h.web("/login?error=restore_pending"), http.StatusFound)
		return
	}
	if created {
		slog.InfoContext(r.Context(), "user created", "user_id", u.ID, "admin", u.IsAdmin)
	}
	if h.OnSignIn != nil {
		h.OnSignIn(r.Context(), u, created)
	}
	csrf := randomHex(32)
	sid, err := h.Users.CreateSession(r.Context(), u.ID, csrf)
	if err != nil {
		slog.ErrorContext(r.Context(), "session", "err", err)
		http.Redirect(w, r, h.web("/login?error=failed"), http.StatusFound)
		return
	}
	h.setSessionCookies(w, sid, csrf)
	http.Redirect(w, r, h.web("/"), http.StatusFound)
}

func (h *Handlers) setSessionCookies(w http.ResponseWriter, sid uuid.UUID, csrf string) {
	maxAge := int(users.SessionTTL.Seconds())
	http.SetCookie(w, &http.Cookie{Name: SessionCookie, Value: sid.String(), Path: "/", Domain: h.CookieDomain, HttpOnly: true,
		Secure: h.Secure, SameSite: http.SameSiteLaxMode, MaxAge: maxAge})
	http.SetCookie(w, &http.Cookie{Name: CSRFCookie, Value: csrf, Path: "/", Domain: h.CookieDomain,
		Secure: h.Secure, SameSite: http.SameSiteLaxMode, MaxAge: maxAge})
}

func (h *Handlers) logout(w http.ResponseWriter, r *http.Request) error {
	if s := httpx.SessionFrom(r.Context()); s != nil {
		if !validCSRF(r, s.CSRFToken) {
			return apperr.ErrCSRF
		}
		if err := h.Users.DeleteSession(r.Context(), s.ID); err != nil {
			return err
		}
	}
	http.SetCookie(w, &http.Cookie{Name: SessionCookie, Path: "/", Domain: h.CookieDomain, MaxAge: -1})
	http.SetCookie(w, &http.Cookie{Name: CSRFCookie, Path: "/", Domain: h.CookieDomain, MaxAge: -1})
	httpx.NoContent(w)
	return nil
}

// Authenticate loads the session and principal when a session cookie is
// present. It never rejects; RequireSession does.
func (h *Handlers) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(SessionCookie)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		sid, err := uuid.Parse(c.Value)
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		ctx := r.Context()
		sess, err := h.Users.Session(ctx, sid)
		if err != nil {
			httpx.Error(w, r, err)
			return
		}
		if sess == nil {
			next.ServeHTTP(w, r)
			return
		}
		p, err := h.Users.Principal(ctx, sess.UserID)
		if err != nil {
			httpx.Error(w, r, err)
			return
		}
		if p == nil {
			next.ServeHTTP(w, r)
			return
		}
		if time.Since(sess.LastSeenAt) > 5*time.Minute {
			_ = h.Users.TouchSession(ctx, sess.ID)
			h.Users.Touch(ctx, p.UserID)
		}
		httpx.SetUserForLog(r, p.UserID.String())
		ctx = httpx.WithSession(ctx, &httpx.Session{ID: sess.ID, UserID: sess.UserID, CSRFToken: sess.CSRFToken})
		ctx = httpx.WithPrincipal(ctx, p)
		ctx = logging.With(ctx, slog.String("user_id", p.UserID.String()))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireSession rejects requests without a session and enforces the CSRF
// double-submit token on state-changing methods.
func RequireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s := httpx.SessionFrom(r.Context())
		if s == nil {
			httpx.Error(w, r, apperr.ErrNoSession)
			return
		}
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
			if !validCSRF(r, s.CSRFToken) {
				httpx.Error(w, r, apperr.ErrCSRF)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// RequireActive rejects blocked users (AUTH-11: the site answers user_blocked).
func RequireActive(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p := httpx.PrincipalFrom(r.Context()); p != nil && p.Blocked {
			httpx.Error(w, r, users.ErrBlocked)
			return
		} else if p != nil && p.Archived {
			httpx.Error(w, r, users.ErrArchived)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RequireAdmin admits platform administrators only (R33).
func RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := httpx.PrincipalFrom(r.Context())
		if p == nil || !p.IsAdmin || p.Blocked || p.Archived {
			httpx.Error(w, r, apperr.Forbidden("forbidden", "the platform administrator role is required"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func validCSRF(r *http.Request, sessionToken string) bool {
	hdr := strings.TrimSpace(r.Header.Get("X-CSRF-Token"))
	c, err := r.Cookie(CSRFCookie)
	if hdr == "" || err != nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(hdr), []byte(c.Value)) == 1 &&
		subtle.ConstantTimeCompare([]byte(hdr), []byte(sessionToken)) == 1
}

// ─── service clients ────────────────────────────────────────────────

// ClientLoader loads an enabled service client by id.
type ClientLoader func(ctx context.Context, id uuid.UUID) (*httpx.Client, error)

// ClientAuth authenticates /client/v1 with a client token of /oauth/token.
func ClientAuth(signer *jwt.Signer, load ClientLoader) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := signer.Verify(httpx.Bearer(r), jwt.AudClient)
			id, perr := uuid.Parse(strings.TrimPrefix(c.Subject, "client:"))
			if err != nil || perr != nil {
				httpx.Error(w, r, apperr.Unauthorized("unauthenticated", "a valid client token is required"))
				return
			}
			cl, err := load(r.Context(), id)
			if err != nil {
				httpx.Error(w, r, err)
				return
			}
			if cl == nil {
				httpx.Error(w, r, apperr.Unauthorized("unauthenticated", "the client is unknown or disabled"))
				return
			}
			ctx := logging.With(httpx.WithClient(r.Context(), cl), slog.String("client", cl.Name))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// Delegation turns Nabu-On-Behalf-Of into the principal of the user: only for
// clients with the delegation right (HMR-02); a user without an account is
// created (HMR-03); blocked users are refused. An archived user gets
// user_archived and a user whose channel of the client is closed —
// channel_unavailable (FTR.NAB.CMN-0002 tech §4, CH-08).
func Delegation(repo *users.Repo, open func(ctx context.Context, uid uuid.UUID, channel string) bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cl := httpx.ClientFrom(r.Context())
			email := strings.TrimSpace(r.Header.Get(DelegationHeader))
			if cl == nil {
				httpx.Error(w, r, apperr.Unauthorized("unauthenticated", "a client token is required"))
				return
			}
			if !cl.CanDelegate {
				httpx.Error(w, r, apperr.Forbidden("delegation_not_allowed", "the client may not act on behalf of users"))
				return
			}
			if email == "" {
				httpx.Error(w, r, apperr.BadRequest("delegation_required", "the "+DelegationHeader+" header is required"))
				return
			}
			u, err := repo.EnsureByEmail(r.Context(), email)
			if err != nil {
				httpx.Error(w, r, err)
				return
			}
			if u.Status == "blocked" {
				httpx.Error(w, r, users.ErrBlocked)
				return
			}
			if u.Status == "archived" {
				httpx.Error(w, r, users.ErrArchived)
				return
			}
			if ch := domain.ChannelOf("client:" + cl.Name); ch != "" && open != nil && !open(r.Context(), u.ID, ch) {
				httpx.Error(w, r, apperr.Forbidden("channel_unavailable", "the channel is not available for this account"))
				return
			}
			id := cl.ID
			p := &domain.Principal{UserID: u.ID, Email: u.Email, Name: u.Name, IsAdmin: false, ClientID: &id, ClientName: cl.Name}
			ctx := httpx.WithPrincipal(r.Context(), p)
			ctx = logging.With(ctx, slog.String("user_id", u.ID.String()))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
