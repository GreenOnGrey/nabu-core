package channels

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

// Checker checks the connection of a channel (tech §2.1: IMAP and SMTP,
// getMe, self/get) and returns the reason of a failure.
type Checker func(ctx context.Context) error

// API serves the channels of users and of the administration.
type API struct {
	Registry *Registry
	Keys     *Keys
	// Validators check the settings of a kind; Checkers its connection.
	Validators map[string]Validator
	Checkers   map[string]Checker
}

// MyChannel is a channel in the Connections of a user (design §3.6).
type MyChannel struct {
	Kind      string   `json:"kind"`
	Available bool     `json:"available"`
	Reason    string   `json:"reason"`
	Binding   *Binding `json:"binding,omitempty"`
	// KeyIssuedAt: a Telegram key was issued (it is never shown again).
	KeyIssuedAt *time.Time `json:"keyIssuedAt,omitempty"`
	// Address is the bot: @username of Telegram, the nick of VK Teams, the mailbox.
	Address string `json:"address,omitempty"`
}

func (a *API) address(ctx context.Context, kind string) string {
	var s struct {
		Username string `json:"username"`
		BotNick  string `json:"botNick"`
		Mailbox  string `json:"mailbox"`
	}
	if _, _, err := a.Registry.Config(ctx, kind, &s); err != nil {
		return ""
	}
	switch kind {
	case domain.ChannelTelegram:
		if s.Username != "" {
			return "@" + s.Username
		}
	case domain.ChannelVKTeams:
		return s.BotNick
	case domain.ChannelEmail:
		return s.Mailbox
	}
	return ""
}

// Mine lists the messenger and mail channels of a user (tech §3).
func (a *API) Mine(ctx context.Context, uid uuid.UUID) ([]MyChannel, error) {
	out := []MyChannel{}
	for _, k := range []string{domain.ChannelTelegram, domain.ChannelVKTeams, domain.ChannelEmail} {
		if !a.Registry.Enabled(ctx, k) {
			continue
		}
		ok, reason, err := a.Registry.Available(ctx, uid, k)
		if err != nil {
			return nil, err
		}
		c := MyChannel{Kind: k, Available: ok, Reason: reason, Address: a.address(ctx, k)}
		if c.Address == "" {
			continue // enabled but not connected yet
		}
		if k == domain.ChannelTelegram {
			c.Binding = a.Keys.BindingOf(ctx, uid)
			if has, at := a.Keys.HasKey(ctx, uid); has {
				c.KeyIssuedAt = &at
			}
		}
		out = append(out, c)
	}
	return out, nil
}

// Routes mounts /channels of the site.
func (a *API) Routes(r chi.Router) {
	r.Get("/channels", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		l, err := a.Mine(r.Context(), p.UserID)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	// TG-01: the key is returned once; a repeated call reissues it.
	r.Post("/channels/telegram/key", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		if !a.Registry.Open(r.Context(), p.UserID, domain.ChannelTelegram) {
			return ErrUnavailable()
		}
		key, err := a.Keys.Issue(r.Context(), p.UserID)
		if err != nil {
			return err
		}
		w.Header().Set("Cache-Control", "no-store")
		httpx.JSON(w, 200, map[string]any{"key": key, "bot": a.address(r.Context(), domain.ChannelTelegram)})
		return nil
	}))
	r.Delete("/channels/telegram/binding", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		if _, err := a.Keys.Unbind(r.Context(), p.UserID); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
}

// AdminRoutes mounts /channels and the channels of users (tech §2.1–2.2).
func (a *API) AdminRoutes(r chi.Router) {
	h := httpx.Handler
	r.Get("/channels", h(func(w http.ResponseWriter, r *http.Request) error {
		l, err := a.Registry.List(r.Context())
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Patch("/channels/{kind}", h(func(w http.ResponseWriter, r *http.Request) error {
		var in Patch
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		kind := chi.URLParam(r, "kind")
		c, err := a.Registry.Update(r.Context(), kind, in, a.Validators[kind])
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, c)
		return nil
	}))
	r.Post("/channels/{kind}/check", h(func(w http.ResponseWriter, r *http.Request) error {
		kind := chi.URLParam(r, "kind")
		check := a.Checkers[kind]
		if check == nil {
			return apperr.Unprocessable("not_checkable", "the channel has no connection to check")
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if err := check(ctx); err != nil {
			a.Registry.SetStatus(r.Context(), kind, "error", err.Error())
		} else {
			a.Registry.SetStatus(r.Context(), kind, "ok", "")
		}
		c, err := a.Registry.Get(r.Context(), kind)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, c)
		return nil
	}))
	r.Get("/channels/{kind}/users", h(func(w http.ResponseWriter, r *http.Request) error {
		l, err := a.Registry.Users(r.Context(), chi.URLParam(r, "kind"))
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Put("/channels/{kind}/users", h(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			Add    []uuid.UUID `json:"add"`
			Remove []uuid.UUID `json:"remove"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := a.Registry.SetUsers(r.Context(), chi.URLParam(r, "kind"), in.Add, in.Remove); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Put("/users/{id}/channels", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in map[string]bool
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := a.Registry.SetUserChannels(r.Context(), id, in); err != nil {
			return err
		}
		l, err := a.Registry.OfUser(r.Context(), id)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Delete("/users/{id}/channels/telegram/binding", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		if _, err := a.Keys.Unbind(r.Context(), id); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
}

// ValidateTelegram normalizes the settings of Telegram.
func ValidateTelegram(raw json.RawMessage) (json.RawMessage, error) {
	var s TelegramSettings
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, apperr.Unprocessable("invalid_settings", "the settings are not valid").With("field", "settings")
	}
	return json.Marshal(s)
}
