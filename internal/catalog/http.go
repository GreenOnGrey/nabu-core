package catalog

import (
	"context"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

func itemID(r *http.Request) (uuid.UUID, error) { return httpx.ParamUUID(r, "id") }

// Routes mounts the user's catalog (tech §3.5).
func (s *Service) Routes(r chi.Router) {
	r.Get("/catalog", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		items, err := s.ForUser(r.Context(), p.UserID)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": items})
		return nil
	}))
	r.Post("/catalog/{id}/connect", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := itemID(r)
		if err != nil {
			return err
		}
		var in struct {
			Token string `json:"token"`
		}
		if r.ContentLength != 0 {
			if err := httpx.Decode(r, &in); err != nil {
				return err
			}
		}
		res, err := s.Connect(r.Context(), p.UserID, id, in.Token)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, res)
		return nil
	}))
	r.Delete("/catalog/{id}/connect", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := itemID(r)
		if err != nil {
			return err
		}
		if err := s.Disconnect(r.Context(), p.UserID, id); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
}

// CallbackRoute mounts GET /api/v1/oauth/callback/{id}: the provider
// redirects the browser here; the state ties it to the user.
func (s *Service) CallbackRoute(r chi.Router) {
	r.Get("/oauth/callback/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		http.Redirect(w, r, s.Callback(r.Context(), id, q.Get("state"), q.Get("code"), q.Get("error")), http.StatusFound)
	})
}

// AdminRoutes mounts the catalog of the administration (tech §4).
func (s *Service) AdminRoutes(r chi.Router, hookKey []byte) {
	r.Get("/catalog", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		items, err := s.List(r.Context())
		if err != nil {
			return err
		}
		type adminItem struct {
			Item
			CallbackURL string `json:"callbackUrl,omitempty"`
			HookURL     string `json:"hookUrl,omitempty"`
			HookSecret  string `json:"hookSecret,omitempty"`
		}
		out := make([]adminItem, 0, len(items))
		for _, it := range items {
			a := adminItem{Item: it}
			if it.PersonalAuth != nil && it.PersonalAuth.Kind == "oauth" {
				a.CallbackURL = s.CallbackURL(it.ID)
			}
			if it.Type == "skill" && it.Source.Kind == "git" {
				a.HookURL = s.PublicAPIURL + "/hooks/v1/skills/" + it.ID.String()
				a.HookSecret = HookSecret(hookKey, it.ID)
			}
			out = append(out, a)
		}
		httpx.JSON(w, 200, map[string]any{"items": out})
		return nil
	}))
	r.Post("/catalog", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in Input
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		it, err := s.Save(r.Context(), nil, in)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, it)
		return nil
	}))
	r.Patch("/catalog/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := itemID(r)
		if err != nil {
			return err
		}
		var in Input
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		it, err := s.Save(r.Context(), &id, in)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, it)
		return nil
	}))
	r.Delete("/catalog/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := itemID(r)
		if err != nil {
			return err
		}
		if err := s.Delete(r.Context(), id); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Post("/catalog/{id}/check", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := itemID(r)
		if err != nil {
			return err
		}
		email := ""
		if p := httpx.PrincipalFrom(r.Context()); p != nil {
			email = p.Email
		}
		it, err := s.Check(r.Context(), id, email)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, it)
		return nil
	}))
	r.Post("/catalog/{id}/sync", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := itemID(r)
		if err != nil {
			return err
		}
		it, err := s.Sync(r.Context(), id)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, it)
		return nil
	}))
	r.Post("/catalog/{id}/upload", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := itemID(r)
		if err != nil {
			return err
		}
		it, err := s.Upload(r.Context(), id, http.MaxBytesReader(w, r.Body, 64<<20))
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, it)
		return nil
	}))
}

// HookRoute mounts POST /hooks/v1/skills/{id}: a push to a git source.
func (s *Service) HookRoute(r chi.Router, hookKey []byte) {
	r.Post("/hooks/v1/skills/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := itemID(r)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 5<<20))
		if err != nil {
			return err
		}
		if !VerifyHook(hookKey, id, body, r.Header.Get("X-Hub-Signature-256")) {
			return apperr.Unauthorized("invalid_signature", "invalid signature")
		}
		if r.Header.Get("X-GitHub-Event") == "ping" {
			httpx.NoContent(w)
			return nil
		}
		go func() { _, _ = s.Sync(context.WithoutCancel(r.Context()), id) }()
		w.WriteHeader(http.StatusAccepted)
		return nil
	}))
}
