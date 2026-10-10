package accounts

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

// AdminRoutes mounts the archive of the administration (tech §2.3).
func (s *Service) AdminRoutes(r chi.Router) {
	h := httpx.Handler
	actor := func(r *http.Request) (Actor, error) {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return Actor{}, err
		}
		id := p.UserID
		return Actor{UserID: &id, Email: p.Email}, nil
	}
	r.Post("/users:archive", h(func(w http.ResponseWriter, r *http.Request) error {
		a, err := actor(r)
		if err != nil {
			return err
		}
		var in struct {
			Emails []string `json:"emails"`
			ArchiveOptions
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		res, err := s.Archive(r.Context(), in.Emails, in.ArchiveOptions, a)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"results": res})
		return nil
	}))
	r.Post("/users/{id}:restore", h(func(w http.ResponseWriter, r *http.Request) error {
		a, err := actor(r)
		if err != nil {
			return err
		}
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			LinkIdentity *uuid.UUID `json:"linkIdentity"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		res, err := s.Restore(r.Context(), id, in.LinkIdentity, false, a)
		if err != nil {
			return err
		}
		if res == NotFound {
			return apperr.NotFound("not_found", "user not found")
		}
		httpx.JSON(w, 200, map[string]any{"result": res})
		return nil
	}))
	r.Get("/restore-requests", h(func(w http.ResponseWriter, r *http.Request) error {
		l, err := s.Requests(r.Context())
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Post("/restore-requests/{id}:reject", h(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		if err := s.RejectRequest(r.Context(), id); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Get("/settings/archive", h(func(w http.ResponseWriter, r *http.Request) error {
		httpx.JSON(w, 200, map[string]int{"retentionDays": s.Retention(r.Context())})
		return nil
	}))
	r.Put("/settings/archive", h(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			RetentionDays int `json:"retentionDays"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		dry := r.URL.Query().Get("dryRun") == "true"
		n, err := s.SetRetention(r.Context(), in.RetentionDays, dry)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"retentionDays": in.RetentionDays, "willPurge": n, "saved": !dry})
		return nil
	}))
}

// ClientRoutes mounts /users:archive and /users:restore of service clients
// (tech §4): separate rights users:archive and users:restore (AR-14).
func (s *Service) ClientRoutes(r chi.Router) {
	h := httpx.Handler
	actor := func(r *http.Request, right string) (Actor, error) {
		cl := httpx.ClientFrom(r.Context())
		if cl == nil {
			return Actor{}, apperr.Unauthorized("unauthenticated", "a client token is required")
		}
		if (right == "archive" && !cl.CanArchive) || (right == "restore" && !cl.CanRestore) {
			return Actor{}, apperr.Forbidden("forbidden", "the client has no right users:"+right)
		}
		id := cl.ID
		return Actor{ClientID: &id, Client: cl.Name}, nil
	}
	r.Post("/users:archive", h(func(w http.ResponseWriter, r *http.Request) error {
		a, err := actor(r, "archive")
		if err != nil {
			return err
		}
		var in struct {
			Emails    []string `json:"emails"`
			Initiator string   `json:"initiator"`
			ArchiveOptions
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		a.Initiator = in.Initiator
		res, err := s.Archive(r.Context(), in.Emails, in.ArchiveOptions, a)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"results": res})
		return nil
	}))
	r.Post("/users:restore", h(func(w http.ResponseWriter, r *http.Request) error {
		a, err := actor(r, "restore")
		if err != nil {
			return err
		}
		var in struct {
			Emails    []string `json:"emails"`
			Initiator string   `json:"initiator"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		a.Initiator = in.Initiator
		res, err := s.RestoreEmails(r.Context(), in.Emails, a)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"results": res})
		return nil
	}))
}
