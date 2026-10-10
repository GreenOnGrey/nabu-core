// Package admin holds the Users section of the administration
// (FTR.NAB.CMN-0001 R33–R34, tech §4).
package admin

import (
	"context"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"

	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

// Users serves /users.
type Users struct {
	Repo *users.Repo
	// Channels and GroupAgents fill the card of a user (FTR.NAB.CMN-0002 tech §2.2).
	Channels    func(ctx context.Context, uid uuid.UUID) (any, error)
	GroupAgents func(ctx context.Context, uid uuid.UUID) (any, error)
}

// Routes mounts GET /users, POST /users (invitation) and PATCH /users/{id}.
func (u *Users) Routes(r chi.Router) {
	r.Get("/users", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 || limit > 500 {
			limit = 200
		}
		within, _ := strconv.Atoi(r.URL.Query().Get("purgeWithinDays"))
		l, err := u.Repo.List(r.Context(), r.URL.Query().Get("q"), r.URL.Query().Get("status"), within, limit)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Get("/users/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		usr, err := u.Repo.Get(r.Context(), id)
		if err != nil {
			return err
		}
		if usr == nil || usr.CreatedVia == "group" {
			return apperr.NotFound("not_found", "user not found")
		}
		out := map[string]any{"user": usr, "channels": []any{}, "groupAgents": []any{}}
		if u.Channels != nil {
			if out["channels"], err = u.Channels(r.Context(), id); err != nil {
				return err
			}
		}
		if u.GroupAgents != nil {
			if out["groupAgents"], err = u.GroupAgents(r.Context(), id); err != nil {
				return err
			}
		}
		httpx.JSON(w, 200, out)
		return nil
	}))
	r.Post("/users", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			Email   string `json:"email"`
			IsAdmin bool   `json:"isAdmin"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		usr, err := u.Repo.Invite(r.Context(), in.Email, in.IsAdmin)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, usr)
		return nil
	}))
	r.Patch("/users/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			IsAdmin *bool `json:"isAdmin"`
			Blocked *bool `json:"blocked"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		usr, err := u.Repo.AdminPatch(r.Context(), p.UserID, id, in.IsAdmin, in.Blocked)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, usr)
		return nil
	}))
}
