package agentpods

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

// Admin serves the Agents section of the administration (tech §5). It only
// reads the tables and marks pods for stopping: the manager in the worker
// has the rights in Kubernetes.
type Admin struct {
	Pool  *pgxpool.Pool
	Local bool
	// Audit records «Stop» of an administrator.
	Audit func(ctx context.Context, admin, owner uuid.UUID, title string)
}

// AdminRoutes mounts under /admin/api/v1.
func (a *Admin) AdminRoutes(r chi.Router) {
	r.Get("/agent-pods", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		o, err := Read(r.Context(), a.Pool, a.Local)
		if err != nil {
			return err
		}
		state, q := r.URL.Query().Get("state"), strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))
		if state != "" || q != "" {
			pods := o.Pods[:0]
			for _, p := range o.Pods {
				if (state == "" || p.State == state) && (q == "" || strings.Contains(strings.ToLower(p.Title), q)) {
					pods = append(pods, p)
				}
			}
			o.Pods = pods
		}
		httpx.JSON(w, http.StatusOK, o)
		return nil
	}))
	r.Delete("/agent-pods/{ownerId}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		owner, err := httpx.ParamUUID(r, "ownerId")
		if err != nil {
			return err
		}
		var title string
		_ = a.Pool.QueryRow(r.Context(), `SELECT COALESCE(g.chat_title, u.email) FROM users u
			LEFT JOIN group_agents g ON g.data_user_id = u.id WHERE u.id = $1`, owner).Scan(&title)
		switch err := RequestStop(r.Context(), a.Pool, owner, r.URL.Query().Get("force") == "true"); {
		case errors.Is(err, ErrNoPod):
			return apperr.NotFound("agent_pod_not_found", "the owner has no agent pod")
		case errors.Is(err, ErrBusy):
			return apperr.Conflict("agent_pod_busy", "the pod runs a turn; stop it with force")
		case err != nil:
			return err
		}
		if a.Audit != nil {
			a.Audit(r.Context(), p.UserID, owner, title)
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	}))
}
