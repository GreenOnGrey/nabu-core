package comm

import (
	"cmp"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

// adminSettings adds to the settings what the administrator needs to know
// about the search by meaning: whether the database can store vectors and
// whether the chosen model resolves.
type adminSettings struct {
	Settings
	VectorsAvailable bool `json:"vectorsAvailable"`
	Semantic         bool `json:"semantic"`
	// Defaults are the values of the environment that runTimeoutMinutes and batchThreads override (decision 52).
	Defaults collectDefaults `json:"defaults"`
}

type collectDefaults struct {
	RunTimeoutMinutes int `json:"runTimeoutMinutes"`
	BatchThreads      int `json:"batchThreads"`
}

func (s *Service) adminSettings(r *http.Request, st Settings) adminSettings {
	return adminSettings{Settings: st, VectorsAvailable: s.vectors.Load(), Semantic: s.Semantic(r.Context()),
		Defaults: collectDefaults{
			// a limit of the environment shorter than a whole minute is shown as the minute it starts (30s → 1)
			RunTimeoutMinutes: int((s.CollectDefaults.RunTimeout + time.Minute - 1) / time.Minute), BatchThreads: s.CollectDefaults.BatchThreads}}
}

// AdminRoutes mounts the administration of the memory of communications
// (R29, tech §4): the settings, the sources of the catalog and the usage.
// None of the routes reads notes, commitments, people or topics: the
// administrator does not see what is collected (R22).
func (s *Service) AdminRoutes(r chi.Router) {
	r.Get("/settings/comm", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		st, err := s.LoadSettings(r.Context())
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, s.adminSettings(r, st))
		return nil
	}))
	r.Patch("/settings/comm", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		var in SettingsPatch
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		st, err := s.SaveSettings(r.Context(), in)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, s.adminSettings(r, st))
		return nil
	}))
	r.Get("/comm/sources", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		rows, err := s.Pool.Query(r.Context(), `SELECT id, name, COALESCE(title,''), COALESCE(mode,''), adapter, comm_source, published
			FROM catalog_items WHERE type = 'mcp' ORDER BY name`)
		if err != nil {
			return err
		}
		defer rows.Close()
		type item struct {
			ID         uuid.UUID `json:"id"`
			Name       string    `json:"name"`
			Title      string    `json:"title"`
			Mode       string    `json:"mode"`
			Adapter    *string   `json:"adapter"`
			CommSource bool      `json:"commSource"`
			Published  bool      `json:"published"`
			Sources    []Source  `json:"commSources"`
		}
		items := []item{}
		for rows.Next() {
			var it item
			if err := rows.Scan(&it.ID, &it.Name, &it.Title, &it.Mode, &it.Adapter, &it.CommSource, &it.Published); err != nil {
				return err
			}
			it.Sources = []Source{}
			if it.Adapter != nil {
				srcs, _ := adapterSources(*it.Adapter)
				for _, src := range srcs {
					if src.Title == "" { // the mail adapter: the item names the mailbox
						src.Title = cmp.Or(it.Title, it.Name)
					}
					it.Sources = append(it.Sources, src)
				}
			}
			items = append(items, it)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": items, "adapters": Adapters()})
		return nil
	}))
	r.Patch("/comm/sources/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			Adapter    *string `json:"adapter"`
			CommSource *bool   `json:"commSource"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := s.SetSource(r.Context(), id, in.Adapter, in.CommSource); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Get("/comm/usage", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		first, err := usageMonth(r)
		if err != nil {
			return err
		}
		out, err := s.Usage(r.Context(), first, r.URL.Query().Get("broken") == "true")
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, out)
		return nil
	}))
	r.Get("/comm/usage/daily", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		first, err := usageMonth(r)
		if err != nil {
			return err
		}
		var user *uuid.UUID
		if v := r.URL.Query().Get("user"); v != "" {
			id, err := uuid.Parse(v)
			if err != nil {
				return apperr.BadRequest("invalid_user", "user is a UUID")
			}
			user = &id
		}
		days, err := s.UsageDaily(r.Context(), first, user)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"days": days})
		return nil
	}))
}
