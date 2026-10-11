package comm

import (
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
		httpx.JSON(w, 200, adminSettings{Settings: st, VectorsAvailable: s.vectors.Load(), Semantic: s.Semantic(r.Context())})
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
		httpx.JSON(w, 200, adminSettings{Settings: st, VectorsAvailable: s.vectors.Load(), Semantic: s.Semantic(r.Context())})
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
				it.Sources = append(it.Sources, srcs...)
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
		ctx := r.Context()
		month := time.Now().UTC()
		if v := r.URL.Query().Get("month"); v != "" {
			m, err := time.Parse("2006-01", v)
			if err != nil {
				return apperr.BadRequest("invalid_month", "month is YYYY-MM")
			}
			month = m
		}
		first := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
		st, err := s.LoadSettings(ctx)
		if err != nil {
			return err
		}
		out := map[string]any{"month": first.Format("2006-01"), "monthlyLimitUsd": st.MonthlyLimitUSD}
		var enabled, notes int
		var cost float64
		if err := s.Pool.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM scheduled_tasks WHERE kind = 'memory_collect' AND status IN ('active','paused')),
				(SELECT COALESCE(sum(notes_count), 0) FROM comm_settings),
				(SELECT COALESCE(sum(cost_usd), 0)::float8 FROM comm_usage WHERE month = $1)`, first).Scan(&enabled, &notes, &cost); err != nil {
			return err
		}
		out["enabledUsers"], out["notes"], out["costUsd"] = enabled, notes, cost
		rows, err := s.Pool.Query(ctx, `SELECT u.id, u.email, cs.notes_count, COALESCE(cu.runs, 0), COALESCE(cu.cost_usd, 0)::float8
			FROM comm_settings cs JOIN users u ON u.id = cs.user_id
			LEFT JOIN comm_usage cu ON cu.user_id = cs.user_id AND cu.month = $1
			WHERE cs.notes_count > 0 OR cu.runs > 0 ORDER BY 5 DESC, 3 DESC, u.email LIMIT 500`, first)
		if err != nil {
			return err
		}
		defer rows.Close()
		type row struct {
			UserID       uuid.UUID `json:"userId"`
			Email        string    `json:"email"`
			Notes        int       `json:"notes"`
			Runs         int       `json:"runs"`
			CostUSD      float64   `json:"costUsd"`
			LimitUsedPct int       `json:"limitUsedPct"`
		}
		items := []row{}
		for rows.Next() {
			var x row
			if err := rows.Scan(&x.UserID, &x.Email, &x.Notes, &x.Runs, &x.CostUSD); err != nil {
				return err
			}
			if st.MonthlyLimitUSD > 0 {
				x.LimitUsedPct = int(x.CostUSD / st.MonthlyLimitUSD * 100)
			}
			items = append(items, x)
		}
		out["items"] = items
		httpx.JSON(w, 200, out)
		return rows.Err()
	}))
}
