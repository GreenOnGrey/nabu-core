package comm

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

// SourceState is a source of communications as the user sees it (tech §3.1).
type SourceState struct {
	ItemID    uuid.UUID `json:"itemId"`
	Source    string    `json:"source"`
	Title     string    `json:"title"`
	Connected bool      `json:"connected"`
	Enabled   bool      `json:"enabled"`
}

// CollectState is the state of the collection of a user.
type CollectState struct {
	Status        string     `json:"status"` // off | active | paused
	PauseKind     *string    `json:"pauseKind"`
	NextRunAt     *time.Time `json:"nextRunAt"`
	Backfill      *Progress  `json:"backfill"`
	BudgetUsedPct int        `json:"budgetUsedPct"`
}

// Progress is the progress of the first collection (R15).
type Progress struct {
	Done  int `json:"done"`
	Total int `json:"total"`
}

// API serves /comm and /commitments (tech §3).
type API struct {
	Store *Service
	// Sources and Collect come from the memory tasks; nil while there are none.
	Sources func(ctx context.Context, uid uuid.UUID) ([]SourceState, error)
	Collect func(ctx context.Context, uid uuid.UUID) (CollectState, error)
	// Timezone is the time zone of the user: "today" of the commitments.
	Timezone func(ctx context.Context, uid uuid.UUID) *time.Location
	// Semantic reports whether the search by meaning works (tech §3.1).
	Semantic func(ctx context.Context) bool
	// OnDeleteAll switches the memory tasks of the user off (R24).
	OnDeleteAll func(ctx context.Context, uid uuid.UUID)
	// GraphMaxNodes is MEMORY_GRAPH_MAX_NODES.
	GraphMaxNodes int
}

func (a *API) today(ctx context.Context, uid uuid.UUID) time.Time {
	loc := time.UTC
	if a.Timezone != nil {
		if l := a.Timezone(ctx, uid); l != nil {
			loc = l
		}
	}
	return a.Store.clock().In(loc)
}

// user wraps a handler of a signed-in user who may see the memory of
// communications: the function is allowed, or it was switched off after the
// user had consented — what is collected stays available to the user until
// its retention ends and can be deleted (R24).
func (a *API) user(fn func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error) http.HandlerFunc {
	return httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		if _, err := a.Store.Allowed(r.Context()); err != nil {
			if ok, cerr := a.Store.Consented(r.Context(), p.UserID); cerr != nil || !ok {
				return err
			}
		}
		return fn(w, r, p.UserID)
	})
}

// allowed wraps a handler that needs the function allowed now: the consent.
func (a *API) allowed(fn func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error) http.HandlerFunc {
	return httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		if _, err := a.Store.Allowed(r.Context()); err != nil {
			return err
		}
		return fn(w, r, p.UserID)
	})
}

func queryUUID(r *http.Request, name string) (*uuid.UUID, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return nil, nil
	}
	id, err := uuid.Parse(v)
	if err != nil {
		return nil, apperr.BadRequest("invalid_"+name, name+" is not an id")
	}
	return &id, nil
}

func queryTime(r *http.Request, name string) (*time.Time, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return nil, nil
	}
	for _, layout := range []string{time.RFC3339, dateLayout} {
		if t, err := time.Parse(layout, v); err == nil {
			return &t, nil
		}
	}
	return nil, apperr.BadRequest("invalid_"+name, name+" is a date or an RFC 3339 time")
}

func parseFilter(r *http.Request) (Filter, error) {
	f := Filter{Query: r.URL.Query().Get("q"), Source: r.URL.Query().Get("source")}
	var err error
	if f.Person, err = queryUUID(r, "person"); err != nil {
		return f, err
	}
	if f.Topic, err = queryUUID(r, "topic"); err != nil {
		return f, err
	}
	if f.From, err = queryTime(r, "from"); err != nil {
		return f, err
	}
	f.To, err = queryTime(r, "to")
	return f, err
}

// Routes mounts the API of the memory of communications.
func (a *API) Routes(r chi.Router) {
	// The state answers even when the function is not allowed: the interface
	// learns from it that there are no tabs to show (R21).
	r.Get("/comm/state", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		ctx := r.Context()
		st, err := a.Store.LoadSettings(ctx)
		if err != nil {
			return err
		}
		out := map[string]any{"allowed": st.Enabled, "available": st.Enabled, "retentionDays": st.RetentionDays, "semantic": false,
			"sources": []SourceState{}, "collect": CollectState{Status: "off"}, "consentAt": nil, "counts": map[string]int{"notes": 0, "openCommitments": 0}}
		us, err := a.Store.State(ctx, p.UserID)
		if err != nil {
			return err
		}
		// available: the tabs are shown — the function is allowed, or it was
		// switched off and the user has something collected to see (R24)
		out["available"] = st.Enabled || us.ConsentAt != nil
		if !st.Enabled && us.ConsentAt == nil {
			httpx.JSON(w, 200, out)
			return nil
		}
		out["consentAt"] = us.ConsentAt
		out["counts"] = map[string]int{"notes": us.Notes, "openCommitments": us.OpenCommitments}
		if !st.Enabled {
			httpx.JSON(w, 200, out)
			return nil
		}
		if a.Semantic != nil {
			out["semantic"] = a.Semantic(ctx)
		}
		if a.Sources != nil {
			src, err := a.Sources(ctx, p.UserID)
			if err != nil {
				return err
			}
			out["sources"] = src
		}
		if a.Collect != nil {
			c, err := a.Collect(ctx, p.UserID)
			if err != nil {
				return err
			}
			out["collect"] = c
		}
		httpx.JSON(w, 200, out)
		return nil
	}))
	r.Put("/comm/consent", a.allowed(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		var in struct {
			Accepted bool `json:"accepted"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if !in.Accepted {
			return apperr.Unprocessable("consent_required", "accepted must be true").With("field", "accepted")
		}
		if err := a.Store.Consent(r.Context(), uid); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Delete("/comm", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		var in struct {
			Confirm string `json:"confirm"`
		}
		if r.ContentLength != 0 {
			if err := httpx.Decode(r, &in); err != nil {
				return err
			}
		}
		if err := a.Store.DeleteAll(r.Context(), uid, in.Confirm); err != nil {
			return err
		}
		if a.OnDeleteAll != nil {
			a.OnDeleteAll(r.Context(), uid)
		}
		httpx.NoContent(w)
		return nil
	}))

	r.Get("/comm/notes", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		f, err := parseFilter(r)
		if err != nil {
			return err
		}
		page, err := httpx.ParsePage(r)
		if err != nil {
			return err
		}
		l, err := a.Store.List(r.Context(), uid, f, page)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, l)
		return nil
	}))
	r.Delete("/comm/notes", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		kind := KindPerson
		tag, err := queryUUID(r, "person")
		if err != nil {
			return err
		}
		if tag == nil {
			kind = KindTopic
			if tag, err = queryUUID(r, "topic"); err != nil {
				return err
			}
		}
		if tag == nil {
			return apperr.BadRequest("invalid_filter", "name the person or the topic whose notes to delete")
		}
		var in struct {
			Confirm string `json:"confirm"`
		}
		if r.ContentLength != 0 {
			if err := httpx.Decode(r, &in); err != nil {
				return err
			}
		}
		n, err := a.Store.DeleteByTag(r.Context(), uid, kind, *tag, in.Confirm)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]int{"deleted": n})
		return nil
	}))
	r.Get("/comm/notes/{id}", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		n, err := a.Store.Get(r.Context(), uid, id)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, n)
		return nil
	}))
	r.Patch("/comm/notes/{id}", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in NotePatch
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		n, err := a.Store.Update(r.Context(), uid, id, in)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, n)
		return nil
	}))
	r.Delete("/comm/notes/{id}", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		if err := a.Store.Delete(r.Context(), uid, id); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Get("/comm/graph", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		f, err := parseFilter(r)
		if err != nil {
			return err
		}
		min := 0
		if v := r.URL.Query().Get("minLinks"); v != "" {
			if err := json.Unmarshal([]byte(v), &min); err != nil || min < 0 {
				return apperr.BadRequest("invalid_min_links", "minLinks is a non-negative integer")
			}
		}
		g, err := a.Store.Graph(r.Context(), uid, f, min, a.GraphMaxNodes)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, g)
		return nil
	}))
	tags := func(kind string) http.HandlerFunc {
		return a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
			l, err := a.Store.Tags(r.Context(), uid, kind, r.URL.Query().Get("q"), 0)
			if err != nil {
				return err
			}
			httpx.JSON(w, 200, map[string]any{"items": l})
			return nil
		})
	}
	r.Get("/comm/people", tags(KindPerson))
	r.Get("/comm/topics", tags(KindTopic))
	r.Patch("/comm/topics/{id}", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			Name string `json:"name"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := a.Store.RenameTopic(r.Context(), uid, id, in.Name); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Post("/comm/topics/{id}/merge", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			Into uuid.UUID `json:"into"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		if err := a.Store.MergeTopic(r.Context(), uid, id, in.Into); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Delete("/comm/topics/{id}", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		if err := a.Store.DeleteTopic(r.Context(), uid, id); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))

	r.Get("/commitments", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		f := CommitmentFilter{State: r.URL.Query().Get("state"), Direction: r.URL.Query().Get("direction"), Today: a.today(r.Context(), uid)}
		var err error
		if f.Person, err = queryUUID(r, "person"); err != nil {
			return err
		}
		if f.Topic, err = queryUUID(r, "topic"); err != nil {
			return err
		}
		l, err := a.Store.Commitments(r.Context(), uid, f, 0)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, map[string]any{"items": l})
		return nil
	}))
	r.Patch("/commitments/{id}", a.user(func(w http.ResponseWriter, r *http.Request, uid uuid.UUID) error {
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			State *string         `json:"state"`
			Due   json.RawMessage `json:"due"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		p := CommitmentPatch{State: in.State}
		if len(in.Due) > 0 {
			p.SetDue = true
			var v *string
			if err := json.Unmarshal(in.Due, &v); err != nil {
				return apperr.Unprocessable("invalid_due", "due is a date or null").With("field", "due")
			}
			if v != nil {
				d, err := ParseDate(*v)
				if err != nil {
					return apperr.Unprocessable("invalid_due", "due is a date like 2026-10-16").With("field", "due")
				}
				p.Due = &d
			}
		}
		c, err := a.Store.UpdateCommitment(r.Context(), uid, id, p, ByUser)
		if err != nil {
			return err
		}
		c.Overdue = c.State == Open && c.Due != nil && c.Due.Format(dateLayout) < a.today(r.Context(), uid).Format(dateLayout)
		httpx.JSON(w, 200, c)
		return nil
	}))
}
