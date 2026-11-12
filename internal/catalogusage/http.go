package catalogusage

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/access"
	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// Routes mounts the usage tab of the administration under /admin/api/v1
// (FTR.NAB.CHAT-0005 R1, R26; the role and the CSRF are checked by the
// middleware of the admin routes).
func (s *Service) Routes(r chi.Router) {
	r.Get("/catalog/usage", observed("/catalog/usage", httpx.Handler(s.report)))
	r.Get("/catalog/usage/summary", observed("/catalog/usage/summary", httpx.Handler(s.summary)))
	r.Get("/catalog/usage/items/{id}", observed("/catalog/usage/items/{id}", httpx.Handler(s.itemCard)))
	r.Get("/catalog/usage/items/{id}/tools", observed("/catalog/usage/items/{id}/tools", httpx.Handler(s.tools)))
	r.Get("/catalog/usage/candidates", observed("/catalog/usage/candidates", httpx.Handler(s.candidates)))
	r.Get("/catalog/usage/pins", observed("/catalog/usage/pins", httpx.Handler(s.pins)))
	r.Get("/catalog/usage/export.csv", observed("/catalog/usage/export.csv", httpx.Handler(s.export)))
	r.Post("/catalog/{id}/unpublish:impact", observed("/catalog/{id}/unpublish:impact", httpx.Handler(s.impactPreview)))
	r.Post("/catalog/{id}/unpublish", observed("/catalog/{id}/unpublish", httpx.Handler(s.unpublish)))
	r.Post("/catalog/{id}/publish", observed("/catalog/{id}/publish", httpx.Handler(s.publish)))
	r.Post("/catalog/{id}/grants:impact", observed("/catalog/{id}/grants:impact", httpx.Handler(s.grantImpact)))
	r.Delete("/catalog/{id}/grants/{groupId}", observed("/catalog/{id}/grants/{groupId}", httpx.Handler(s.revokeGrant)))
	r.Post("/catalog/{id}/pins", observed("/catalog/{id}/pins", httpx.Handler(s.pin)))
	r.Delete("/catalog/{id}/pins/{pinId}", observed("/catalog/{id}/pins/{pinId}", httpx.Handler(s.releasePin)))
}

// observed counts the requests of the report by path and code (R1, R26).
func observed(path string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, code: 200}
		next(rec, r)
		metrics.CatalogUsageReportRequests.WithLabelValues(path, strconv.Itoa(rec.code)).Inc()
	}
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}

// filter reads the query of the table (tech §«API»).
func (s *Service) filter(r *http.Request) (Filter, error) {
	var f Filter
	q := r.URL.Query()
	from, err := ParseDay(q.Get("from"))
	if err != nil {
		return f, apperr.Unprocessable("invalid_period", "invalid from").With("field", "from")
	}
	to, err := ParseDay(q.Get("to"))
	if err != nil {
		return f, apperr.Unprocessable("invalid_period", "invalid to").With("field", "to")
	}
	now := Today(s.clock())
	if to == nil {
		to = &now
	}
	if from == nil {
		d := to.AddDate(0, 0, -29)
		from = &d
	}
	if from.After(*to) {
		return f, apperr.Unprocessable("invalid_period", "from is after to").With("field", "from")
	}
	f.From, f.To = *from, *to
	switch q.Get("step") {
	case "", "day":
		f.Step = "day"
	case "week":
		f.Step = "week"
	case "month":
		f.Step = "month"
	default:
		return f, apperr.Unprocessable("invalid_period", "invalid step").With("field", "step")
	}
	for _, raw := range q["group"] {
		id, err := uuid.Parse(raw)
		if err != nil {
			return f, apperr.Unprocessable("invalid_period", "invalid group").With("field", "group")
		}
		f.Groups = append(f.Groups, id)
	}
	f.CallerKind = q.Get("callerKind")
	switch f.CallerKind {
	case "", CallerPersonal, CallerService, CallerClient:
	default:
		return f, apperr.Unprocessable("invalid_period", "invalid callerKind").With("field", "callerKind")
	}
	f.Channel = q.Get("channel")
	f.Mode = q.Get("mode")
	switch f.Mode {
	case "", "personal", "platform":
	default:
		return f, apperr.Unprocessable("invalid_period", "invalid mode").With("field", "mode")
	}
	f.IncludeUnpublished = q.Get("includeUnpublished") == "true"
	f.Hint = q.Get("hint")
	switch f.Hint {
	case "", HintUnused, HintSingleUser, HintErrorsOnly, HintDormant:
	default:
		return f, apperr.Unprocessable("invalid_period", "invalid hint").With("field", "hint")
	}
	page, err := httpx.ParsePage(r)
	if err != nil {
		return f, err
	}
	f.Limit, f.Cursor = page.Limit, page.Cursor
	return f, nil
}

func (s *Service) report(w http.ResponseWriter, r *http.Request) error {
	f, err := s.filter(r)
	if err != nil {
		return err
	}
	rows, next, meta, err := s.Report(r.Context(), f)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, map[string]any{"items": rows, "nextCursor": next, "meta": meta})
	return nil
}

func (s *Service) summary(w http.ResponseWriter, r *http.Request) error {
	f, err := s.filter(r)
	if err != nil {
		return err
	}
	tiles, meta, err := s.Summary(r.Context(), f.From, f.To, f.IncludeUnpublished)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, map[string]any{"tiles": tiles, "meta": meta})
	return nil
}

func (s *Service) itemCard(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.ParamUUID(r, "id")
	if err != nil {
		return ErrItemNotFound
	}
	f, err := s.filter(r)
	if err != nil {
		return err
	}
	card, groups, pubs, overlap, pins, states, meta, err := s.Item(r.Context(), id, f.From, f.To)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, map[string]any{"item": card, "groups": groups, "publications": pubs, "overlap": overlap,
		"pins": pins, "states": states, "meta": meta})
	return nil
}

func (s *Service) tools(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.ParamUUID(r, "id")
	if err != nil {
		return ErrItemNotFound
	}
	f, err := s.filter(r)
	if err != nil {
		return err
	}
	rows, meta, err := s.Tools(r.Context(), id, f.From, f.To)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, map[string]any{"items": rows, "meta": meta})
	return nil
}

func (s *Service) candidates(w http.ResponseWriter, r *http.Request) error {
	f, err := s.filter(r)
	if err != nil {
		return err
	}
	page, err := httpx.ParsePage(r)
	if err != nil {
		return err
	}
	rows, next, err := s.Candidates(r.Context(), f, r.URL.Query().Get("reason"), page.Limit, page.Cursor)
	if err != nil {
		return err
	}
	meta, err := s.Meta(r.Context())
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, map[string]any{"items": rows, "nextCursor": next, "meta": meta})
	return nil
}

func (s *Service) pins(w http.ResponseWriter, r *http.Request) error {
	page, err := httpx.ParsePage(r)
	if err != nil {
		return err
	}
	overdue := r.URL.Query().Get("overdue") == "true"
	rows, next, err := s.PinList(r.Context(), overdue, page.Limit, page.Cursor)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, map[string]any{"items": rows, "nextCursor": next})
	return nil
}

func (s *Service) export(w http.ResponseWriter, r *http.Request) error {
	f, err := s.filter(r)
	if err != nil {
		return err
	}
	f.Limit = 10000
	rows, _, _, err := s.Report(r.Context(), f)
	if err != nil {
		return err
	}
	b, err := CSV(rows)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="catalog-usage.csv"`)
	w.WriteHeader(200)
	_, _ = w.Write(b)
	return nil
}

// actor of an action: the author of the journal record (R31).
func actorOf(r *http.Request) Actor {
	if p := httpx.PrincipalFrom(r.Context()); p != nil {
		return Actor{ID: p.UserID, Email: p.Email}
	}
	return Actor{}
}

func (s *Service) impactPreview(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.ParamUUID(r, "id")
	if err != nil {
		return ErrItemNotFound
	}
	out, err := s.ImpactUnpublish(r.Context(), id)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, out)
	return nil
}

func (s *Service) unpublish(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.ParamUUID(r, "id")
	if err != nil {
		return ErrItemNotFound
	}
	var in struct {
		Reason      string `json:"reason"`
		ConfirmName string `json:"confirmName"`
	}
	if err := httpx.Decode(r, &in); err != nil {
		return err
	}
	res, err := s.Unpublish(r.Context(), actorOf(r), id, in.Reason, in.ConfirmName)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, res)
	return nil
}

func (s *Service) publish(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.ParamUUID(r, "id")
	if err != nil {
		return ErrItemNotFound
	}
	var in struct {
		Reason string `json:"reason"`
	}
	if err := httpx.Decode(r, &in); err != nil {
		return err
	}
	res, err := s.Publish(r.Context(), actorOf(r), id, in.Reason)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, res)
	return nil
}

func (s *Service) grantImpact(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.ParamUUID(r, "id")
	if err != nil {
		return ErrItemNotFound
	}
	var in struct {
		GroupID string `json:"groupId"`
	}
	if err := httpx.Decode(r, &in); err != nil {
		return err
	}
	group, err := uuid.Parse(in.GroupID)
	if err != nil {
		return apperr.Unprocessable("invalid_group", "invalid groupId").With("field", "groupId")
	}
	out, err := s.ImpactGrant(r.Context(), id, group)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, out)
	return nil
}

func (s *Service) revokeGrant(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.ParamUUID(r, "id")
	if err != nil {
		return ErrItemNotFound
	}
	group, err := uuid.Parse(chi.URLParam(r, "groupId"))
	if err != nil {
		return apperr.NotFound("grant_not_found", "the grant is not found")
	}
	if err := s.RevokeGrant(r.Context(), actorOf(r), id, group, r.URL.Query().Get("reason")); err != nil {
		return err
	}
	httpx.NoContent(w)
	return nil
}

func (s *Service) pin(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.ParamUUID(r, "id")
	if err != nil {
		return ErrItemNotFound
	}
	var in struct {
		Reason   string     `json:"reason"`
		ReviewAt *time.Time `json:"reviewAt"`
	}
	if err := httpx.Decode(r, &in); err != nil {
		return err
	}
	p, err := s.PublishPin(r.Context(), actorOf(r), id, in.Reason, in.ReviewAt)
	if err != nil {
		return err
	}
	httpx.JSON(w, http.StatusCreated, p)
	return nil
}

func (s *Service) releasePin(w http.ResponseWriter, r *http.Request) error {
	id, err := httpx.ParamUUID(r, "id")
	if err != nil {
		return ErrItemNotFound
	}
	pin, err := uuid.Parse(chi.URLParam(r, "pinId"))
	if err != nil {
		return apperr.NotFound("pin_not_found", "the pin is not found")
	}
	if err := s.ReleasePin(r.Context(), id, pin); err != nil {
		return err
	}
	httpx.NoContent(w)
	return nil
}

// Reconcile runs the one-time reconciliation of access of the rollout window
// (R32, arch A22): worker calls it at start; a second call changes nothing.
func (s *Service) Reconcile(ctx context.Context) (int, bool, error) {
	if s.Access == nil {
		return 0, false, nil
	}
	return s.Access.ReconcilePublished(ctx, accessActor())
}

func accessActor() access.Actor { return access.Actor{Email: "system"} }
