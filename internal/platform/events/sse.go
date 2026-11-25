package events

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SSEHandler serves the user stream: GET /api/v1/events and, for delegation,
// GET /client/v1/me/events. One stream carries every event type.
func SSEHandler(hub *Hub, pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p := httpx.PrincipalFrom(r.Context())
		if p == nil {
			httpx.Error(w, r, apperr.ErrNoSession)
			return
		}
		// subscribe before the replay: an event published between the replay and
		// the subscription would be lost; a duplicate is harmless (the site reads
		// the object itself)
		sub := hub.Subscribe(p.UserID)
		defer hub.Unsubscribe(sub)
		var first []Event
		if after := lastEventID(r); after > 0 {
			var err error
			if first, err = Replay(r.Context(), pool, p.UserID, after, ReplayLimit); err != nil {
				slog.WarnContext(r.Context(), "replay events", "err", err)
			}
		}
		Stream(w, r, sub, first)
	}
}

// lastEventID is the id of the last event the browser got (EventSource sends
// it on reconnect by itself, FTR.NAB.CMN-0001 tech §3.2).
func lastEventID(r *http.Request) int64 {
	id, err := strconv.ParseInt(strings.TrimSpace(r.Header.Get("Last-Event-ID")), 10, 64)
	if err != nil || id < 0 {
		return 0
	}
	return id
}

// frame is one SSE frame of the stream; "" when the data cannot be encoded.
// The id makes the stream resumable: the browser sends it back as
// Last-Event-ID (FTR.NAB.CMN-0001 tech §3.2, decision 27).
func frame(e Event) string {
	data, err := json.Marshal(e.Data)
	if err != nil {
		return ""
	}
	if e.Seq > 0 {
		return fmt.Sprintf("id: %d\nevent: %s\ndata: %s\n\n", e.Seq, e.Type, data)
	}
	return fmt.Sprintf("event: %s\ndata: %s\n\n", e.Type, data)
}

// Stream writes the subscriber's events as SSE until the request ends. first
// is written before the live events (a replay).
func Stream(w http.ResponseWriter, r *http.Request, sub *Subscriber, first []Event) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "retry: 3000\n: connected\n\n")
	write := func(e Event) {
		if f := frame(e); f != "" {
			fmt.Fprint(w, f)
		}
	}
	for _, e := range first {
		write(e)
	}
	fl.Flush()
	// the replay and the live stream may carry the same event: the subscription
	// is taken before the replay, and the second copy must not undo the first
	var replayed int64
	if n := len(first); n > 0 {
		replayed = first[n-1].Seq
	}
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		case e := <-sub.C:
			if e.Seq > 0 && e.Seq <= replayed {
				continue // already in the replay
			}
			write(e)
			fl.Flush()
		}
	}
}
