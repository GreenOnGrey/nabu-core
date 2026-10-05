package events

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
)

// SSEHandler serves the user stream: GET /api/v1/events and, for delegation,
// GET /client/v1/me/events. One stream carries every event type.
func SSEHandler(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p := httpx.PrincipalFrom(r.Context())
		if p == nil {
			httpx.Error(w, r, apperr.ErrNoSession)
			return
		}
		sub := hub.Subscribe(p.UserID)
		defer hub.Unsubscribe(sub)
		Stream(w, r, sub, nil)
	}
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
		data, err := json.Marshal(e.Data)
		if err != nil {
			return
		}
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Type, data)
	}
	for _, e := range first {
		write(e)
	}
	fl.Flush()
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
			write(e)
			fl.Flush()
		}
	}
}
