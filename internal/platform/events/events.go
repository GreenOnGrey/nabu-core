// Package events delivers server-sent events to browser clients and service
// clients.
//
// Every api and worker instance publishes events through Postgres NOTIFY; every
// api pod LISTENs and fans them out to its locally connected SSE clients. The
// worker runs agent turns and publishes their deltas this way, so a user's
// stream gets them on any api pod (FTR.NAB.CMN-0001 tech §3.2).
package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event types of the user stream (tech spec §3.2, §3.2a).
const (
	MessageCreated      = "message.created"
	MessageDelta        = "message.delta"
	MessageDone         = "message.done"
	ToolStep            = "tool.step"
	ChatError           = "chat.error"
	ConversationUpdated = "conversation.updated"
	SpaceState          = "space.state"
	TaskCreated         = "task.created"
	TaskUpdated         = "task.updated"
	TaskRunFinished     = "task.run.finished"
	MemoryChanged       = "memory.changed"
	AgentUpdated        = "agent.updated"
	ConnectionsChanged  = "connections.changed"
	// RunEvent carries one event of a service agent run (client API §5).
	RunEvent = "run.event"
)

const channel = "nabu_events"

// Event is one SSE message. UserID limits delivery to one user; RunID to the
// readers of one run; neither broadcasts.
type Event struct {
	Type   string     `json:"type"`
	UserID *uuid.UUID `json:"userId,omitempty"`
	RunID  *uuid.UUID `json:"runId,omitempty"`
	Data   any        `json:"data"`
}

// Publisher publishes events to all pods.
type Publisher interface {
	Publish(ctx context.Context, e Event)
}

// PGPublisher publishes via pg_notify.
type PGPublisher struct{ pool *pgxpool.Pool }

// NewPGPublisher creates a publisher.
func NewPGPublisher(pool *pgxpool.Pool) *PGPublisher { return &PGPublisher{pool: pool} }

// Publish sends the event; failures are logged, never returned: events are
// a refresh signal, the source of truth is the database.
func (p *PGPublisher) Publish(ctx context.Context, e Event) {
	b, err := json.Marshal(e)
	if err != nil {
		slog.ErrorContext(ctx, "encode event", "err", err)
		return
	}
	if len(b) > 7900 {
		slog.WarnContext(ctx, "event too large for NOTIFY, dropping data", "type", e.Type)
		b, _ = json.Marshal(Event{Type: e.Type, UserID: e.UserID, RunID: e.RunID})
	}
	if _, err := p.pool.Exec(context.WithoutCancel(ctx), "SELECT pg_notify($1, $2)", channel, string(b)); err != nil {
		slog.ErrorContext(ctx, "publish event", "err", err, "type", e.Type)
	}
}

// Hub fans events out to local SSE subscribers.
type Hub struct {
	mu   sync.RWMutex
	subs map[*Subscriber]struct{}
}

// Subscriber is one SSE connection: of a user or of a run.
type Subscriber struct {
	UserID uuid.UUID
	RunID  uuid.UUID
	C      chan Event
}

// NewHub creates a hub.
func NewHub() *Hub { return &Hub{subs: map[*Subscriber]struct{}{}} }

// Subscribe registers a user connection.
func (h *Hub) Subscribe(userID uuid.UUID) *Subscriber {
	return h.add(&Subscriber{UserID: userID, C: make(chan Event, 512)})
}

// SubscribeRun registers a connection to the events of one run.
func (h *Hub) SubscribeRun(runID uuid.UUID) *Subscriber {
	return h.add(&Subscriber{RunID: runID, C: make(chan Event, 512)})
}

func (h *Hub) add(s *Subscriber) *Subscriber {
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

// Unsubscribe removes a connection.
func (h *Hub) Unsubscribe(s *Subscriber) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
}

// Deliver sends to matching local subscribers; slow clients drop events.
func (h *Hub) Deliver(e Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs {
		if s.RunID != uuid.Nil {
			if e.RunID == nil || *e.RunID != s.RunID {
				continue
			}
		} else if e.RunID != nil || (e.UserID != nil && *e.UserID != s.UserID) {
			continue
		}
		select {
		case s.C <- e:
		default:
		}
	}
}

// Publish implements Publisher for local-only delivery.
func (h *Hub) Publish(_ context.Context, e Event) { h.Deliver(e) }

// Listen consumes NOTIFY messages into the hub until ctx is done, reconnecting on failure.
func (h *Hub) Listen(ctx context.Context, pool *pgxpool.Pool) {
	for ctx.Err() == nil {
		if err := h.listenOnce(ctx, pool); err != nil && ctx.Err() == nil {
			slog.Error("event listener failed, reconnecting", "err", err)
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (h *Hub) listenOnce(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		return err
	}
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		var e Event
		if err := json.Unmarshal([]byte(n.Payload), &e); err != nil {
			continue
		}
		h.Deliver(e)
	}
}
