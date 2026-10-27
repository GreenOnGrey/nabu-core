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
	// FTR.NAB.CMN-0003 tech §3.5: the memory of communications.
	CommChanged        = "comm.changed"
	CommitmentsChanged = "commitments.changed"
	CommCollect        = "comm.collect"
	AgentUpdated       = "agent.updated"
	ConnectionsChanged = "connections.changed"
	// ConversationContext: the fill of the context window of a conversation
	// changed (FTR.NAB.CHAT-0001 R3).
	ConversationContext = "conversation.context"
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
	if len(b) > maxNotify {
		slog.WarnContext(ctx, "event too large for NOTIFY, sending its ids", "type", e.Type)
		b, _ = json.Marshal(Event{Type: e.Type, UserID: e.UserID, RunID: e.RunID, Data: Stub(e.Data)})
	}
	if _, err := p.pool.Exec(context.WithoutCancel(ctx), "SELECT pg_notify($1, $2)", channel, string(b)); err != nil {
		slog.ErrorContext(ctx, "publish event", "err", err, "type", e.Type)
	}
}

// maxNotify is the limit of a NOTIFY payload (8000 bytes) with a margin.
const maxNotify = 7900

// stubKeys are the fields an event keeps when its data does not fit NOTIFY:
// enough for a client to tell what changed and to read it again.
var stubKeys = []string{"id", "conversationId", "messageId", "taskId", "runId", "role", "status"}

// Stub is the data of an event too large for NOTIFY: its ids and status with
// "truncated": true, so the client reads the object again instead of waiting
// for it (FTR.NAB.CMN-0001 tech, decision 23). Data that is not an object
// gives nil.
func Stub(data any) any {
	b, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(b, &m) != nil || m == nil {
		return nil
	}
	out := map[string]any{"truncated": true}
	for _, k := range stubKeys {
		if v, ok := m[k]; ok && len(v) <= 100 {
			out[k] = v
		}
	}
	return out
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
