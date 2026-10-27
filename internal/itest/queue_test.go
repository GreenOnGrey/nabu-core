//go:build integration

package itest

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/kafka"
)

// keys records the keys of nabu.inbound.
type keys struct {
	*bus
	mu   sync.Mutex
	keys []string
}

func (k *keys) Publish(ctx context.Context, topic, key string, v []byte) error {
	if topic == kafka.TopicInbound {
		k.mu.Lock()
		k.keys = append(k.keys, key)
		k.mu.Unlock()
	}
	return k.bus.Publish(ctx, topic, key, v)
}

// running puts an answer being written into the conversation, as a turn in
// progress does.
func running(t *testing.T, conv uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(context.Background(), `INSERT INTO messages (conversation_id, role, text, channel, status)
		VALUES ($1,'assistant','','web','streaming') RETURNING id`, conv).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func queueOf(t *testing.T, id uuid.UUID) string {
	t.Helper()
	var q *string
	_ = pool.QueryRow(context.Background(), `SELECT queue FROM messages WHERE id = $1`, id).Scan(&q)
	if q == nil {
		return ""
	}
	return *q
}

// FTR.NAB.CHAT-0004: QUE-03, QUE-04, QUE-07…QUE-09, QUE-11, QUE-14, QUE-15,
// QUE-18, QUE-19
func TestChatQueue(t *testing.T) {
	ctx := context.Background()
	r := newChatRig(t)
	kb := &keys{bus: r.bus}
	r.api.Bus = kb
	uid := newUser(t, "queue-"+uuid.NewString()[:6]+"@x.org")
	main, _ := r.api.Store.Main(ctx, uid)

	// QUE-03: behind a running answer a message is queued — in the answer
	// of the POST, the event and the listing
	busy := running(t, main.ID)
	a, err := r.api.Post(ctx, uid, nil, "А", domain.ChannelWeb, nil, nil, nil)
	if err != nil || !a.Queued || queueOf(t, a.ID) != "waiting" {
		t.Fatalf("QUE-03: %+v %v", a, err)
	}
	l, _ := r.api.Store.Messages(ctx, main.ID, httpx.Page{Limit: 10})
	if l.Items[0].ID != a.ID || !l.Items[0].Queued {
		t.Fatalf("QUE-03: listing %+v", l.Items[0])
	}
	// QUE-15: the key of nabu.inbound is the conversation
	if kb.keys[len(kb.keys)-1] != main.ID.String() {
		t.Fatalf("QUE-15: key %v", kb.keys)
	}
	// QUE-14: a reset waits for the queue too
	if _, err := r.api.Reset(ctx, uid, nil, domain.ChannelWeb); !isCodeErr(err, "context_busy") {
		t.Fatalf("QUE-14: %v", err)
	}

	// QUE-07: a waiting message is cancelled: gone from the listing, the
	// engine skips it, its turn leaves the queue of the pod (QUE-19)
	b, _ := r.api.Post(ctx, uid, nil, "Б", domain.ChannelWeb, nil, nil, nil)
	_, _ = pool.Exec(ctx, `INSERT INTO agent_queue (owner_id, ref, kind, reason, topic, key, value) VALUES ($1, $2, 'message', 'order', 'nabu.inbound', '', '{}')`,
		uid, "m:"+b.ID.String())
	if err := r.api.CancelQueued(ctx, uid, b.ID); err != nil {
		t.Fatalf("QUE-07: %v", err)
	}
	var left int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM agent_queue WHERE ref = $1`, "m:"+b.ID.String()).Scan(&left)
	if left != 0 || !strings.Contains(strings.Join(r.ev.types(), ","), "message.cancelled") {
		t.Fatalf("QUE-07, QUE-19: queue %d, events %v", left, r.ev.types())
	}
	l, _ = r.api.Store.Messages(ctx, main.ID, httpx.Page{Limit: 10})
	for _, m := range l.Items {
		if m.ID == b.ID {
			t.Fatal("QUE-07: the cancelled message is listed")
		}
	}
	// QUE-08: a cancelled again, an answer, a message of another user
	if err := r.api.CancelQueued(ctx, uid, b.ID); !isCodeErr(err, "message_started") {
		t.Fatalf("QUE-08: again %v", err)
	}
	if err := r.api.CancelQueued(ctx, uid, busy); !isCodeErr(err, "message_started") {
		t.Fatalf("QUE-08: answer %v", err)
	}
	other := newUser(t, "queue-other-"+uuid.NewString()[:6]+"@x.org")
	if err := r.api.CancelQueued(ctx, other, a.ID); !isCodeErr(err, "not_found") {
		t.Fatalf("QUE-08: another user %v", err)
	}

	// QUE-11: ten waiting messages fill the queue; one older than a day does not count
	_, _ = pool.Exec(ctx, `UPDATE messages SET created_at = now() - interval '25 hours' WHERE id = $1`, a.ID)
	for i := 0; i < 10; i++ {
		if _, err := r.api.Post(ctx, uid, nil, "в очередь", domain.ChannelWeb, nil, nil, nil); err != nil {
			t.Fatalf("QUE-11: message %d: %v", i+1, err)
		}
	}
	if _, err := r.api.Post(ctx, uid, nil, "лишнее", domain.ChannelWeb, nil, nil, nil); !isCodeErr(err, "queue_full") {
		t.Fatalf("QUE-11: %v", err)
	}

	// QUE-04: the answers follow the order of the queue; every message is started
	_, _ = pool.Exec(ctx, `UPDATE messages SET status = 'done' WHERE id = $1`, busy)
	r.drain()
	rows, _ := pool.Query(ctx, `SELECT u.id, u.queue FROM messages u JOIN messages a ON a.reply_to = u.id
		WHERE u.conversation_id = $1 ORDER BY a.created_at`, main.ID)
	var prev time.Time
	n := 0
	for rows.Next() {
		var id uuid.UUID
		var q string
		_ = rows.Scan(&id, &q)
		var at time.Time
		_ = pool.QueryRow(ctx, `SELECT created_at FROM messages WHERE id = $1`, id).Scan(&at)
		if q != "started" || at.Before(prev) {
			t.Fatalf("QUE-04: %s %s", id, q)
		}
		prev = at
		n++
	}
	rows.Close()
	if n != 11 {
		t.Fatalf("QUE-04: %d answers", n)
	}
	var answeredB int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE reply_to = $1`, b.ID).Scan(&answeredB)
	if answeredB != 0 {
		t.Fatal("QUE-07: the cancelled message is answered")
	}

	// QUE-09: a cancellation and a start of the same message race: exactly one wins
	for i := 0; i < 50; i++ {
		m, err := r.api.Store.AddUser(ctx, main.ID, "гонка", "web", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		var cancelErr error
		var cancelled bool
		wg.Add(2)
		go func() { defer wg.Done(); cancelErr = r.api.CancelQueued(ctx, uid, m.ID) }()
		go func() { defer wg.Done(); cancelled = r.api.Store.Start(ctx, m.ID) }()
		wg.Wait()
		q := queueOf(t, m.ID)
		won := q == "cancelled" && cancelErr == nil && cancelled
		lost := q == "started" && isCodeErr(cancelErr, "message_started") && !cancelled
		if !won && !lost {
			t.Fatalf("QUE-09 #%d: queue %s, cancel %v, start saw cancelled %v", i, q, cancelErr, cancelled)
		}
	}

	// QUE-18: an old message without a queue is answered as before
	var old uuid.UUID
	_ = pool.QueryRow(ctx, `INSERT INTO messages (conversation_id, role, text, channel) VALUES ($1,'user','старое','web') RETURNING id`, main.ID).Scan(&old)
	m, _ := r.api.Store.Message(ctx, uid, old)
	if m.Queued {
		t.Fatal("QUE-18: an old message is queued")
	}
}

// FTR.NAB.CHAT-0004: STP-01, STP-04, STP-05, STP-07, STP-08
func TestChatStop(t *testing.T) {
	ctx := context.Background()
	r := newChatRig(t)
	uid := newUser(t, "stop-"+uuid.NewString()[:6]+"@x.org")
	main, _ := r.api.Store.Main(ctx, uid)

	// STP-01, STP-05: the user stops the answer while the agent writes it
	r.op.during = func() {
		var id uuid.UUID
		_ = pool.QueryRow(ctx, `SELECT id FROM messages WHERE conversation_id = $1 AND role = 'assistant' AND status = 'streaming'`, main.ID).Scan(&id)
		if err := r.api.Store.RequestStop(ctx, uid, id); err != nil {
			t.Errorf("STP-01: %v", err)
		}
		if err := r.api.Store.RequestStop(ctx, uid, id); err != nil { // STP-04: a repeated request
			t.Errorf("STP-04: again %v", err)
		}
		time.Sleep(3 * time.Second) // the turn sees the mark within 2 s
	}
	if _, err := r.api.Post(ctx, uid, nil, "пиши долго", domain.ChannelWeb, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	r.drain()
	r.op.during = nil
	h, _ := r.api.Store.History(ctx, main.ID, 1)
	if h[0].Status != "stopped" || h[0].Text != "ok" || h[0].ErrorClass != nil {
		t.Fatalf("STP-01, STP-05: %+v", h[0])
	}
	// STP-04: a finished answer is not stopped
	if err := r.api.Store.RequestStop(ctx, uid, h[0].ID); !isCodeErr(err, "not_running") {
		t.Fatalf("STP-04: %v", err)
	}
	// STP-07: the next prompt tells the agent
	if _, err := r.api.Post(ctx, uid, nil, "дальше", domain.ChannelWeb, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	r.drain()
	if p := r.op.last().Text; !strings.HasPrefix(p, "[The user stopped your previous answer.]") {
		t.Fatalf("STP-07: %q", p)
	}
	r.api.Post(ctx, uid, nil, "ещё", domain.ChannelWeb, nil, nil, nil) //nolint:errcheck
	r.drain()
	if p := r.op.last().Text; strings.Contains(p, "stopped your previous") {
		t.Fatal("STP-07: the mark stays")
	}

	// STP-08: /stop in a messenger with an answer and without one
	_ = r.outbound()
	if err := r.api.Receive(ctx, channels.Inbound{UserID: uid, Channel: domain.ChannelTelegram, Text: "/stop"}); err != nil {
		t.Fatal(err)
	}
	if out := r.outbound(); len(out) != 1 || out[0] != channels.T("en", "cmd.not_running") && out[0] != channels.T("ru", "cmd.not_running") {
		t.Fatalf("STP-08: %v", out)
	}
	busy := running(t, main.ID)
	if err := r.api.Receive(ctx, channels.Inbound{UserID: uid, Channel: domain.ChannelTelegram, Text: "/stop"}); err != nil {
		t.Fatal(err)
	}
	if !r.api.Store.StopRequested(ctx, busy) {
		t.Fatal("STP-08: the answer is not marked")
	}
	if out := r.outbound(); len(out) != 1 || !strings.Contains(out[0], "Stopped") && !strings.Contains(out[0], "Остановил") {
		t.Fatalf("STP-08: %v", out)
	}
	_, _ = pool.Exec(ctx, `UPDATE messages SET status = 'done' WHERE id = $1`, busy)

	// R9: a stopped answer can be retried
	if err := r.api.Retry(ctx, &domain.Principal{UserID: uid}, h[0].ID); err != nil {
		t.Fatalf("R9: retry %v", err)
	}
}

// FTR.NAB.CHAT-0004 QUE-11 (parallel posts), R3 (the history of a session), R10 (a stopped answer in it)
func TestChatQueueEdges(t *testing.T) {
	ctx := context.Background()
	r := newChatRig(t)
	uid := newUser(t, "qedge-"+uuid.NewString()[:6]+"@x.org")
	main, _ := r.api.Store.Main(ctx, uid)

	// QUE-11: fifteen messages at once behind a running answer: ten wait, five are refused
	busy := running(t, main.ID)
	var wg sync.WaitGroup
	var mu sync.Mutex
	full := 0
	for i := 0; i < 15; i++ {
		wg.Go(func() {
			_, err := r.api.Post(ctx, uid, nil, "сразу", domain.ChannelWeb, nil, nil, nil)
			if isCodeErr(err, "queue_full") {
				mu.Lock()
				full++
				mu.Unlock()
			} else if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	var waiting int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM messages WHERE conversation_id = $1 AND queue = 'waiting'`, main.ID).Scan(&waiting)
	if waiting != 10 || full != 5 {
		t.Fatalf("QUE-11: %d waiting, %d refused", waiting, full)
	}

	// R3: the history has no cancelled message (the engine also skips the waiting ones); R10: a stopped answer is in it
	var cancelled uuid.UUID
	_ = pool.QueryRow(ctx, `SELECT id FROM messages WHERE conversation_id = $1 AND queue = 'waiting' LIMIT 1`, main.ID).Scan(&cancelled)
	if err := r.api.CancelQueued(ctx, uid, cancelled); err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `UPDATE messages SET status = 'stopped', text = 'половина' WHERE id = $1`, busy)
	h, _ := r.api.Store.History(ctx, main.ID, 50)
	stopped := false
	for _, m := range h {
		if m.ID == cancelled {
			t.Fatalf("R3: the cancelled message %s is in the history", m.ID)
		}
		stopped = stopped || m.ID == busy
	}
	if !stopped {
		t.Fatal("R10: the stopped answer is not in the history")
	}
}
