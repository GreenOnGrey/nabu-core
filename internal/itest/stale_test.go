//go:build integration

package itest

import (
	"context"
	"testing"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/chat"
)

// STP-14…STP-18: an answer that a turn left unfinished is closed no matter
// why the turn died, and is never left in streaming (FTR.NAB.CHAT-0004 R13).
func TestStaleAnswers(t *testing.T) {
	ctx := context.Background()
	st := &chat.Store{Pool: pool}
	uid := newUser(t, "stale@x.org")
	conv, err := st.Main(ctx, uid)
	if err != nil {
		t.Fatal(err)
	}
	ask := func(text string) (*chat.Message, *chat.Message) {
		t.Helper()
		m, err := st.AddUser(ctx, conv.ID, text, "web", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		a, err := st.AddAssistant(ctx, conv.ID, "web", &m.ID, "m", nil)
		if err != nil {
			t.Fatal(err)
		}
		return m, a
	}

	// STP-14: a NUL character of a tool result fails neither the text nor the steps
	user, answer := ask("nul")
	if _, err := st.Finish(ctx, answer.ID, "a\x00b", "done",
		[]chat.ToolStep{{ID: "1", Server: "nul\x00", Tool: "read", Summary: "bin\x00ary", Status: "done"}}, "", "e\x00"); err != nil {
		t.Fatalf("STP-14: Finish with NUL: %v", err)
	}
	got, err := st.Message(ctx, uid, answer.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Text != "ab" || got.ToolSteps[0].Summary != "binary" {
		t.Fatalf("STP-14: %q %+v", got.Text, got.ToolSteps)
	}

	// STP-15: the sweeper closes the answer of a dead turn and leaves a live one alone
	oldAsk, oldAnswer := ask("old")
	freshAsk, freshAnswer := ask("fresh")
	_ = oldAsk
	_ = freshAsk
	if _, err := pool.Exec(ctx, `UPDATE messages SET created_at = now() - interval '2 hours' WHERE id = $1`, oldAnswer.ID); err != nil {
		t.Fatal(err)
	}
	closed, err := st.CloseStale(ctx, chat.TurnStale(30*time.Minute), "agent_crashed", "the turn did not finish within its time", 100)
	if err != nil {
		t.Fatal(err)
	}
	var closedOld, closedFresh bool
	for _, c := range closed {
		if c.ID == oldAnswer.ID {
			closedOld = true
			if c.UserID != uid || c.Status != "failed" || c.ErrorClass == nil || *c.ErrorClass != "agent_crashed" {
				t.Fatalf("STP-15: closed answer %+v", c)
			}
		}
		if c.ID == freshAnswer.ID {
			closedFresh = true
		}
	}
	if !closedOld || closedFresh {
		t.Fatalf("STP-15: closed old = %v, fresh = %v", closedOld, closedFresh)
	}
	old, _ := st.Message(ctx, uid, oldAnswer.ID)
	if old.Status != "failed" || old.ErrorText == nil {
		t.Fatalf("STP-15: %q %v", old.Status, old.ErrorText)
	}

	// STP-15: an answer waiting for a pod to continue it is alive, not dead
	resumedAsk, resumed := ask("resume")
	if _, err := pool.Exec(ctx, `INSERT INTO agent_queue (owner_id, kind, reason, ref, topic, key, value, conversation_id, message_id, resume)
		VALUES ($1,'message','order',$2,'nabu.inbound','k','{}',$3,$4,$5)`,
		uid, "m:"+resumedAsk.ID.String(), conv.ID, resumedAsk.ID, resumed.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE messages SET created_at = now() - interval '2 hours' WHERE id = $1`, resumed.ID); err != nil {
		t.Fatal(err)
	}
	closed, err = st.CloseStale(ctx, chat.TurnStale(30*time.Minute), "agent_crashed", "the turn did not finish within its time", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range closed {
		if c.ID == resumed.ID {
			t.Fatalf("STP-15: an answer waiting for a pod was closed: %+v", c)
		}
	}

	// STP-16: a finished answer answers the message; an unfinished one is a
	// leftover the redelivery closes instead of answering again
	if finished, unfinished, err := st.Answers(ctx, user.ID); err != nil || !finished || len(unfinished) > 0 {
		t.Fatalf("STP-16: finished %v, unfinished %v, err %v", finished, unfinished, err)
	}
	leftAsk, left := ask("left")
	if finished, unfinished, err := st.Answers(ctx, leftAsk.ID); err != nil || finished || len(unfinished) != 1 || unfinished[0] != left.ID {
		t.Fatalf("STP-16: finished %v, unfinished %v, err %v", finished, unfinished, err)
	}

	// STP-17: the stop of a live answer only marks it; the stop of a stale one
	// closes it at once — no turn is there to read the mark
	_, live := ask("stop live")
	stale := chat.TurnStale(30 * time.Minute)
	m, err := st.RequestStop(ctx, uid, live.ID, stale)
	if err != nil || m.Status != "streaming" || !st.StopRequested(ctx, live.ID) {
		t.Fatalf("STP-17: live %+v, %v", m, err)
	}
	_, abandoned := ask("stop abandoned")
	if _, err := pool.Exec(ctx, `UPDATE messages SET created_at = now() - interval '2 hours' WHERE id = $1`, abandoned.ID); err != nil {
		t.Fatal(err)
	}
	m, err = st.RequestStop(ctx, uid, abandoned.ID, stale)
	if err != nil || m.Status != "stopped" {
		t.Fatalf("STP-17: abandoned %+v, %v", m, err)
	}

	// STP-18: Fail keeps what the turn wrote and closes the answer once
	if m, err := st.Fail(ctx, abandoned.ID, "agent_crashed", "late"); err != nil || m != nil {
		t.Fatalf("STP-18: a stopped answer is not failed again: %+v, %v", m, err)
	}
	if m, err := st.Fail(ctx, live.ID, "agent_crashed", "the turn did not finish"); err != nil || m == nil || m.Status != "failed" {
		t.Fatalf("STP-18: %+v, %v", m, err)
	}
}
