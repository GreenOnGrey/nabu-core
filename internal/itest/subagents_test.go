//go:build integration

package itest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// FTR.NAB.CMN-0009: SUB-16, SUB-18…SUB-20 — the engine side of subagents.
func TestSubagentsInTheTurn(t *testing.T) {
	ctx := context.Background()
	r := newChatRig(t)
	uid := newUser(t, "sub-"+uuid.NewString()[:6]+"@x.org")
	main, _ := r.api.Store.Main(ctx, uid)

	// SUB-20: without SUBAGENTS_ENABLED the sessions have no subagents
	if _, err := r.api.Post(ctx, uid, nil, "привет", domain.ChannelWeb, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	r.drain()
	if r.op.opened[len(r.op.opened)-1].Subagents != nil {
		t.Fatal("SUB-20: subagents without SUBAGENTS_ENABLED")
	}

	limits := &agent.SubagentLimits{MaxParallel: 4, MaxPerTurn: 8, Timeout: agent.Duration(15 * time.Minute), ResultChars: 16000,
		SlotWait: agent.Duration(time.Minute)}
	r.eng.Cfg.Subagents = limits
	_, _ = pool.Exec(ctx, `UPDATE harness_sessions SET closed_at = now() WHERE conversation_id = $1`, main.ID)
	state := func(task int, st string, tools int, result string) *agent.SubagentState {
		return &agent.SubagentState{Call: "tc1", Task: task, Description: "Ревью " + string(rune('0'+task)), State: st, Tools: tools, Result: result}
	}
	r.op.script = []agent.Event{
		{Type: agent.EventToolCall, ID: "tc1", Name: "subagent", Args: []byte(`{"tasks":[]}`)},
		{Type: agent.EventSubagent, Subagent: state(1, agent.SubagentRunning, 0, "")},
		{Type: agent.EventSubagent, Subagent: state(2, agent.SubagentRunning, 0, "")},
		{Type: agent.EventToolCall, ID: "x1", Name: "mcp__nabu__memory_list", Args: []byte(`{}`), Of: "tc1/1"},
		{Type: agent.EventToolResult, ID: "x1", Name: "mcp__nabu__memory_list", Summary: "[]", Of: "tc1/1"},
		{Type: agent.EventSubagent, Subagent: state(1, agent.SubagentDone, 1, "Нарушений нет"), Usage: agent.Usage{TokensIn: 100, TokensOut: 10}},
		{Type: agent.EventSubagent, Subagent: state(2, agent.SubagentTimeout, 0, "Половина"), Usage: agent.Usage{TokensIn: 50, TokensOut: 5}},
		{Type: agent.EventToolResult, ID: "tc1", Name: "subagent", Summary: "## Ревью 1 — done"},
		{Type: agent.EventTextDelta, Delta: "Готово"},
	}
	if _, err := r.api.Post(ctx, uid, nil, "сделай ревью", domain.ChannelWeb, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	r.drain()
	r.op.script = nil
	if got := r.op.opened[len(r.op.opened)-1].Subagents; got == nil || got.MaxPerTurn != 8 {
		t.Fatalf("SUB-20: the limits are not in the session: %+v", got)
	}

	// SUB-16: the step of the call keeps the final states of its tasks; the tools of a subagent are no steps
	h, _ := r.api.Store.History(ctx, main.ID, 1)
	a := h[0]
	if len(a.ToolSteps) != 1 || a.ToolSteps[0].Tool != "subagent" {
		t.Fatalf("SUB-16: steps %+v", a.ToolSteps)
	}
	sa := a.ToolSteps[0].Subagents
	if len(sa) != 2 || sa[0].State != "done" || sa[0].Tools != 1 || sa[0].Result != "Нарушений нет" || sa[1].State != "timeout" {
		t.Fatalf("SUB-16: %+v", sa)
	}
	// SUB-18: the cost of the answer is the sum of the agent and its subagents
	var in int64
	_ = pool.QueryRow(ctx, `SELECT COALESCE(sum(tokens_in), 0) FROM usage WHERE user_id = $1 AND created_at > now() - interval '1 minute'`, uid).Scan(&in)
	if in < 150+10 {
		t.Fatalf("SUB-18: tokens in %d", in)
	}
	// SUB-19: the tool of the subagent is audited with its call
	var mark string
	if err := pool.QueryRow(ctx, `SELECT subagent FROM audit WHERE user_id = $1 AND tool = 'memory_list'`, uid).Scan(&mark); err != nil || mark != "tc1/1 Ревью 1" {
		t.Fatalf("SUB-19: %q %v", mark, err)
	}
}
