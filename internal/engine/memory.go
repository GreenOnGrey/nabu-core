package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/comm"
	"github.com/GreenOnGrey/nabu-core/internal/ledger"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/tasks"
	"github.com/GreenOnGrey/nabu-core/internal/users"
)

// Collector performs a run of «Сбор коммуникаций» (FTR.NAB.CMN-0003 arch
// §3.1): it finds what changed in the sources of the user and starts the
// runs of the collector agent. It finishes the run of the task itself.
type Collector interface {
	Collect(ctx context.Context, u *users.User, ri *tasks.RunInfo) error
}

// memoryTask performs a run of a prepared memory task (FTR.NAB.CMN-0003 R13):
// the worker itself builds a digest, the collector gathers communications.
// Neither needs the pod of the user.
func (e *Engine) memoryTask(ctx context.Context, u *users.User, ri *tasks.RunInfo) error {
	if e.Comm == nil {
		return e.Tasks.Skip(ctx, ri.RunID, "unavailable", "the memory of communications is not configured")
	}
	if _, err := e.Comm.Allowed(ctx); err != nil {
		if ae, ok := apperr.As(err); ok && ae.Code == "comm_disabled" {
			// R24: the administrator switched the function off — nothing runs
			// and nothing is reported as a failure.
			_, _ = e.Pool.Exec(ctx, `DELETE FROM task_runs WHERE id = $1`, ri.RunID)
			return nil
		}
		return err
	}
	switch ri.Kind {
	case tasks.KindDigestDay:
		return e.digest(ctx, u, ri, "day")
	case tasks.KindDigestWeek:
		return e.digest(ctx, u, ri, "week")
	case tasks.KindCollect:
		if e.Collector == nil {
			return e.Tasks.Skip(ctx, ri.RunID, "unavailable", "the collector of communications is not configured")
		}
		return e.Collector.Collect(ctx, u, ri)
	}
	return e.Tasks.Skip(ctx, ri.RunID, "unavailable", "unknown kind of task "+ri.Kind)
}

// digest sends what to remind of today or this week (R16, tech §7): the
// commitments are selected by a query and laid out by a template — no model,
// no pod. An empty digest is not sent; the run is successful (DIG-03).
func (e *Engine) digest(ctx context.Context, u *users.User, ri *tasks.RunInfo, period string) error {
	loc, err := time.LoadLocation(u.Timezone)
	if err != nil || u.Timezone == "" {
		loc = time.UTC
	}
	items, err := e.Comm.Digest(ctx, u.ID, period, time.Now().In(loc))
	if err != nil {
		slog.ErrorContext(ctx, "digest", "task", ri.TaskID, "err", err)
		metrics.MemoryDigests.WithLabelValues(period, "failed").Inc()
		return e.Tasks.Finish(ctx, ri.RunID, false, "", "unavailable", "the commitments could not be read", nil)
	}
	if len(items) == 0 {
		metrics.MemoryDigests.WithLabelValues(period, "empty").Inc()
		return e.Tasks.Finish(ctx, ri.RunID, true, "nothing_to_remind", "", "", nil)
	}
	main, err := e.Chat.Main(ctx, u.ID)
	if err != nil {
		return err
	}
	text, card := comm.RenderDigest(items, period, u.Language)
	shown := *ri
	shown.Title = comm.DigestTitle(period, u.Language)
	msg := e.postTaskCard(ctx, u.ID, main.ID, &shown, text, card)
	metrics.MemoryDigests.WithLabelValues(period, "sent").Inc()
	if e.Ledger != nil {
		e.Ledger.Audit(ctx, ledger.Entry{AgentKind: "service", Agent: "memory-digest", UserID: &u.ID, Channel: "task:" + ri.TaskID.String(),
			Tool: "memory.digest." + period, Result: "ok"}, fmt.Sprintf(`{"items":%d}`, len(items)), nil) // R30
	}
	if msg == nil {
		return e.Tasks.Finish(ctx, ri.RunID, false, "", "unavailable", "the digest could not be written to the conversation", nil)
	}
	return e.Tasks.Finish(ctx, ri.RunID, true, fmt.Sprintf("items:%d", len(items)), "", "", &msg.ID)
}
