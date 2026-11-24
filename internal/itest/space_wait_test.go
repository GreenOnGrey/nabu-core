//go:build integration

package itest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/space"
)

// FTR.NAB.CMN-0001 SPC-09; FTR.NAB.CHAT-0001 R33: an agent waits for the file the user has just uploaded instead of
// looping with sleeps in the shell; the tool answers when the file is readable and says so when it does not appear.
func TestSpaceWaitTool(t *testing.T) {
	ctx := context.Background()
	uid := newUser(t, "space-wait@x.org")
	sp := &space.Service{Pool: pool}
	if _, err := sp.Info(ctx, uid); err != nil { // the row of the space
		t.Fatal(err)
	}
	snaps := &space.Snapshots{Space: sp} // no sandbox: the space is the only copy
	tool := snaps.WaitTool()
	grant := mcp.Grant{UserID: uid, Conversation: uuid.New()}

	args, _ := json.Marshal(map[string]any{"path": "attachments/upload.zip", "timeoutSeconds": 5})
	go func() {
		time.Sleep(300 * time.Millisecond)
		_, _ = pool.Exec(ctx, `INSERT INTO space_files (user_id, path, size, sha256, modified_by) VALUES ($1,$2,4,$3,'user')`,
			uid, "attachments/upload.zip", strings.Repeat("a", 64))
	}()
	out, err := tool.Handler(ctx, grant, args)
	if err != nil {
		t.Fatalf("the file appeared, the tool failed: %v", err)
	}
	if !strings.Contains(out, "attachments/upload.zip") || !strings.Contains(out, "4 bytes") {
		t.Fatalf("the answer: %q", out)
	}

	// a file that does not come: the tool names the wait instead of hanging
	never, _ := json.Marshal(map[string]any{"path": "attachments/never.zip", "timeoutSeconds": 1})
	start := time.Now()
	_, err = tool.Handler(ctx, grant, never)
	var te *mcp.ToolError
	if !errors.As(err, &te) || !strings.Contains(te.Msg, "did not appear") {
		t.Fatalf("no timeout message: %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("waited %s for a second", d)
	}

	// a run of a task has a working copy of its own: the tool is not offered there
	if tool.Available(ctx, mcp.Grant{UserID: uid, TaskID: uuid.New()}) {
		t.Fatal("space_wait is offered to a task run")
	}
	if !tool.Available(ctx, grant) {
		t.Fatal("space_wait is not offered to a conversation")
	}
}
