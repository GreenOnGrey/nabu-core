package space

// A file the user has just uploaded reaches the agent without a loop of sleeps in the shell: the tool waits for the
// file where the agent reads it. FTR.NAB.CMN-0001 SPC-09 — the uploaded file appears in the working copy;
// FTR.NAB.CHAT-0001 R33 — a file of the space in the answer. A person uploading a large archive saw the agent poll `attachments/…` with
// `for i in $(seq 1 10); do … sleep 6; done` for half an hour, and the turns were spent on that.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
)

const (
	// waitDefault is how long space_wait waits when the agent names no time, waitMax — at most.
	waitDefault = 60 * time.Second
	waitMax     = 5 * time.Minute
	// waitStep is the pause between the looks at the space and the sandbox.
	waitStep = time.Second
)

// ErrWaitTimeout is the file that did not reach the space and the sandbox in time.
var ErrWaitTimeout = errors.New("the file did not appear")

// WaitFor blocks until the space holds the file and, when a sandbox runs, its working copy too.
func (s *Snapshots) WaitFor(ctx context.Context, uid uuid.UUID, p string, timeout time.Duration) (File, error) {
	p, err := CleanPath(p)
	if err != nil {
		return File{}, catalog.ErrFileInvalid
	}
	if timeout <= 0 {
		timeout = waitDefault
	}
	deadline := time.Now().Add(timeout)
	for {
		f, err := s.Space.Stat(ctx, uid, p)
		if err == nil && s.inSandbox(ctx, uid, p) {
			return f, nil
		}
		if time.Now().After(deadline) {
			// the file may still be uploading, or the path is a typo: the agent asks the user instead of guessing
			return File{}, ErrWaitTimeout
		}
		select {
		case <-ctx.Done():
			return File{}, ctx.Err()
		case <-time.After(waitStep):
		}
	}
}

// inSandbox reports whether the working copy has the file; with no sandbox the space is the only copy and the next
// start brings it.
func (s *Snapshots) inSandbox(ctx context.Context, uid uuid.UUID, p string) bool {
	ok, err := s.existsInSandbox(ctx, uid, p)
	if errors.Is(err, errNoSandbox) {
		return true
	}
	return err == nil && ok
}

// WaitTool is space_wait of the built-in MCP.
func (s *Snapshots) WaitTool() mcp.Tool {
	return mcp.Tool{Name: "space_wait", ReadOnly: true,
		Description: "Wait until a file is in your space and in your working copy: the user has just uploaded it. " +
			"Call it once instead of writing a loop with sleeps; it answers when the file is readable and says so " +
			"when it does not appear. Waits 60 seconds by default, 300 at most.",
		InputSchema: mcp.Schema(map[string]any{
			"path":           map[string]any{"type": "string", "description": "Path of the file in the space, e.g. attachments/cars.zip"},
			"timeoutSeconds": map[string]any{"type": "integer", "description": "How long to wait: 60 by default, 300 at most"},
		}, "path"),
		// a conversation: a run of a task has a working copy of its own that the upload does not reach
		Available: func(_ context.Context, g mcp.Grant) bool { return g.Conversation != uuid.Nil },
		Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
			var in struct {
				Path    string `json:"path"`
				Timeout int    `json:"timeoutSeconds"`
			}
			if err := json.Unmarshal(args, &in); err != nil || strings.TrimSpace(in.Path) == "" {
				return "", &mcp.ToolError{Msg: "path is required"}
			}
			timeout := time.Duration(in.Timeout) * time.Second
			if timeout <= 0 {
				timeout = waitDefault
			}
			if timeout > waitMax {
				timeout = waitMax
			}
			f, err := s.WaitFor(ctx, g.UserID, in.Path, timeout)
			switch {
			case errors.Is(err, catalog.ErrFileInvalid):
				return "", &mcp.ToolError{Msg: "the path is not one of your space: " + in.Path}
			case errors.Is(err, ErrWaitTimeout):
				return "", &mcp.ToolError{Msg: fmt.Sprintf("%s did not appear in %d seconds: the file may still be uploading — ask the user, or call again once they say it is ready", in.Path, int(timeout.Seconds()))}
			case err != nil:
				return "", err
			}
			return fmt.Sprintf("%s is in the space (%d bytes): read it now.", f.Path, f.Size), nil
		}}
}
