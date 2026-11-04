package sandbox

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/GreenOnGrey/nabu-core/internal/runfiles"
)

// The working copy of a run (FTR.HMR.CMN-0007 tech §2.4, §2.5): the sandbox
// unpacks it before it connects to the relay, so the agent sees the copy from
// its first call; after the agent the engine asks for the changes.

func (s *Sandbox) runURL(op string) string {
	return s.cfg.APIURL + "/internal/v1/sandbox/runs/" + s.cfg.RunID + "/" + op
}

func (s *Sandbox) fetchFiles(ctx context.Context) (runfiles.Manifest, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.runURL("files"), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.RunToken)
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return nil, fmt.Errorf("files of the run: %s: %s", resp.Status, b)
	}
	m, err := runfiles.Extract(resp.Body, s.cfg.Root)
	if err != nil {
		return nil, err
	}
	slog.Info("working copy unpacked", "files", len(m))
	return m, nil
}

// changesReport is the answer of the workspace/changes operation.
type changesReport struct {
	Files   int                `json:"files"`
	Skipped []runfiles.Skipped `json:"skipped"`
}

// hasChanges tells whether the working copy changed (FTR.HMR.CMN-0008 R7).
func (s *Sandbox) hasChanges(m runfiles.Manifest) (bool, error) {
	return runfiles.HasChanges(s.cfg.Root, m)
}

func (s *Sandbox) uploadChanges(ctx context.Context, m runfiles.Manifest) (any, error) {
	pr, pw := io.Pipe()
	done := make(chan *runfiles.Changes, 1)
	go func() {
		c, err := runfiles.Collect(s.cfg.Root, m, pw)
		pw.CloseWithError(err)
		done <- c
	}()
	// The count is known only after the walk: the engine stores it from the
	// answer of the operation.
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.runURL("changes"), pr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.RunToken)
	req.Header.Set("Content-Type", "application/gzip")
	resp, err := s.http.Do(req)
	c := <-done
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return nil, fmt.Errorf("changes of the run: %s: %s", resp.Status, b)
	}
	if c == nil {
		return nil, fmt.Errorf("changes of the run: the walk failed")
	}
	slog.Info("changes uploaded", "files", len(c.Changes), "skipped", len(c.Skipped))
	return changesReport{Files: len(c.Changes), Skipped: c.Skipped}, nil
}
