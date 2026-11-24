package sandbox

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// SPC-09: the working copy takes the files the user uploaded even while the sandbox has changes of its own. The pull
// waited for the upload path to be free, and a tick with a pending change went on with the upload: a sandbox whose
// agent keeps writing (a long answer, a build) never saw the uploaded file, and the agent polled attachments/ for
// minutes with the file already in the space.
func TestSyncPullsWhileUploading(t *testing.T) {
	const user = "u1"
	var pulls, uploads atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/spaces/"+user+"/manifest":
			pulls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]remoteFile{{Path: "attachments/upload.zip", Size: 4, SHA256: "abc"}})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/spaces/"+user+"/objects":
			_, _ = w.Write([]byte("data"))
		case r.Method == http.MethodPut:
			uploads.Add(1)
			_, _ = w.Write([]byte(`{}`))
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "build"), 0o755); err != nil {
		t.Fatal(err)
	}
	// a change of the agent stands from the start: the first tick has it pending and its quiet period has not passed
	if err := os.WriteFile(filepath.Join(root, "build", "local.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s := New(Config{UserID: user, Root: root, APIURL: srv.URL, SandboxToken: "t", Quiet: time.Minute, PullEvery: time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Sync(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	// one tick is a second: the file must come while the change of the agent still waits for its quiet period
	deadline := time.Now().Add(1500 * time.Millisecond)
	for {
		if _, err := os.Stat(filepath.Join(root, "attachments", "upload.zip")); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the uploaded file did not reach the working copy: manifests %d, uploads %d", pulls.Load(), uploads.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
}
