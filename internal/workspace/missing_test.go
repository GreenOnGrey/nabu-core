package workspace

import (
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// FTR.NAB.CMN-0008 SIG-01: a missing module of Python, the first segment of
// a dotted name.
func TestFindMissingModule(t *testing.T) {
	out := "Traceback (most recent call last):\n  File \"<string>\", line 1, in <module>\nModuleNotFoundError: No module named 'cv2.aruco'\n"
	got := FindMissing([]byte(out))
	if len(got) != 1 || got[0] != (Missing{Ecosystem: "python-module", Name: "cv2"}) {
		t.Fatalf("SIG-01: %+v", got)
	}
}

// SIG-02: commands that bash and dash could not find.
func TestFindMissingCommands(t *testing.T) {
	out := "bash: line 1: ffmpeg: command not found\nsh: 1: convert: not found\nzsh: command not found: x\n"
	got := FindMissing([]byte(out))
	want := []Missing{{"command", "ffmpeg"}, {"command", "convert"}, {"command", "x"}}
	if len(got) != len(want) {
		t.Fatalf("SIG-02: %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("SIG-02: %+v", got)
		}
	}
}

// SIG-03: at most five names a command, each once.
func TestFindMissingLimit(t *testing.T) {
	var b strings.Builder
	for _, m := range []string{"a1", "a2", "a3", "a1", "a4", "a5", "a6", "a7"} {
		b.WriteString("ModuleNotFoundError: No module named '" + m + "'\n")
	}
	if got := FindMissing([]byte(b.String())); len(got) != 5 || got[0].Name != "a1" || got[4].Name != "a5" {
		t.Fatalf("SIG-03: %+v", got)
	}
}

// SIG-04: only the last 64 KB of the output are searched.
func TestTailBuffer(t *testing.T) {
	tb := &tailBuffer{max: missingTail}
	tb.Write([]byte("ModuleNotFoundError: No module named 'early'\n"))
	tb.Write([]byte(strings.Repeat("x", missingTail)))
	if got := FindMissing(tb.Bytes()); len(got) != 0 {
		t.Fatalf("SIG-04: %+v", got)
	}
	if len(tb.Bytes()) != missingTail {
		t.Fatalf("tail %d", len(tb.Bytes()))
	}
}

// SIG-01, SIG-05: the hook gets the names after the command; the result of
// the command does not wait for it.
func TestExecReportsMissing(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("bash")
	}
	ws, srv, _ := wsServer(t)
	var mu sync.Mutex
	var got []Missing
	done := make(chan struct{})
	ws.OnMissing = func(m []Missing) {
		mu.Lock()
		got = m
		mu.Unlock()
		close(done)
		time.Sleep(time.Second) // a slow api must not delay the command
	}
	start := time.Now()
	resp, body := call(t, srv, "/v1/exec", "ws-token", map[string]any{"command": "nabu-surely-missing-cmd"})
	if resp.StatusCode != 200 || !strings.Contains(string(body), "exitCode") {
		t.Fatalf("exec: %d %s", resp.StatusCode, body)
	}
	if time.Since(start) > 900*time.Millisecond {
		t.Fatal("SIG-05: the command waited for the report")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("no report")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].Name != "nabu-surely-missing-cmd" {
		t.Fatalf("SIG-02: %+v", got)
	}
}

// The commands of a sandbox get the environment of the profile image but
// never the tokens of the sandbox (FTR.NAB.CMN-0008 tech §6).
func TestSandboxEnv(t *testing.T) {
	t.Setenv("NABU_SANDBOX_TOKEN", "secret")
	t.Setenv("MPLCONFIGDIR", "/tmp/mpl")
	t.Setenv("ORACLE_HOME", "/opt/oracle")
	env := strings.Join(SandboxEnv("/work"), "\n")
	if strings.Contains(env, "secret") || strings.Contains(env, "NABU_") {
		t.Fatal("a token leaked into the environment of commands")
	}
	for _, want := range []string{"HOME=/work", "MPLCONFIGDIR=/tmp/mpl", "ORACLE_HOME=/opt/oracle", "PATH=" + os.Getenv("PATH")} {
		if !strings.Contains(env, want) {
			t.Fatalf("missing %s in %s", want, env)
		}
	}
}
