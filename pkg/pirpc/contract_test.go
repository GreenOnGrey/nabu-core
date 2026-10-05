package pirpc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Contract tests against a real Pi (RPC-07, ERR-10). They run when
// PIRPC_PI_CMD names the Pi command, e.g. "pi" or "node /path/to/cli.js";
// the CI job that changes PI_VERSION or this package sets it.
func realPi(t *testing.T, providerURL string) *Client {
	t.Helper()
	cmd := strings.Fields(os.Getenv("PIRPC_PI_CMD"))
	if len(cmd) == 0 {
		t.Skip("PIRPC_PI_CMD is not set")
	}
	dir := t.TempDir()
	agent := filepath.Join(dir, "agent")
	for _, d := range []string{agent, filepath.Join(dir, "cwd"), filepath.Join(dir, "sessions")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	models := map[string]any{"providers": map[string]any{"hmr-test": map[string]any{
		"baseUrl": providerURL, "api": "openai-completions", "apiKey": "${HMR_LLM_KEY}",
		"models": []map[string]any{
			{"id": "ok", "contextWindow": 1000000, "maxTokens": 8000, "input": []string{"text"}, "reasoning": true,
				"thinkingLevelMap": map[string]any{"minimal": nil, "low": nil, "medium": nil, "high": "high", "xhigh": "max"},
				"cost":             map[string]float64{"input": 1.74, "output": 3.48, "cacheRead": 0.145, "cacheWrite": 0}},
			{"id": "e402", "contextWindow": 100000, "maxTokens": 8000},
			{"id": "e401", "contextWindow": 100000, "maxTokens": 8000},
			{"id": "e503", "contextWindow": 100000, "maxTokens": 8000},
		}}}}
	write := func(name string, v any) {
		b, _ := json.Marshal(v)
		if err := os.WriteFile(filepath.Join(agent, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("models.json", models)
	write("settings.json", map[string]any{"defaultTools": []string{}, "defaultProjectTrust": "never",
		"retry": map[string]any{"maxRetries": 1, "baseDelayMs": 50}})
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "USERPROFILE=" + dir, "SystemRoot=" + os.Getenv("SystemRoot"),
		"PI_CODING_AGENT_DIR=" + agent, "PI_OFFLINE=1", "PI_TELEMETRY=0", "PI_SKIP_VERSION_CHECK=1", "HMR_LLM_KEY=sk-contract"}
	c, err := Start(t.Context(), Options{Binary: cmd[0], Args: append(cmd[1:], "--mode", "rpc", "--no-approve",
		"--session-dir", filepath.Join(dir, "sessions")), Env: env, Dir: filepath.Join(dir, "cwd")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// fakeProvider is an OpenAI-compatible endpoint whose model id selects the answer.
func fakeProvider(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if r.Header.Get("Authorization") != "Bearer sk-contract" {
			http.Error(w, `{"error":{"message":"no key"}}`, http.StatusUnauthorized)
			return
		}
		fail := map[string]int{"e402": 402, "e401": 401, "e503": 503}
		if code, ok := fail[req.Model]; ok {
			msg := map[int]string{402: "Insufficient Balance", 401: "Authentication Fails", 503: "Service Unavailable"}[code]
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			fmt.Fprintf(w, `{"error":{"message":%q,"type":"api_error"}}`, msg)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for _, d := range []string{"Hello", " contract"} {
			fmt.Fprintf(w, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"model\":%q,\"choices\":[{\"index\":0,\"delta\":{\"content\":%q}}]}\n\n", req.Model, d)
		}
		fmt.Fprintf(w, "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"model\":%q,\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":5,\"total_tokens\":105}}\n\ndata: [DONE]\n\n", req.Model)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// RPC-07: the commands used by Hammurapi answer with the types of this package.
func TestContractCommands(t *testing.T) {
	c := realPi(t, fakeProvider(t).URL)
	m, err := c.SetModel(ctx(t), "hmr-test", "ok")
	if err != nil || m.ID != "ok" || m.Provider != "hmr-test" || m.Cost.Input != 1.74 {
		t.Fatalf("set_model %+v %v", m, err)
	}
	levels, err := c.GetAvailableThinkingLevels(ctx(t))
	if err != nil || strings.Join(levels, ",") != "off,high,xhigh" {
		t.Fatalf("levels %v %v", levels, err)
	}
	if err := c.SetThinkingLevel(ctx(t), "off"); err != nil {
		t.Fatal(err)
	}
	text, _ := deltas(t, c)
	if text != "Hello contract" {
		t.Fatalf("text %q", text)
	}
	st, err := c.GetSessionStats(ctx(t))
	if err != nil || st.Tokens.Input+st.Tokens.CacheRead != 100 || st.Tokens.Output != 5 || st.Cost <= 0 || st.SessionFile == "" {
		t.Fatalf("stats %+v %v", st, err)
	}
	if err := c.SwitchSession(ctx(t), st.SessionFile); err != nil {
		t.Fatal(err)
	}
	state, err := c.GetState(ctx(t))
	if err != nil || state.Model == nil || state.Model.ID != "ok" {
		t.Fatalf("state %+v %v", state, err)
	}
}

// ERR-10: the provider's HTTP status and text reach the final assistant message
// (and the final retry error for transient statuses).
func TestContractErrorFormat(t *testing.T) {
	srv := fakeProvider(t)
	for _, tc := range []struct {
		model, status, text string
	}{{"e402", "402", "Insufficient Balance"}, {"e401", "401", "Authentication Fails"}, {"e503", "503", "Service Unavailable"}} {
		t.Run(tc.model, func(t *testing.T) {
			c := realPi(t, srv.URL)
			if _, err := c.SetModel(ctx(t), "hmr-test", tc.model); err != nil {
				t.Fatal(err)
			}
			var lastErr, finalErr string
			err := c.PromptAndWait(ctx(t), PromptParams{Message: "hi"}, func(e Event) {
				switch e.Type {
				case EventMessageEnd:
					var me MessageEnd
					_ = e.Decode(&me)
					if me.Message.StopReason == "error" {
						lastErr = me.Message.ErrorMessage
					}
				case EventAutoRetryEnd:
					var r AutoRetry
					_ = e.Decode(&r)
					finalErr = r.FinalError
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(lastErr, tc.status+":") || !strings.Contains(lastErr, tc.text) {
				t.Fatalf("errorMessage %q", lastErr)
			}
			if tc.model == "e503" && !strings.HasPrefix(finalErr, "503:") {
				t.Fatalf("finalError %q", finalErr)
			}
		})
	}
}
