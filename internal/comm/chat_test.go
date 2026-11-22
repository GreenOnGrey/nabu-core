package comm

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// FTR.NAB.MEM-0001 arch A1: the call of the model of the compaction. A server that does not know response_format
// is asked again without it, and the tokens of both calls count; another 400 is not repeated.
func TestChatModelComplete(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer k" {
			http.Error(w, "bad", http.StatusNotFound)
			return
		}
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		if req["model"] == "long" {
			http.Error(w, `{"error":"the context is too long"}`, http.StatusBadRequest)
			return
		}
		if _, ok := req["response_format"]; ok {
			http.Error(w, `{"error":"unknown parameter response_format"}`, http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{\"title\":\"t\"}"}}],"usage":{"prompt_tokens":100,"completion_tokens":20}}`))
	}))
	defer srv.Close()
	m := &ChatModel{BaseURL: srv.URL + "/v1/", Key: "k", Model: "m"}
	text, usage, err := m.Complete(context.Background(), "sys", "user")
	if err != nil || text != `{"title":"t"}` || usage.PromptTokens != 100 || calls != 2 {
		t.Fatalf("%q %+v %v, %d calls", text, usage, err, calls)
	}
	calls = 0
	m.Model = "long"
	if _, _, err = m.Complete(context.Background(), "sys", "user"); err == nil || !strings.Contains(err.Error(), "too long") || calls != 1 {
		t.Fatalf("another 400: %v, %d calls", err, calls)
	}
}

// FTR.NAB.MEM-0001 tech, decision 23: the fields of the thinking off travel in both calls of the compaction — with
// response_format and without it (MEM-CP-32).
func TestChatModelExtra(t *testing.T) {
	str := func(s string) *string { return &s }
	for _, tc := range []struct {
		name    string
		conn    string
		setting *string
		check   func(map[string]any) bool
	}{
		{"deepseek", models.TypeDeepSeek, nil, func(b map[string]any) bool {
			th, _ := b["thinking"].(map[string]any)
			return th["type"] == "disabled"
		}},
		{"openai chat-template", models.TypeOpenAICompatible, str(models.ThinkingOffChatTemplate), func(b map[string]any) bool {
			kw, _ := b["chat_template_kwargs"].(map[string]any)
			return kw["enable_thinking"] == false
		}},
		{"openai reasoning-effort", models.TypeOpenAICompatible, str(models.ThinkingOffReasoningEffort), func(b map[string]any) bool {
			return b["reasoning_effort"] == "none"
		}},
		{"openai allow", models.TypeOpenAICompatible, str(models.ThinkingAllow), func(b map[string]any) bool {
			return b["thinking"] == nil && b["chat_template_kwargs"] == nil && b["reasoning_effort"] == nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var bodies []map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req map[string]any
				_ = json.NewDecoder(r.Body).Decode(&req)
				bodies = append(bodies, req)
				if _, ok := req["response_format"]; ok {
					http.Error(w, `{"error":"unknown parameter response_format"}`, http.StatusBadRequest)
					return
				}
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"{}"}}]}`))
			}))
			defer srv.Close()
			spec := agent.ModelSpec{ConnectionType: tc.conn, ModelID: "m", Models: []agent.ModelDef{{ID: "m", Reasoning: true}}}
			m := &ChatModel{BaseURL: srv.URL, Model: "m", Extra: models.ThinkingBody(spec, tc.setting)}
			if _, _, err := m.Complete(context.Background(), "sys", "user"); err != nil {
				t.Fatal(err)
			}
			if len(bodies) != 2 {
				t.Fatalf("%d calls", len(bodies))
			}
			for _, b := range bodies {
				if !tc.check(b) {
					t.Fatalf("the fields are not in the body: %+v", b)
				}
			}
		})
	}
}

// MEM-CP-09, MEM-CP-12: the answer of the model is checked: the length of the summary, topics of the records only,
// numbers of the messages of the input only.
func TestParseAnswer(t *testing.T) {
	in := &compactInput{Limit: 50, Records: []compactNote{{Topics: []string{"Релиз 2.3"}}}, Messages: []compactMessage{{N: 1}, {N: 2}}}
	a, err := parseAnswer("```json\n{\"title\":\"Неделя\",\"summary\":\"Коротко.\",\"topics\":[\"релиз 2.3\",\"Чужая\"],\"keyMessages\":[2,7]}\n```", in, 20)
	if err != nil || len(a.Topics) != 1 || a.Topics[0] != "Релиз 2.3" || len(a.KeyMessages) != 1 || a.KeyMessages[0] != 2 {
		t.Fatalf("%+v %v", a, err)
	}
	if _, err := parseAnswer(`{"title":"t","summary":"`+strings.Repeat("я", 51)+`"}`, in, 20); err == nil {
		t.Fatal("a summary over the limit passed")
	}
	if _, err := parseAnswer("no json", in, 20); err == nil {
		t.Fatal("an answer without JSON passed")
	}
}
