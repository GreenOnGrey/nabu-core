package pi

import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent/fakellm"
)

// ADM-06 (FTR.NAB.CMN-0003 tech, decision 58): at the level off a real Pi says to an OpenAI-compatible model that
// it must not think, in the way of the run's model; without a way it sends neither parameter.
func TestThinkingOffWithPi(t *testing.T) {
	cmd := strings.Fields(os.Getenv("NABU_PI_CMD"))
	if len(cmd) == 0 {
		t.Skip("NABU_PI_CMD is not set")
	}
	low := "low"
	for _, tc := range []struct{ way, check string }{{"", "none"}, {models.ThinkingOffChatTemplate, "kwargs"}, {models.ThinkingOffReasoningEffort, "effort"}} {
		t.Run("way="+tc.way, func(t *testing.T) {
			llm := &fakellm.Server{}
			srv := httptest.NewServer(llm)
			defer srv.Close()
			spec := agent.ModelSpec{Provider: "nabu-test", API: "openai-completions", BaseURL: srv.URL, ModelID: "qwen", Thinking: "off",
				ConnectionType: models.TypeOpenAICompatible, Models: []agent.ModelDef{{ID: "qwen", ContextWindow: 32000, MaxTokens: 4096,
					Input: []string{"text"}, Reasoning: true, ThinkingLevelMap: map[string]*string{"low": &low}}}}
			spec = models.WithThinkingOff(spec, tc.way)
			out := CheckLLM(context.Background(), Runtime{Command: cmd, Options: Options{Path: os.Getenv("PATH")}}, t.TempDir(),
				agent.LLMCheckRequest{Model: spec, Secrets: agent.Secrets{LLMKey: "sk-test"}})
			if len(out.Results) != 1 || !out.Results[0].OK {
				t.Fatalf("the check failed: %+v", out.Results)
			}
			reqs := llm.Requests()
			if len(reqs) == 0 {
				t.Fatal("no request reached the model")
			}
			for _, r := range reqs {
				kw, effort := r.ChatTemplateKwargs, r.ReasoningEffort
				switch tc.check {
				case "none":
					if kw != nil || effort != nil {
						t.Fatalf("a thinking parameter without a way: %v %v", kw, effort)
					}
				case "kwargs":
					if v, ok := kw["enable_thinking"]; !ok || v != false {
						t.Fatalf("chat_template_kwargs: %v", kw)
					}
				case "effort":
					if effort == nil || *effort != "none" {
						t.Fatalf("reasoning_effort: %v", effort)
					}
				}
			}
		})
	}
}

// The setting allows the thinking (FTR.NAB.CMN-0003 tech, decision 58): the deepseek and zai compat of Pi must not
// force the off at the level off, so the request carries no thinking parameter.
func TestAllowThinkingWithPi(t *testing.T) {
	cmd := strings.Fields(os.Getenv("NABU_PI_CMD"))
	if len(cmd) == 0 {
		t.Skip("NABU_PI_CMD is not set")
	}
	allow := models.ThinkingAllow
	for _, tc := range []struct {
		name, typ string
		compat    map[string]any
	}{
		{"deepseek", models.TypeDeepSeek, map[string]any{"thinkingFormat": "deepseek", "requiresReasoningContentOnAssistantMessages": true}},
		{"zai", models.TypeZAI, map[string]any{"thinkingFormat": "zai"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			llm := &fakellm.Server{}
			srv := httptest.NewServer(llm)
			defer srv.Close()
			spec := agent.ModelSpec{Provider: "nabu-test", API: "openai-completions", BaseURL: srv.URL, ModelID: "m", Thinking: "off",
				ConnectionType: tc.typ, Models: []agent.ModelDef{{ID: "m", ContextWindow: 32000, MaxTokens: 4096,
					Input: []string{"text"}, Reasoning: true, Compat: tc.compat}}}
			spec = models.ApplyThinking(spec, &allow)
			out := CheckLLM(context.Background(), Runtime{Command: cmd, Options: Options{Path: os.Getenv("PATH")}}, t.TempDir(),
				agent.LLMCheckRequest{Model: spec, Secrets: agent.Secrets{LLMKey: "sk-test"}})
			if len(out.Results) != 1 || !out.Results[0].OK {
				t.Fatalf("the check failed: %+v", out.Results)
			}
			for _, r := range llm.Requests() {
				if len(r.Thinking) != 0 || r.ReasoningEffort != nil || r.ChatTemplateKwargs != nil {
					t.Fatalf("the thinking is forced: thinking=%v effort=%v kwargs=%v", r.Thinking, r.ReasoningEffort, r.ChatTemplateKwargs)
				}
			}
		})
	}
}
