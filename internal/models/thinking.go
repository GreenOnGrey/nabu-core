package models

import (
	"maps"
	"slices"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// The ways a run switches the thinking of an OpenAI-compatible model off explicitly (FTR.NAB.CMN-0003 tech,
// decision 58).
const (
	// ThinkingOffChatTemplate sends chat_template_kwargs.enable_thinking = false (Qwen behind vLLM or LiteLLM).
	ThinkingOffChatTemplate = "chat-template"
	// ThinkingOffReasoningEffort sends reasoning_effort = "none".
	ThinkingOffReasoningEffort = "reasoning-effort"
)

// ThinkingOffWays are the accepted values of a way.
var ThinkingOffWays = []string{ThinkingOffChatTemplate, ThinkingOffReasoningEffort}

// WithThinkingOff returns the spec of a run at the level off whose model tells Pi how to say so: without a
// compat of its own Pi sends nothing at level off to an OpenAI-compatible endpoint, and Qwen3 then thinks by its
// template. Only the model of this run changes, in a copy: the connection keeps its description, so the same
// model may think in the chat. An empty way, another connection type (DeepSeek and Z.AI already switch it off,
// GigaChat does not think) or an unknown model leave the spec as it is.
func WithThinkingOff(spec agent.ModelSpec, way string) agent.ModelSpec {
	if spec.ConnectionType != TypeOpenAICompatible || !slices.Contains(ThinkingOffWays, way) {
		return spec
	}
	i := slices.IndexFunc(spec.Models, func(m agent.ModelDef) bool { return m.ID == spec.ModelID })
	if i < 0 {
		return spec
	}
	spec.Models = slices.Clone(spec.Models)
	d := spec.Models[i]
	d.Reasoning = true
	d.Compat = maps.Clone(d.Compat)
	if d.Compat == nil {
		d.Compat = map[string]any{}
	}
	d.ThinkingLevelMap = maps.Clone(d.ThinkingLevelMap)
	if d.ThinkingLevelMap == nil {
		d.ThinkingLevelMap = map[string]*string{}
	}
	switch way {
	case ThinkingOffChatTemplate:
		d.Compat["thinkingFormat"] = "qwen-chat-template"
	case ThinkingOffReasoningEffort:
		d.Compat["thinkingFormat"] = "openai"
		d.Compat["supportsReasoningEffort"] = true
		none := "none"
		d.ThinkingLevelMap["off"] = &none
	}
	spec.Models[i] = d
	spec.Thinking = "off"
	return spec
}
