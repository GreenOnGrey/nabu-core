package models

import (
	"maps"
	"slices"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// The values of the thinking-off setting of the memory of communications (FTR.NAB.CMN-0003 tech, decision 58;
// FTR.NAB.MEM-0001 tech, decision 23): how a run of the collector and a call of the compaction switch the thinking
// of the model off, or let it think.
const (
	// ThinkingOffProvider switches the thinking off with the provider's own parameter: `thinking: {type:
	// "disabled"}` of DeepSeek and Z.AI.
	ThinkingOffProvider = "provider"
	// ThinkingOffChatTemplate sends chat_template_kwargs.enable_thinking = false (Qwen behind vLLM or LiteLLM).
	ThinkingOffChatTemplate = "chat-template"
	// ThinkingOffReasoningEffort sends reasoning_effort = "none".
	ThinkingOffReasoningEffort = "reasoning-effort"
	// ThinkingAllow lets the model think: the off parameter is not sent and the default of the provider applies.
	ThinkingAllow = "allow"
)

// ThinkingOffValues are the accepted values of the setting: nil is the default of the connection type.
var ThinkingOffValues = []string{ThinkingOffProvider, ThinkingOffChatTemplate, ThinkingOffReasoningEffort, ThinkingAllow}

// thinkingOffWays are the ways that describe the model for Pi; the provider's own parameter needs no description.
var thinkingOffWays = []string{ThinkingOffChatTemplate, ThinkingOffReasoningEffort}

// ThinkingOffState tells whether the setting switches the thinking of the model off, and how: the provider's own
// parameter, chat-template or reasoning-effort. A nil setting is the default of the type: off for DeepSeek and
// Z.AI, allowed for the others. The GigaChat connection is the public API, which does not let Nabu manage the
// thinking of its models (FTR.NAB.CMN-0003 tech, decision 58), so it is never switched off there.
func ThinkingOffState(setting *string, connType string) (off bool, way string) {
	if connType == TypeGigaChat {
		return false, ""
	}
	if setting != nil {
		switch *setting {
		case ThinkingAllow:
			return false, ""
		case ThinkingOffProvider:
			if connType == TypeDeepSeek || connType == TypeZAI {
				return true, ThinkingOffProvider
			}
			return false, ""
		default:
			if !slices.Contains(thinkingOffWays, *setting) {
				return false, "" // an unknown value never comes from the administration (comm.SettingsPatch)
			}
			if connType == TypeOpenAICompatible {
				return true, *setting
			}
			if connType == TypeDeepSeek || connType == TypeZAI {
				// a way chosen for an OpenAI-compatible connection while the model is DeepSeek or Z.AI: their
				// own parameter switches the thinking off
				return true, ThinkingOffProvider
			}
			return false, ""
		}
	}
	if connType == TypeDeepSeek || connType == TypeZAI {
		return true, ThinkingOffProvider
	}
	return false, ""
}

// ApplyThinking returns the spec of a run whose thinking follows the setting (FTR.NAB.CMN-0003 tech, decision 58):
// the copy of the model tells Pi how to switch the thinking off, or how to stop switching it off when the setting
// allows it. The description of the connection stays as it is, so the same model may think in the chat (A16).
func ApplyThinking(spec agent.ModelSpec, setting *string) agent.ModelSpec {
	def, ok := modelDef(spec)
	if !ok {
		return spec
	}
	if off, way := ThinkingOffState(setting, spec.ConnectionType); off {
		return WithThinkingOff(spec, way)
	}
	if !def.Reasoning {
		return spec // a model that cannot think has nothing to allow
	}
	// the setting allows the thinking: Pi must not force the off of its own compat
	switch spec.ConnectionType {
	case TypeDeepSeek, TypeZAI:
		return withModelDef(spec, def.ID, func(d *agent.ModelDef) {
			compat := maps.Clone(d.Compat)
			if compat == nil {
				compat = map[string]any{}
			}
			compat["thinkingFormat"] = "openai" // at level off Pi sends nothing, so the provider's default applies
			d.Compat = compat
		})
	}
	return spec
}

// ThinkingBody is the fields of the body of a direct chat completions call that switch the thinking off
// (FTR.NAB.MEM-0001 tech, decision 23): the same shapes a run of the collector sends. nil — the model does not
// think, the setting allows the thinking, or the public GigaChat API cannot manage it.
func ThinkingBody(spec agent.ModelSpec, setting *string) map[string]any {
	def, ok := modelDef(spec)
	if !ok {
		return nil
	}
	off, way := ThinkingOffState(setting, spec.ConnectionType)
	if !off {
		return nil
	}
	switch spec.ConnectionType {
	case TypeDeepSeek, TypeZAI:
		if !def.Reasoning {
			return nil
		}
		return map[string]any{"thinking": map[string]string{"type": "disabled"}}
	case TypeOpenAICompatible:
		switch way {
		case ThinkingOffChatTemplate:
			return map[string]any{"chat_template_kwargs": map[string]any{"enable_thinking": false}}
		case ThinkingOffReasoningEffort:
			return map[string]any{"reasoning_effort": "none"}
		}
	}
	return nil
}

// WithThinkingOff returns the spec of a run at the level off whose model tells Pi how to say so: without a compat
// of its own Pi sends nothing at level off to an OpenAI-compatible endpoint, and Qwen3 then thinks by its template.
// Only the model of this run changes, in a copy: the connection keeps its description, so the same model may think
// in the chat. An empty way, another connection type (DeepSeek and Z.AI already switch it off, the public GigaChat
// API cannot manage it) or an unknown model leave the spec as it is.
func WithThinkingOff(spec agent.ModelSpec, way string) agent.ModelSpec {
	if spec.ConnectionType != TypeOpenAICompatible || !slices.Contains(thinkingOffWays, way) {
		return spec
	}
	if _, ok := modelDef(spec); !ok {
		return spec
	}
	spec = withModelDef(spec, spec.ModelID, func(d *agent.ModelDef) {
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
	})
	spec.Thinking = "off"
	return spec
}

// modelDef is the definition of the model of the spec.
func modelDef(spec agent.ModelSpec) (agent.ModelDef, bool) {
	i := slices.IndexFunc(spec.Models, func(m agent.ModelDef) bool { return m.ID == spec.ModelID })
	if i < 0 {
		return agent.ModelDef{}, false
	}
	return spec.Models[i], true
}

// withModelDef edits the copy of one model of the spec.
func withModelDef(spec agent.ModelSpec, id string, edit func(*agent.ModelDef)) agent.ModelSpec {
	spec.Models = slices.Clone(spec.Models)
	for i := range spec.Models {
		if spec.Models[i].ID == id {
			edit(&spec.Models[i])
			break
		}
	}
	return spec
}
