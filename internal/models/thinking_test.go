package models

import (
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// FTR.NAB.CMN-0003 tech, decision 58 (ADM-06, ADM-07): the model of a run says how Pi switches the thinking off;
// the connection's description and other types stay as they are.
func TestWithThinkingOff(t *testing.T) {
	low := "low"
	base := agent.ModelSpec{ConnectionType: TypeOpenAICompatible, ModelID: "qwen", Thinking: "off", Models: []agent.ModelDef{
		{ID: "other"}, {ID: "qwen", ThinkingLevelMap: map[string]*string{"low": &low}}}}

	got := WithThinkingOff(base, ThinkingOffChatTemplate)
	if d := got.Models[1]; !d.Reasoning || d.Compat["thinkingFormat"] != "qwen-chat-template" || got.Thinking != "off" {
		t.Fatalf("chat-template: %+v", d)
	}
	if base.Models[1].Compat != nil || base.Models[1].Reasoning {
		t.Fatal("the description of the connection changed")
	}
	got = WithThinkingOff(base, ThinkingOffReasoningEffort)
	if d := got.Models[1]; !d.Reasoning || d.Compat["supportsReasoningEffort"] != true || d.ThinkingLevelMap["off"] == nil ||
		*d.ThinkingLevelMap["off"] != "none" || *d.ThinkingLevelMap["low"] != "low" {
		t.Fatalf("reasoning-effort: %+v", d)
	}
	if _, ok := base.Models[1].ThinkingLevelMap["off"]; ok {
		t.Fatal("the levels of the connection changed")
	}
	for name, spec := range map[string]agent.ModelSpec{
		"deepseek":      {ConnectionType: TypeDeepSeek, ModelID: "qwen", Models: base.Models},
		"unknown model": {ConnectionType: TypeOpenAICompatible, ModelID: "gone", Models: base.Models},
	} {
		if got := WithThinkingOff(spec, ThinkingOffChatTemplate); got.Models[1].Compat != nil {
			t.Errorf("%s: changed", name)
		}
	}
	if got := WithThinkingOff(base, "always"); got.Models[1].Compat != nil {
		t.Error("an unknown way changed the model")
	}
}
