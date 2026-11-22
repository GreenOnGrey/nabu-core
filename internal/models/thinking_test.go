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

// FTR.NAB.CMN-0003 tech, decision 58; FTR.NAB.MEM-0001 tech, decision 23: the setting of the memory governs the
// collection and the compaction of every connection type; the public GigaChat API never switches it off.
func TestThinkingOffState(t *testing.T) {
	str := func(s string) *string { return &s }
	for _, tc := range []struct {
		name    string
		setting *string
		typ     string
		off     bool
		way     string
	}{
		{"default deepseek", nil, TypeDeepSeek, true, ThinkingOffProvider},
		{"default zai", nil, TypeZAI, true, ThinkingOffProvider},
		{"default openai", nil, TypeOpenAICompatible, false, ""},
		{"default gigachat", nil, TypeGigaChat, false, ""},
		{"allow deepseek", str(ThinkingAllow), TypeDeepSeek, false, ""},
		{"provider deepseek", str(ThinkingOffProvider), TypeDeepSeek, true, ThinkingOffProvider},
		{"provider openai", str(ThinkingOffProvider), TypeOpenAICompatible, false, ""},
		{"chat-template openai", str(ThinkingOffChatTemplate), TypeOpenAICompatible, true, ThinkingOffChatTemplate},
		{"reasoning-effort openai", str(ThinkingOffReasoningEffort), TypeOpenAICompatible, true, ThinkingOffReasoningEffort},
		{"a way of openai on deepseek", str(ThinkingOffChatTemplate), TypeDeepSeek, true, ThinkingOffProvider},
		{"a way of openai on gigachat", str(ThinkingOffChatTemplate), TypeGigaChat, false, ""},
	} {
		off, way := ThinkingOffState(tc.setting, tc.typ)
		if off != tc.off || way != tc.way {
			t.Errorf("%s: %v %q, want %v %q", tc.name, off, way, tc.off, tc.way)
		}
	}
}

// The spec of a run at a level off: the setting allows the thinking — Pi must not force the off of its compat.
func TestApplyThinkingAllows(t *testing.T) {
	def := agent.ModelDef{ID: "m", Reasoning: true,
		ThinkingLevelMap: map[string]*string{"high": func() *string { s := "high"; return &s }()},
		Compat:           map[string]any{"thinkingFormat": "deepseek"}}
	spec := agent.ModelSpec{ConnectionType: TypeDeepSeek, ModelID: "m", Thinking: "off", Models: []agent.ModelDef{def}}
	allow := ThinkingAllow
	got := ApplyThinking(spec, &allow)
	if got.Models[0].Compat["thinkingFormat"] != "openai" {
		t.Fatalf("the deepseek compat: %+v", got.Models[0].Compat)
	}
	if spec.Models[0].Compat["thinkingFormat"] != "deepseek" {
		t.Fatal("the connection's spec changed")
	}
	if got.Models[0].ThinkingLevelMap["high"] == nil || *got.Models[0].ThinkingLevelMap["high"] != "high" {
		t.Fatalf("the levels changed: %+v", got.Models[0].ThinkingLevelMap)
	}
	zai := agent.ModelSpec{ConnectionType: TypeZAI, ModelID: "m", Thinking: "off",
		Models: []agent.ModelDef{{ID: "m", Reasoning: true, Compat: map[string]any{"thinkingFormat": "zai"}}}}
	got = ApplyThinking(zai, &allow)
	if got.Models[0].Compat["thinkingFormat"] != "openai" || zai.Models[0].Compat["thinkingFormat"] != "zai" {
		t.Fatalf("the zai compat: %+v", got.Models[0].Compat)
	}
	openai := agent.ModelSpec{ConnectionType: TypeOpenAICompatible, ModelID: "m", Thinking: "off",
		Models: []agent.ModelDef{{ID: "m", Reasoning: true}}}
	if got = ApplyThinking(openai, &allow); got.Models[0].Compat != nil {
		t.Fatal("the OpenAI-compatible spec changed")
	}
	if got = ApplyThinking(zai, nil); got.Models[0].Compat["thinkingFormat"] != "zai" {
		t.Fatal("the default of Z.AI changed")
	}
}

// The body of the direct call of the compaction follows the same setting.
func TestThinkingBody(t *testing.T) {
	str := func(s string) *string { return &s }
	openai := agent.ModelSpec{ConnectionType: TypeOpenAICompatible, ModelID: "m", Models: []agent.ModelDef{{ID: "m", Reasoning: true}}}
	if b := ThinkingBody(openai, str(ThinkingOffChatTemplate)); b["chat_template_kwargs"].(map[string]any)["enable_thinking"] != false {
		t.Fatalf("chat-template: %+v", b)
	}
	if b := ThinkingBody(openai, str(ThinkingOffReasoningEffort)); b["reasoning_effort"] != "none" {
		t.Fatalf("reasoning-effort: %+v", b)
	}
	if b := ThinkingBody(openai, nil); b != nil {
		t.Fatalf("openai without a way: %+v", b)
	}
	deepseek := agent.ModelSpec{ConnectionType: TypeDeepSeek, ModelID: "m", Models: []agent.ModelDef{{ID: "m", Reasoning: true}}}
	if b := ThinkingBody(deepseek, nil); b["thinking"].(map[string]string)["type"] != "disabled" {
		t.Fatalf("deepseek: %+v", b)
	}
	if b := ThinkingBody(deepseek, str(ThinkingAllow)); b != nil {
		t.Fatalf("deepseek allowing: %+v", b)
	}
	zai := agent.ModelSpec{ConnectionType: TypeZAI, ModelID: "m", Models: []agent.ModelDef{{ID: "m", Reasoning: true}}}
	if b := ThinkingBody(zai, str(ThinkingOffProvider)); b["thinking"].(map[string]string)["type"] != "disabled" {
		t.Fatalf("zai: %+v", b)
	}
	gigachat := agent.ModelSpec{ConnectionType: TypeGigaChat, ModelID: "m", Models: []agent.ModelDef{{ID: "m", Reasoning: true}}}
	if b := ThinkingBody(gigachat, str(ThinkingOffChatTemplate)); b != nil {
		t.Fatalf("gigachat: %+v", b)
	}
	plain := agent.ModelSpec{ConnectionType: TypeDeepSeek, ModelID: "m", Models: []agent.ModelDef{{ID: "m"}}}
	if b := ThinkingBody(plain, nil); b != nil {
		t.Fatalf("a model without reasoning: %+v", b)
	}
	// the explicit way of an OpenAI-compatible connection applied before the capability was known: it stays
	plainOpenAI := agent.ModelSpec{ConnectionType: TypeOpenAICompatible, ModelID: "m", Models: []agent.ModelDef{{ID: "m"}}}
	if b := ThinkingBody(plainOpenAI, str(ThinkingOffChatTemplate)); b["chat_template_kwargs"] == nil {
		t.Fatalf("the explicit way of a model without the capability: %+v", b)
	}
	if got := ApplyThinking(plainOpenAI, str(ThinkingOffChatTemplate)); !got.Models[0].Reasoning || got.Models[0].Compat["thinkingFormat"] != "qwen-chat-template" {
		t.Fatalf("the run of a model without the capability: %+v", got.Models[0])
	}
}
