package models

import (
	"strings"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// The directory of Nabu (FTR.NAB.CHAT-0001 R7, R12, tech §5): the models
// Nabu knows by type, with their capabilities. It is a source of capabilities
// below the provider's own description and an administrator's edit; a model
// the provider lists and the directory does not know gets the defaults and
// the probes of a check. Prices are USD per million tokens; a model whose
// price Nabu does not know has known = false and keeps the price at zero
// with the source "default".
type known struct {
	typ        string
	def        agent.ModelDef
	priceKnown bool
}

func strp(s string) *string { return &s }

// deepseekCompat follows DeepSeek's official configuration for Pi.
var deepseekCompat = map[string]any{
	"supportsStore": false, "supportsDeveloperRole": false, "maxTokensField": "max_tokens",
	"requiresReasoningContentOnAssistantMessages": true, "thinkingFormat": "deepseek",
}

var deepseekLevels = map[string]*string{"minimal": nil, "low": nil, "medium": nil, "high": strp("high"), "xhigh": strp("max")}

// zaiCompat: the thinking of GLM is switched on and off (thinking.type).
var zaiCompat = map[string]any{"supportsStore": false, "supportsDeveloperRole": false, "thinkingFormat": "zai"}

var zaiLevels = map[string]*string{"minimal": nil, "low": nil, "medium": nil, "high": strp("high")}

// gigachatCompat: the bridge speaks Chat Completions without the newer fields.
var gigachatCompat = map[string]any{"supportsStore": false, "supportsDeveloperRole": false, "maxTokensField": "max_tokens"}

// openAILevels are the levels of a reasoning model behind an
// OpenAI-compatible endpoint (reasoning_effort).
var openAILevels = map[string]*string{"minimal": nil, "low": strp("low"), "medium": strp("medium"), "high": strp("high")}

var text, textImage = []string{"text"}, []string{"text", "image"}

var directory = []known{
	// DeepSeek: V4.1 Flash sees images; the older names of Flash are aliases of it.
	{TypeDeepSeek, agent.ModelDef{ID: "deepseek-v4.1-flash", Name: "DeepSeek V4.1 Flash", Aliases: []string{"deepseek-v4-flash", "deepseek-flash"},
		ContextWindow: 1_000_000, MaxTokens: 384_000, Reasoning: true, Input: textImage, Tools: true,
		Cost: agent.Cost{Input: 0.14, Output: 0.28, CacheRead: 0.028}}, true},
	{TypeDeepSeek, agent.ModelDef{ID: "deepseek-v4-pro", Name: "DeepSeek V4 Pro", ContextWindow: 1_000_000, MaxTokens: 384_000,
		Reasoning: true, Input: text, Tools: true, Cost: agent.Cost{Input: 1.74, Output: 3.48, CacheRead: 0.145}}, true},

	// GigaChat: billed by packages of tokens — the price is not per token.
	{TypeGigaChat, agent.ModelDef{ID: "GigaChat-2-Max", Name: "GigaChat 2 Max", Aliases: []string{"GigaChat-Max"},
		ContextWindow: 131_072, MaxTokens: 32_768, Input: textImage, Tools: true}, false},
	{TypeGigaChat, agent.ModelDef{ID: "GigaChat-2-Pro", Name: "GigaChat 2 Pro", Aliases: []string{"GigaChat-Pro"},
		ContextWindow: 131_072, MaxTokens: 32_768, Input: textImage, Tools: true}, false},
	{TypeGigaChat, agent.ModelDef{ID: "GigaChat-2", Name: "GigaChat 2 Lite", Aliases: []string{"GigaChat"},
		ContextWindow: 131_072, MaxTokens: 32_768, Input: text, Tools: true}, false},
	{TypeGigaChat, agent.ModelDef{ID: "EmbeddingsGigaR", Name: "Embeddings GigaR", ContextWindow: 4_096, MaxTokens: 0, Input: text, Embeddings: true}, false},
	{TypeGigaChat, agent.ModelDef{ID: "Embeddings", Name: "Embeddings", ContextWindow: 512, MaxTokens: 0, Input: text, Embeddings: true}, false},

	// Z.AI: GLM; the models with "v" see images.
	{TypeZAI, agent.ModelDef{ID: "glm-5.3", Name: "GLM-5.3", ContextWindow: 200_000, MaxTokens: 128_000, Reasoning: true, Input: text, Tools: true}, false},
	{TypeZAI, agent.ModelDef{ID: "glm-4.6", Name: "GLM-4.6", ContextWindow: 200_000, MaxTokens: 128_000, Reasoning: true, Input: text, Tools: true}, false},
	{TypeZAI, agent.ModelDef{ID: "glm-4.5-air", Name: "GLM-4.5-Air", ContextWindow: 128_000, MaxTokens: 96_000, Reasoning: true, Input: text, Tools: true}, false},
	{TypeZAI, agent.ModelDef{ID: "glm-5v-turbo", Name: "GLM-5V Turbo", ContextWindow: 128_000, MaxTokens: 32_000, Reasoning: true, Input: textImage, Tools: true}, false},
	{TypeZAI, agent.ModelDef{ID: "glm-4.6v", Name: "GLM-4.6V", ContextWindow: 128_000, MaxTokens: 32_000, Reasoning: true, Input: textImage, Tools: true}, false},
	{TypeZAI, agent.ModelDef{ID: "glm-4.6v-flash", Name: "GLM-4.6V Flash", ContextWindow: 128_000, MaxTokens: 32_000, Input: textImage, Tools: true}, false},
}

// lookup finds a model of the directory by id or alias, the type's own models
// first: a LiteLLM endpoint that proxies DeepSeek gets DeepSeek's description.
func lookup(typ, id string) (known, bool) {
	match := func(k known) bool {
		if strings.EqualFold(k.def.ID, id) {
			return true
		}
		for _, a := range k.def.Aliases {
			if strings.EqualFold(a, id) {
				return true
			}
		}
		return false
	}
	for _, k := range directory {
		if k.typ == typ && match(k) {
			return k, true
		}
	}
	for _, k := range directory {
		if match(k) {
			return k, true
		}
	}
	// "deepseek/deepseek-v4-flash", "openai/glm-4.6": LiteLLM names with a provider prefix
	if i := strings.LastIndexByte(id, '/'); i >= 0 && i < len(id)-1 {
		return lookup(typ, id[i+1:])
	}
	return known{}, false
}

// ofType lists the directory of a type (the models offered when the provider
// does not list them).
func ofType(typ string) []known {
	var out []known
	for _, k := range directory {
		if k.typ == typ {
			out = append(out, k)
		}
	}
	return out
}

// compatOf is the compatibility Pi needs for a model of a connection type.
func compatOf(typ string) map[string]any {
	switch typ {
	case TypeDeepSeek:
		return deepseekCompat
	case TypeZAI:
		return zaiCompat
	case TypeGigaChat:
		return gigachatCompat
	}
	return nil
}

// levelsOf is the map of reasoning levels of a reasoning model of a type.
func levelsOf(typ string) map[string]*string {
	switch typ {
	case TypeDeepSeek:
		return deepseekLevels
	case TypeZAI:
		return zaiLevels
	case TypeGigaChat:
		return nil
	}
	return openAILevels
}
