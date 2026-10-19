package models

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// Sources of a capability (FTR.NAB.CHAT-0001 R7) and the fields that have one.
const (
	SrcManual   = "manual"
	SrcProvider = "provider"
	SrcCatalog  = "catalog"
	SrcProbe    = "probe"
	SrcDefault  = "default"
)

// Fields of the capabilities of a model.
const (
	FImage         = "image"
	FTools         = "tools"
	FReasoning     = "reasoning"
	FContextWindow = "contextWindow"
	FMaxTokens     = "maxTokens"
	FCost          = "cost"
)

var fields = []string{FImage, FTools, FReasoning, FContextWindow, FMaxTokens, FCost}

// providerInfo is what a provider says of a model (LiteLLM /model/info,
// GigaChat's type); nil fields are unknown.
type providerInfo struct {
	Name          string
	Embeddings    bool
	Image         *bool
	Tools         *bool
	Reasoning     *bool
	ContextWindow int64
	MaxTokens     int64
	Cost          *agent.Cost
}

// CapsPatch is an administrator's edit of the capabilities of one model (R8);
// Reset returns fields to their sources.
type CapsPatch struct {
	Image         *bool       `json:"image,omitempty"`
	Tools         *bool       `json:"tools,omitempty"`
	Reasoning     *bool       `json:"reasoning,omitempty"`
	ContextWindow *int64      `json:"contextWindow,omitempty"`
	MaxTokens     *int64      `json:"maxTokens,omitempty"`
	Cost          *agent.Cost `json:"cost,omitempty"`
	Reset         []string    `json:"reset,omitempty"`
}

// ModelRef chooses a model of a connection with an optional edit; in JSON it
// is the id or {id, overrides}.
type ModelRef struct {
	ID        string     `json:"id"`
	Overrides *CapsPatch `json:"overrides,omitempty"`
}

// UnmarshalJSON accepts "id" as well as {"id": …}.
func (m *ModelRef) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		*m = ModelRef{ID: s}
		return nil
	}
	type plain ModelRef
	return json.Unmarshal(b, (*plain)(m))
}

func set(d *agent.ModelDef, f string, v any) {
	switch f {
	case FImage:
		if v.(bool) {
			if !d.HasInput("image") {
				d.Input = append(slices.Clone(d.Input), "image")
			}
		} else {
			d.Input = slices.DeleteFunc(slices.Clone(d.Input), func(s string) bool { return s == "image" })
		}
	case FTools:
		d.Tools = v.(bool)
	case FReasoning:
		d.Reasoning = v.(bool)
	case FContextWindow:
		d.ContextWindow = v.(int64)
	case FMaxTokens:
		d.MaxTokens = v.(int64)
	case FCost:
		d.Cost = v.(agent.Cost)
	}
}

func get(d agent.ModelDef, f string) any {
	switch f {
	case FImage:
		return d.HasInput("image")
	case FTools:
		return d.Tools
	case FReasoning:
		return d.Reasoning
	case FContextWindow:
		return d.ContextWindow
	case FMaxTokens:
		return d.MaxTokens
	}
	return d.Cost
}

// build describes a model of a connection from its sources by priority:
// manual > provider > catalog > probe > default (R7). old is the stored
// description (its manual and probe values and, without a fresh info, its
// provider values); info is a fresh description of the provider.
func build(typ, id string, info *providerInfo, old *agent.ModelDef) agent.ModelDef {
	d := agent.ModelDef{ID: id, Name: id, ContextWindow: 128_000, MaxTokens: 16_000, Input: []string{"text"}, Tools: true,
		Sources: map[string]string{}}
	for _, f := range fields {
		d.Sources[f] = SrcDefault
	}
	if old != nil {
		d.Name = old.Name
		for _, f := range fields {
			if old.Sources[f] == SrcProbe {
				set(&d, f, get(*old, f))
				d.Sources[f] = SrcProbe
			}
		}
	}
	k, inCatalog := lookup(typ, id)
	if inCatalog {
		c := k.def
		d.Name, d.Aliases, d.Embeddings = c.Name, c.Aliases, c.Embeddings
		for _, f := range fields {
			if f == FCost && !k.priceKnown {
				continue
			}
			set(&d, f, get(c, f))
			d.Sources[f] = SrcCatalog
		}
		if c.Embeddings {
			d.Tools = false
		}
	}
	// a model of vectors the catalog does not know is told by its name
	if !inCatalog && agent.EmbeddingsName(id) {
		d.Embeddings, d.Tools = true, false
		d.Sources[FTools] = SrcCatalog
	}
	switch {
	case info != nil:
		if info.Name != "" && !inCatalog {
			d.Name = info.Name
		}
		if info.Embeddings {
			d.Embeddings, d.Tools = true, false
			d.Sources[FTools] = SrcProvider
		}
		apply := func(f string, v any, known bool) {
			if known {
				set(&d, f, v)
				d.Sources[f] = SrcProvider
			}
		}
		if info.Image != nil {
			apply(FImage, *info.Image, true)
		}
		if info.Tools != nil && !info.Embeddings {
			apply(FTools, *info.Tools, true)
		}
		if info.Reasoning != nil {
			apply(FReasoning, *info.Reasoning, true)
		}
		apply(FContextWindow, info.ContextWindow, info.ContextWindow > 0)
		apply(FMaxTokens, info.MaxTokens, info.MaxTokens > 0)
		if info.Cost != nil {
			apply(FCost, *info.Cost, true)
		}
	case old != nil:
		for _, f := range fields {
			if old.Sources[f] == SrcProvider {
				set(&d, f, get(*old, f))
				d.Sources[f] = SrcProvider
			}
		}
		if old.Embeddings {
			d.Embeddings = true
		}
	}
	if old != nil {
		for _, f := range old.Overrides {
			if slices.Contains(fields, f) {
				set(&d, f, get(*old, f))
				d.Sources[f] = SrcManual
				d.Overrides = append(d.Overrides, f)
			}
		}
		d.Gone = old.Gone
	}
	finish(typ, &d)
	return d
}

// finish adds what Pi needs: the compatibility and the levels of reasoning.
func finish(typ string, d *agent.ModelDef) {
	d.Compat = compatOf(typ)
	d.ThinkingLevelMap = nil
	if d.Reasoning {
		d.ThinkingLevelMap = levelsOf(typ)
		if d.ThinkingLevelMap == nil {
			d.Reasoning = false // GigaChat has no reasoning levels
		}
	}
	if len(d.Input) == 0 {
		d.Input = []string{"text"}
	}
}

// edit applies an administrator's patch: the fields it sets become manual,
// the fields it resets go back to their sources.
func edit(typ string, d agent.ModelDef, p *CapsPatch) agent.ModelDef {
	if p == nil {
		return d
	}
	if len(p.Reset) > 0 {
		d.Overrides = slices.DeleteFunc(slices.Clone(d.Overrides), func(f string) bool { return slices.Contains(p.Reset, f) })
		for _, f := range p.Reset {
			if d.Sources[f] == SrcManual {
				d.Sources[f] = SrcDefault // build takes it from its source again
			}
		}
		old := d
		d = build(typ, d.ID, nil, &old)
	}
	mark := func(f string, v any) {
		set(&d, f, v)
		if d.Sources == nil {
			d.Sources = map[string]string{}
		}
		d.Sources[f] = SrcManual
		if !slices.Contains(d.Overrides, f) {
			d.Overrides = append(d.Overrides, f)
		}
	}
	if p.Image != nil {
		mark(FImage, *p.Image)
	}
	if p.Tools != nil {
		mark(FTools, *p.Tools)
	}
	if p.Reasoning != nil {
		mark(FReasoning, *p.Reasoning)
	}
	if p.ContextWindow != nil && *p.ContextWindow > 0 {
		mark(FContextWindow, *p.ContextWindow)
	}
	if p.MaxTokens != nil && *p.MaxTokens >= 0 {
		mark(FMaxTokens, *p.MaxTokens)
	}
	if p.Cost != nil {
		mark(FCost, *p.Cost)
	}
	finish(typ, &d)
	return d
}

// Usable reports whether a personal agent may run on the model (R8): it calls
// tools, chats and the provider still lists it.
func Usable(d agent.ModelDef) bool { return d.Tools && !d.Embeddings && !d.Gone }

// embeddingsByName marks a model of vectors stored before its kind was known
// (FTR.NAB.CHAT-0001 R8): an administrator who switched tools on by hand keeps it.
func embeddingsByName(d *agent.ModelDef) {
	if d.Embeddings || !agent.EmbeddingsName(d.ID) || slices.Contains(d.Overrides, FTools) {
		return
	}
	d.Embeddings, d.Tools = true, false
	if d.Sources == nil {
		d.Sources = map[string]string{}
	}
	d.Sources[FTools] = SrcCatalog
}

// normalizeID trims a model id.
func normalizeID(id string) string { return strings.TrimSpace(id) }
