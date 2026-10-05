// Package importer moves the agent settings of Hammurapi (FTR.HMR.CMN-0004)
// into Nabu: POST /client/v1/import/hammurapi-agent, for clients with the
// import right (FTR.NAB.CMN-0001 R25, arch §12; FTR.HMR.CMN-0006 R11, tech §4).
// It creates model connections, a service agent per scenario, MCP servers of
// the catalog and the git source of skills; a repeated import updates the
// same objects instead of duplicating them (HMR-06).
package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/catalog"
	"github.com/GreenOnGrey/nabu-core/internal/models"
	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/services"
)

// Choice is a model of a scenario.
type Choice struct {
	ConnectionRef string `json:"connectionRef"`
	Model         string `json:"model"`
	Thinking      string `json:"thinking"`
}

// Body is the request (FTR.HMR.CMN-0006 tech §4).
type Body struct {
	Connections []struct {
		Ref     string            `json:"ref"`
		Name    string            `json:"name"`
		Type    string            `json:"type"`
		BaseURL string            `json:"baseUrl"`
		APIKey  string            `json:"apiKey"`
		Models  []json.RawMessage `json:"models"`
	} `json:"connections"`
	Scenarios map[string]Choice `json:"scenarios"`
	Default   *Choice           `json:"default"`
	MCP       []struct {
		Name      string            `json:"name"`
		URL       string            `json:"url"`
		Headers   map[string]string `json:"headers"`
		Exposure  string            `json:"exposure"`
		Scenarios []string          `json:"scenarios"`
	} `json:"mcp"`
	Skills *struct {
		Repo        string              `json:"repo"`
		Path        string              `json:"path"`
		Ref         string              `json:"ref"`
		Assignments map[string][]string `json:"assignments"`
	} `json:"skills"`
}

// Skipped is an object that was not imported, with the reason.
type Skipped struct {
	Object string `json:"object"`
	Reason string `json:"reason"`
}

// Report is the answer.
type Report struct {
	ServiceAgents map[string]string `json:"serviceAgents"`
	Connections   []string          `json:"connections"`
	MCP           []string          `json:"mcp"`
	Skills        string            `json:"skills,omitempty"`
	Skipped       []Skipped         `json:"skipped"`
}

// Scenarios of Hammurapi that service agents serve (the chat is the
// personal agent and needs no service agent).
var Scenarios = []string{"issue_analysis", "gate_generation", "conformance_check", "codegen", "review_update", "rollback_revert"}

// codeScenarios work on a checkout in Hammurapi's runner (R26).
var codeScenarios = map[string]bool{"codegen": true, "review_update": true, "rollback_revert": true}

var descriptions = map[string]string{
	"issue_analysis":    "Hammurapi: analysis of an issue (Discovery)",
	"gate_generation":   "Hammurapi: generation of the tech and qa gates",
	"conformance_check": "Hammurapi: check of code against the specification",
	"codegen":           "Hammurapi: code generation in the runner",
	"review_update":     "Hammurapi: answers to review comments",
	"rollback_revert":   "Hammurapi: conflicts and reverts",
}

// AgentName is the service agent of a scenario: hammurapi-<scenario>.
func AgentName(scenario string) string {
	return "hammurapi-" + strings.ReplaceAll(scenario, "_", "-")
}

// Importer performs the import.
type Importer struct {
	Models   *models.Service
	Catalog  *catalog.Service
	Services *services.Service
}

func defs(raw []json.RawMessage) []agent.ModelDef {
	var out []agent.ModelDef
	for _, r := range raw {
		var id string
		if json.Unmarshal(r, &id) == nil {
			out = append(out, agent.ModelDef{ID: id, Name: id, ContextWindow: 128_000, MaxTokens: 16_000, Input: []string{"text"}})
			continue
		}
		var d agent.ModelDef
		if json.Unmarshal(r, &d) == nil && d.ID != "" {
			out = append(out, d)
		}
	}
	return out
}

// Run imports the settings for the client.
func (im *Importer) Run(ctx context.Context, client string, b Body) (*Report, error) {
	rep := &Report{ServiceAgents: map[string]string{}, Connections: []string{}, MCP: []string{}, Skipped: []Skipped{}}
	conns := map[string]models.Choice{}
	for _, c := range b.Connections {
		typ := models.TypeOpenAICompatible
		if c.Type == "deepseek" {
			typ = models.TypeDeepSeek
		}
		created, err := im.Models.Create(ctx, models.Input{Name: "Hammurapi: " + strings.TrimSpace(c.Name), Type: typ, BaseURL: c.BaseURL,
			APIKey: c.APIKey, Defs: defs(c.Models), ImportRef: "hammurapi:" + c.Ref})
		if err != nil {
			rep.Skipped = append(rep.Skipped, Skipped{Object: "connection " + c.Name, Reason: err.Error()})
			continue
		}
		conns[c.Ref] = models.Choice{ConnectionID: created.ID}
		rep.Connections = append(rep.Connections, created.Name)
	}
	resolve := func(ch *Choice) (models.Choice, bool) {
		if ch == nil {
			return models.Choice{}, false
		}
		c, ok := conns[ch.ConnectionRef]
		if !ok {
			return models.Choice{}, false
		}
		c.Model, c.Thinking = ch.Model, ch.Thinking
		return c, true
	}
	if d, ok := resolve(b.Default); ok {
		if pm, err := im.Models.Personal(ctx); err == nil && pm.Default == nil {
			d.Thinking = ""
			if _, err := im.Models.PutPersonal(ctx, models.PersonalModels{Default: &d, Available: []models.Choice{d}}); err != nil {
				rep.Skipped = append(rep.Skipped, Skipped{Object: "default model", Reason: err.Error()})
			}
		}
	}
	// MCP servers become platform items of the catalog, used by the agents of
	// their scenarios; Hammurapi's own MCP comes with every run (callerMcp).
	mcpOf := map[string][]string{}
	for _, m := range b.MCP {
		name := strings.ToLower(strings.TrimSpace(m.Name))
		if existing, _ := im.Catalog.ByName(ctx, name); existing != nil && (existing.Mode == nil || *existing.Mode != "platform" ||
			!strings.HasPrefix(existing.Description, "Imported from Hammurapi")) {
			rep.Skipped = append(rep.Skipped, Skipped{Object: "mcp " + name, Reason: "a catalog item with this name exists"})
			continue
		}
		platform := "platform"
		exp := m.Exposure
		if exp != "direct" {
			exp = "deferred"
		}
		ro, pub := false, true
		desc := "Imported from Hammurapi"
		headers := m.Headers
		in := catalog.Input{Type: "mcp", Name: name, Title: &m.Name, Description: &desc, Source: &catalog.Source{Kind: "url", URL: m.URL},
			Mode: &platform, ReadOnly: &ro, Exposure: &exp, Published: &pub, PlatformAuth: &headers}
		existing, _ := im.Catalog.ByName(ctx, name)
		var err error
		if existing != nil {
			_, err = im.Catalog.Save(ctx, &existing.ID, in)
		} else {
			_, err = im.Catalog.Save(ctx, nil, in)
		}
		if err != nil {
			rep.Skipped = append(rep.Skipped, Skipped{Object: "mcp " + name, Reason: err.Error()})
			continue
		}
		rep.MCP = append(rep.MCP, name)
		for _, sc := range m.Scenarios {
			mcpOf[sc] = append(mcpOf[sc], name)
		}
	}
	skillsOf := map[string][]string{}
	if s := b.Skills; s != nil && s.Repo != "" {
		const name = "hammurapi-skills"
		title := "Hammurapi skills"
		desc := "Imported from Hammurapi: the agent skills of the specification repository"
		pub := true
		in := catalog.Input{Type: "skill", Name: name, Title: &title, Description: &desc,
			Source: &catalog.Source{Kind: "git", Repo: s.Repo, Path: s.Path, Ref: s.Ref}, Published: &pub}
		it, _ := im.Catalog.ByName(ctx, name)
		var err error
		if it != nil {
			it, err = im.Catalog.Save(ctx, &it.ID, in)
		} else {
			it, err = im.Catalog.Save(ctx, nil, in)
		}
		if err != nil {
			rep.Skipped = append(rep.Skipped, Skipped{Object: "skills", Reason: err.Error()})
		} else {
			rep.Skills = name
			if _, err := im.Catalog.Sync(ctx, it.ID); err != nil {
				rep.Skipped = append(rep.Skipped, Skipped{Object: "skills sync", Reason: err.Error() + " (synchronize it later in the catalog)"})
			}
		}
		for skill, scs := range s.Assignments {
			for _, sc := range scs {
				skillsOf[sc] = append(skillsOf[sc], skill)
			}
		}
	}
	for _, sc := range Scenarios {
		ch, ok := resolve(ptr(b.Scenarios[sc]))
		if !ok {
			if ch, ok = resolve(b.Default); !ok {
				rep.Skipped = append(rep.Skipped, Skipped{Object: "scenario " + sc, Reason: "no model for the scenario"})
				continue
			}
		}
		ws := services.WorkspaceNone
		if codeScenarios[sc] {
			ws = services.WorkspaceExternal
		}
		sort.Strings(skillsOf[sc])
		cfg := services.Config{Name: AgentName(sc), Description: descriptions[sc], Harness: "pi", Model: ch,
			Instructions: fmt.Sprintf("You are the service agent of Hammurapi for the %s scenario. The input describes the task; use the hammurapi MCP tools of the run to read the context and to submit the results.", sc),
			Skills:       skillsOf[sc], MCP: mcpOf[sc], AcceptCallerMC: true, Workspace: ws,
			Limits: services.Limits{TimeoutSec: 7200}, Clients: []string{client}}
		_, err := im.Services.Get(ctx, cfg.Name)
		create := err != nil
		if !create {
			if cur, err := im.Services.Get(ctx, cfg.Name); err == nil {
				cfg.Clients = appendUnique(cur.Clients, client)
				if cur.Limits.MaxTokens > 0 {
					cfg.Limits.MaxTokens = cur.Limits.MaxTokens
				}
			}
		}
		if _, err := im.Services.Save(ctx, nil, cfg.Name, cfg, create); err != nil {
			rep.Skipped = append(rep.Skipped, Skipped{Object: "scenario " + sc, Reason: err.Error()})
			continue
		}
		rep.ServiceAgents[sc] = cfg.Name
	}
	slog.InfoContext(ctx, "hammurapi agent settings imported", "client", client, "agents", len(rep.ServiceAgents), "skipped", len(rep.Skipped))
	return rep, nil
}

func ptr(c Choice) *Choice {
	if c.ConnectionRef == "" {
		return nil
	}
	return &c
}

func appendUnique(xs []string, x string) []string {
	for _, v := range xs {
		if v == x {
			return xs
		}
	}
	return append(xs, x)
}

// Route mounts POST /import/hammurapi-agent under /client/v1.
func (im *Importer) Route(r chi.Router) {
	r.Post("/import/hammurapi-agent", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		cl := httpx.ClientFrom(r.Context())
		if cl == nil || !cl.CanImport {
			return apperr.Forbidden("forbidden", "the client has no import right")
		}
		var b Body
		if err := httpx.DecodeMax(r, &b, 2<<20); err != nil {
			return err
		}
		rep, err := im.Run(r.Context(), cl.Name, b)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, rep)
		return nil
	}))
}
