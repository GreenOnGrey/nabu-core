// Package pi is the Pi harness of Nabu (FTR.NAB.CMN-0001 arch §4.1, ported
// from FTR.HMR.CMN-0004 arch §3.3–3.4): it writes a Pi agent directory per
// session, starts `pi --mode rpc` through pkg/pirpc with a clean environment,
// translates Pi's events into the operator's stream, classifies model errors
// and counts usage.
package pi

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/GreenOnGrey/nabu-core/internal/platform/agent"
)

// CodeTools are Pi's built-in file and shell tools. Sessions without a
// workspace get none of them; the others get them routed to the workspace
// through the relay by the nabu-workspace extension (arch §5.1).
var CodeTools = []string{"read", "bash", "edit", "write", "grep", "find", "ls"}

// Layout is the directory of one session.
type Layout struct {
	Root     string // /work/sessions/<id>
	AgentDir string // PI_CODING_AGENT_DIR
	// CWD is the empty working directory: no files of the operator reach the
	// context. It is shared by the sessions of an operator, because Pi records
	// it in the session file and a restored session needs it to exist.
	CWD         string
	SessionsDir string
}

// NewLayout returns the layout under root; cwd empty means root/cwd.
func NewLayout(root, cwd string) Layout {
	if cwd == "" {
		cwd = filepath.Join(root, "cwd")
	}
	return Layout{Root: root, AgentDir: filepath.Join(root, "agent"), CWD: cwd, SessionsDir: filepath.Join(root, "sessions")}
}

// Options configure the files and the process environment.
type Options struct {
	// ExtensionDir is the nabu-workspace extension (sessions with a workspace).
	ExtensionDir string
	// CWD is the shared empty working directory of the sessions (see Layout.CWD).
	CWD string
	// SkillsDir holds the extracted skills snapshot of the session (nil: none).
	SkillsDir string
	// PATH, LANG of the Pi process (the operator's own environment is not inherited).
	Path, Lang string
	// ExtraEnv is appended as is (tests, proxy settings).
	ExtraEnv []string
}

// Write creates the agent directory of a session. Secrets never reach the
// files: they reference environment variables as ${…}.
func Write(l Layout, req agent.SessionRequest, o Options) error {
	for _, d := range []string{l.AgentDir, l.CWD, l.SessionsDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return err
		}
	}
	files := map[string]any{
		"models.json":   ModelsJSON(req.Model),
		"mcp.json":      MCPJSON(req),
		"settings.json": SettingsJSON(req, o.ExtensionDir),
	}
	for name, v := range files {
		if err := writeJSON(filepath.Join(l.AgentDir, name), v); err != nil {
			return err
		}
	}
	if s := strings.TrimSpace(req.SystemAppend); s != "" {
		if err := os.WriteFile(filepath.Join(l.AgentDir, "APPEND_SYSTEM.md"), []byte(s+"\n"), 0o600); err != nil {
			return err
		}
	}
	if req.Skills != nil && o.SkillsDir != "" {
		for _, name := range req.Skills.Names {
			if !skillName.MatchString(name) {
				continue
			}
			src := filepath.Join(o.SkillsDir, name)
			if _, err := os.Stat(filepath.Join(src, "SKILL.md")); err != nil {
				continue // not in this snapshot
			}
			if err := copyDir(src, filepath.Join(l.AgentDir, "skills", name)); err != nil {
				return fmt.Errorf("skill %s: %w", name, err)
			}
		}
	}
	if req.Snapshot != nil {
		if err := os.WriteFile(SnapshotPath(l), req.Snapshot, 0o600); err != nil {
			return err
		}
	}
	return nil
}

// SnapshotPath is where a restored session file is written before switch_session.
func SnapshotPath(l Layout) string { return filepath.Join(l.SessionsDir, "restored.jsonl") }

var skillName = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

// ModelsJSON is the provider of the session's connection.
func ModelsJSON(m agent.ModelSpec) map[string]any {
	models := make([]map[string]any, 0, len(m.Models))
	for _, d := range m.Models {
		e := map[string]any{"id": d.ID, "contextWindow": d.ContextWindow, "maxTokens": d.MaxTokens,
			"reasoning": d.Reasoning, "input": orDefault(d.Input, []string{"text"}),
			// All four prices are required: Pi drops the provider otherwise.
			"cost": map[string]float64{"input": d.Cost.Input, "output": d.Cost.Output, "cacheRead": d.Cost.CacheRead, "cacheWrite": d.Cost.CacheWrite}}
		if d.Name != "" {
			e["name"] = d.Name
		}
		if len(d.ThinkingLevelMap) > 0 {
			e["thinkingLevelMap"] = d.ThinkingLevelMap
		}
		if len(d.Compat) > 0 {
			e["compat"] = d.Compat
		}
		models = append(models, e)
	}
	return map[string]any{"providers": map[string]any{m.Provider: map[string]any{
		"baseUrl": m.BaseURL, "api": m.API, "apiKey": "${NABU_LLM_KEY}", "models": models}}}
}

// MCPJSON lists the MCP servers of the session with headers from the environment.
func MCPJSON(req agent.SessionRequest) map[string]any {
	servers := map[string]any{}
	for i, s := range req.MCP {
		h := map[string]string{}
		for _, name := range s.HeaderNames {
			h[name] = "${" + HeaderEnv(i+1, name) + "}"
		}
		e := map[string]any{"url": s.URL, "exposure": exposure(s.Exposure)}
		if len(h) > 0 {
			e["headers"] = h
		}
		if s.Description != "" {
			e["description"] = s.Description
		}
		servers[s.Name] = e
	}
	return map[string]any{"mcpServers": servers}
}

func exposure(e string) string {
	if e == "direct" {
		return "direct"
	}
	return "deferred"
}

// HeaderEnv is the variable of header name of the n-th MCP server:
// NABU_MCP_<N>_<HEADER>, upper case, other characters as "_".
func HeaderEnv(n int, header string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(header) {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return fmt.Sprintf("NABU_MCP_%d_%s", n, b.String())
}

// SettingsJSON is the agent-level settings of the session. Extensions are
// never taken from anywhere but the image: scheduling and background-task
// extensions of Pi are not used (arch §9a, TSK-13).
func SettingsJSON(req agent.SessionRequest, extensionDir string) map[string]any {
	s := map[string]any{
		"defaultProjectTrust":    "never",
		"enableInstallTelemetry": false,
		"defaultProvider":        req.Model.Provider,
		"defaultModel":           req.Model.ModelID,
		"defaultThinkingLevel":   thinkingOrOff(req.Model.Thinking),
		"retry":                  map[string]any{"enabled": true, "maxRetries": 3, "baseDelayMs": 2000},
		"compaction":             map[string]any{"enabled": true},
		"quietStartup":           true,
		"extensions":             []string{},
	}
	if req.Workspace != nil {
		s["defaultTools"] = CodeTools
		if extensionDir != "" {
			s["extensions"] = []string{extensionDir}
		}
	} else {
		s["defaultTools"] = []string{} // no read, bash, edit, write, grep, find, ls (SVC-05)
	}
	return s
}

func thinkingOrOff(t string) string {
	if t == "" {
		return "off"
	}
	return t
}

// Env is the complete environment of the Pi process: nothing of the
// operator's own environment is inherited.
func Env(l Layout, req agent.SessionRequest, o Options) []string {
	env := []string{
		"PATH=" + o.Path,
		"HOME=" + l.Root,
		"PI_CODING_AGENT_DIR=" + l.AgentDir,
		"PI_OFFLINE=1", "PI_TELEMETRY=0", "PI_SKIP_VERSION_CHECK=1",
		"NABU_LLM_KEY=" + req.Secrets.LLMKey,
	}
	if o.Lang != "" {
		env = append(env, "LANG="+o.Lang)
	}
	for i, s := range req.MCP {
		vals := req.Secrets.MCPHeaders[s.Name]
		for _, name := range s.HeaderNames {
			env = append(env, HeaderEnv(i+1, name)+"="+vals[name])
		}
	}
	if req.Workspace != nil {
		env = append(env, "NABU_WORKSPACE_URL="+req.Workspace.URL, "NABU_WORKSPACE_TOKEN="+req.Workspace.Token,
			"NABU_WORKSPACE_NOTE="+oneLine(req.Workspace.Note))
	}
	return append(env, o.ExtraEnv...)
}

func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

// SecretValues lists the secret values of a request for log masking.
func SecretValues(req agent.SessionRequest) []string {
	var out []string
	add := func(s string) {
		if len(s) >= 6 {
			out = append(out, s)
		}
	}
	add(req.Secrets.LLMKey)
	for _, hs := range req.Secrets.MCPHeaders {
		for _, v := range hs {
			add(v)
			if f := strings.Fields(v); len(f) == 2 {
				add(f[1]) // the token of "Bearer <token>"
			}
		}
	}
	if req.Workspace != nil {
		add(req.Workspace.Token)
	}
	sort.Slice(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return out
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func orDefault(v, d []string) []string {
	if len(v) == 0 {
		return d
	}
	return v
}

func copyDir(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		target := filepath.Join(dst, rel)
		if d.Type()&os.ModeSymlink != 0 {
			return nil // snapshots never contain links; skip defensively
		}
		if d.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	})
}
