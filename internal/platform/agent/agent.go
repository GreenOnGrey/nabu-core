// Package agent is the contract of the agent operator — the `nabu agent` mode
// (FTR.NAB.CMN-0001 arch §3–4): the internal API agent:8090/v1 through which
// the worker runs harness sessions, its stream events and the classes of
// model errors. The operator lives in agent/operator, the Pi harness in
// agent/pi. Ported from Hammurapi (FTR.HMR.CMN-0004) with the scenarios
// replaced by session kinds.
package agent

import (
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"time"
)

// SessionKind separates the operator's capacity (tech spec §9):
// conversations of personal agents and task runs vs service agent runs.
type SessionKind string

// Session kinds.
const (
	KindChat SessionKind = "chat"
	KindRun  SessionKind = "run"
)

// ErrorClass is the class of a model failure (as in FTR.HMR.CMN-0004 arch §8).
type ErrorClass string

// Error classes.
const (
	ErrInsufficientBalance ErrorClass = "insufficient_balance"
	ErrAuth                ErrorClass = "auth"
	ErrRateLimit           ErrorClass = "rate_limit"
	ErrUnavailable         ErrorClass = "unavailable"
	ErrBadRequest          ErrorClass = "bad_request"
	ErrContextOverflow     ErrorClass = "context_overflow"
	ErrAgentCrashed        ErrorClass = "agent_crashed"
)

// Retryable reports whether the user may retry later without an
// administrator fixing the connection.
func (c ErrorClass) Retryable() bool {
	return c == ErrRateLimit || c == ErrUnavailable || c == ErrAgentCrashed || c == ErrContextOverflow
}

// Cost is a price in US dollars per million tokens.
type Cost struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cacheRead"`
	CacheWrite float64 `json:"cacheWrite"`
}

// ModelDef is one model of a connection as Pi needs it in models.json.
type ModelDef struct {
	ID               string             `json:"id"`
	Name             string             `json:"name,omitempty"`
	ContextWindow    int64              `json:"contextWindow"`
	MaxTokens        int64              `json:"maxTokens"`
	Reasoning        bool               `json:"reasoning"`
	Input            []string           `json:"input"`
	ThinkingLevelMap map[string]*string `json:"thinkingLevelMap,omitempty"`
	Cost             Cost               `json:"cost"`
	Compat           map[string]any     `json:"compat,omitempty"`
	// FTR.NAB.CHAT-0001 R8: the capabilities Nabu keeps next to what Pi needs.
	// Tools: the model calls tools (a personal agent needs it). Embeddings:
	// the model only answers /embeddings. Aliases: other ids of the provider
	// for the same model.
	Tools      bool     `json:"tools"`
	Embeddings bool     `json:"embeddings,omitempty"`
	Aliases    []string `json:"aliases,omitempty"`
	// Sources: where each capability came from — provider, catalog, probe,
	// manual or default (R7). Overrides: the fields an administrator set by
	// hand; a refresh keeps them. Gone: the provider no longer lists the model.
	Sources   map[string]string `json:"sources,omitempty"`
	Overrides []string          `json:"overrides,omitempty"`
	Gone      bool              `json:"gone,omitempty"`
}

// EmbeddingsName reports a model that only gives vectors by its id
// (FTR.NAB.CHAT-0001 R8): text-embedding-3-small, nomic-embed-text, bge-m3,
// multilingual-e5-large. A provider that lists models without their kind
// (OpenAI, DeepSeek) gives nothing else to tell them by.
func EmbeddingsName(id string) bool {
	id = strings.ToLower(id)
	if strings.Contains(id, "embed") {
		return true
	}
	base := id[strings.LastIndex(id, "/")+1:]
	for _, p := range []string{"bge-", "e5-", "gte-", "multilingual-e5", "jina-embeddings"} {
		if strings.HasPrefix(base, p) {
			return true
		}
	}
	return false
}

// HasInput reports whether the model takes the kind of input (text, image).
func (m ModelDef) HasInput(kind string) bool { return slices.Contains(m.Input, kind) }

// ThinkingLevels lists the levels offered for the model.
func (m ModelDef) ThinkingLevels() []string {
	levels := []string{"off"}
	if !m.Reasoning {
		return levels
	}
	for _, l := range []string{"minimal", "low", "medium", "high", "xhigh", "max"} {
		if v, ok := m.ThinkingLevelMap[l]; ok && v != nil {
			levels = append(levels, l)
		}
	}
	return levels
}

// SupportsThinking reports whether level is offered for the model.
func (m ModelDef) SupportsThinking(level string) bool {
	for _, l := range m.ThinkingLevels() {
		if l == level {
			return true
		}
	}
	return false
}

// ModelSpec is the resolved model of a session: the provider (one per
// connection) with its models, and the model and level to use.
type ModelSpec struct {
	Provider     string     `json:"provider"` // nabu-<connection id>
	ConnectionID string     `json:"connectionId"`
	API          string     `json:"api"`
	BaseURL      string     `json:"baseUrl"`
	ModelID      string     `json:"modelId"`
	Thinking     string     `json:"thinking"`
	Models       []ModelDef `json:"models"`
}

// Secrets travel only in the request body and live in the Pi process
// environment; files on disk reference them as ${…}.
type Secrets struct {
	LLMKey     string                       `json:"llmKey"`
	MCPHeaders map[string]map[string]string `json:"mcpHeaders,omitempty"` // server → header → value
}

// MCPServer is an MCP server of the session (streamable HTTP).
type MCPServer struct {
	Name        string   `json:"name"`
	URL         string   `json:"url"`
	HeaderNames []string `json:"headerNames,omitempty"`
	Exposure    string   `json:"exposure"` // direct | deferred
	Description string   `json:"description,omitempty"`
}

// Skills names the skills snapshot and the skills of the session.
type Skills struct {
	Hash   string   `json:"hash"`
	Names  []string `json:"names"`
	Bundle []byte   `json:"bundle,omitempty"` // tar.gz, sent when the operator does not know the hash
}

// Workspace is the workspace of the session, reached through the relay
// (arch §5.1): the nabu-workspace extension routes Pi's file and shell tools
// to URL with the call token.
type Workspace struct {
	ID    string `json:"id"`
	URL   string `json:"url"`
	Token string `json:"token"`
	// Note is appended to the system prompt: where the files are.
	Note string `json:"note,omitempty"`
	// Root is the working directory as the commands of the workspace see it
	// (/work in a sandbox); the model is shown it instead of the local one of
	// Pi. Empty when unknown, as in a workspace of a service client.
	Root string `json:"root,omitempty"`
}

// HistoryMessage seeds a session that has no snapshot.
type HistoryMessage struct {
	Role string `json:"role"` // user | assistant
	Text string `json:"text"`
}

// SessionRequest is the body of POST /v1/sessions.
type SessionRequest struct {
	Kind         SessionKind      `json:"kind"`
	Model        ModelSpec        `json:"model"`
	Secrets      Secrets          `json:"secrets"`
	MCP          []MCPServer      `json:"mcp,omitempty"`
	Skills       *Skills          `json:"skills,omitempty"`
	SystemAppend string           `json:"systemAppend,omitempty"`
	Snapshot     []byte           `json:"snapshot,omitempty"`
	History      []HistoryMessage `json:"history,omitempty"`
	Workspace    *Workspace       `json:"workspace,omitempty"`
	// Label is a short description for the operator's logs (never secrets).
	Label string `json:"label,omitempty"`
	// Subagents lets the agent start subagents (FTR.NAB.CMN-0009 R1, R6);
	// nil — the session has none.
	Subagents *SubagentLimits `json:"subagents,omitempty"`
	// probeTools gives a check session Pi's ls in the empty working
	// directory, to see whether the model calls tools (FTR.NAB.CHAT-0001 R9).
	// It is set only inside the operator, never from a request.
	probeTools bool
}

// WithProbeTools returns the request with the tool probe switched on.
func (r SessionRequest) WithProbeTools() SessionRequest { r.probeTools = true; return r }

// ProbeTools reports whether the request is a check that probes tools.
func (r SessionRequest) ProbeTools() bool { return r.probeTools }

// SessionResponse is the answer of POST /v1/sessions.
type SessionResponse struct {
	SessionID    string `json:"sessionId"`
	SessionToken string `json:"sessionToken"`
	Model        string `json:"model"`
	Thinking     string `json:"thinking"`
}

// Image is an image attached to a prompt.
type Image struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"` // base64
}

// PromptRequest is the body of POST /v1/sessions/{id}/prompt.
type PromptRequest struct {
	Text   string  `json:"text"`
	Images []Image `json:"images,omitempty"`
	// Deadline is the end of the turn: a subagent does not live longer
	// (FTR.NAB.CMN-0009 tech §2.1).
	Deadline time.Time `json:"deadline,omitzero"`
}

// SubagentLimits are the limits of the subagents of a session
// (FTR.NAB.CMN-0009 R6, tech §2.1); the operator only applies them.
type SubagentLimits struct {
	MaxParallel int      `json:"maxParallel"` // tasks of one call
	MaxPerTurn  int      `json:"maxPerTurn"`  // subagents of one turn
	Timeout     Duration `json:"timeout"`     // of one task
	ResultChars int      `json:"resultChars"` // the result the agent gets back
	SlotWait    Duration `json:"slotWait"`    // for a place in the pod
}

// Duration is a time.Duration written as "15m" in JSON.
type Duration time.Duration

// MarshalJSON writes the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// UnmarshalJSON reads "15m".
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	*d = Duration(v)
	return err
}

// States of a subagent task (FTR.NAB.CMN-0009 R8).
const (
	SubagentRunning = "running"
	SubagentDone    = "done"
	SubagentFailed  = "failed"
	SubagentTimeout = "timeout"
	SubagentAborted = "aborted"
)

// SubagentState is the state of one task of a subagent call, as the stream
// of the parent turn carries it (event subagent) and the answer keeps it.
type SubagentState struct {
	Call        string `json:"call"`
	Task        int    `json:"task"`
	Description string `json:"description"`
	State       string `json:"state"`
	Tools       int    `json:"tools"`
	Error       string `json:"error,omitempty"`
	// Result is the beginning of the result, only when done or timed out.
	Result string `json:"result,omitempty"`
}

// PatchRequest changes the model or level of a session.
type PatchRequest struct {
	ModelID  string `json:"modelId,omitempty"`
	Thinking string `json:"thinking,omitempty"`
}

// Event types of the operator's NDJSON stream.
const (
	EventTextDelta     = "text_delta"
	EventThinkingDelta = "thinking_delta"
	EventToolCall      = "tool_call"
	EventToolResult    = "tool_result"
	EventCompaction    = "compaction"
	// EventContext reports the fill of the context window after a run or a
	// compaction (FTR.NAB.CHAT-0001 R3): ContextTokens of ContextWindow.
	EventContext = "context"
	EventRetry   = "retry"
	EventUsage   = "usage"
	EventError   = "error"
	EventSettled = "settled"
	// EventSubagent is the progress of a subagent of the turn; its Usage is
	// what the subagent spent since its last event (FTR.NAB.CMN-0009 R8, R9).
	EventSubagent = "subagent"
)

// Event is one record of the prompt stream.
type Event struct {
	Type         string          `json:"type"`
	Delta        string          `json:"delta,omitempty"`
	ID           string          `json:"id,omitempty"`
	Name         string          `json:"name,omitempty"`
	Args         json.RawMessage `json:"args,omitempty"`
	IsError      bool            `json:"isError,omitempty"`
	Summary      string          `json:"summary,omitempty"`
	TokensBefore int64           `json:"tokensBefore,omitempty"`
	TokensAfter  int64           `json:"tokensAfter,omitempty"`
	Attempt      int             `json:"attempt,omitempty"`
	ErrorClass   ErrorClass      `json:"errorClass,omitempty"`
	HTTPStatus   int             `json:"httpStatus,omitempty"`
	Message      string          `json:"message,omitempty"`
	// Aborted marks a settled run stopped by abort: the stream still ends normally.
	Aborted bool `json:"aborted,omitempty"`
	// Auto marks a compaction the harness made itself (FTR.NAB.CHAT-0001 R2).
	Auto          bool           `json:"auto,omitempty"`
	ContextTokens int64          `json:"contextTokens,omitempty"`
	ContextWindow int64          `json:"contextWindow,omitempty"`
	Subagent      *SubagentState `json:"subagent,omitempty"`
	// Of marks a tool call or result of a subagent, "<call>/<task>": the
	// worker audits it (FTR.NAB.CMN-0009 R9), the answer does not show it.
	Of string `json:"of,omitempty"`
	Usage
}

// Usage is token use and cost.
type Usage struct {
	TokensIn   int64   `json:"tokensIn,omitempty"`
	TokensOut  int64   `json:"tokensOut,omitempty"`
	CacheRead  int64   `json:"cacheRead,omitempty"`
	CacheWrite int64   `json:"cacheWrite,omitempty"`
	CostUSD    float64 `json:"costUsd,omitempty"`
}

// Add accumulates u2 into u.
func (u *Usage) Add(u2 Usage) {
	u.TokensIn += u2.TokensIn
	u.TokensOut += u2.TokensOut
	u.CacheRead += u2.CacheRead
	u.CacheWrite += u2.CacheWrite
	u.CostUSD += u2.CostUSD
}

// IsZero reports whether nothing was used.
func (u Usage) IsZero() bool {
	return u.TokensIn == 0 && u.TokensOut == 0 && u.CacheRead == 0 && u.CacheWrite == 0 && u.CostUSD == 0
}

// Total is all tokens of the usage.
func (u Usage) Total() int64 { return u.TokensIn + u.TokensOut + u.CacheRead + u.CacheWrite }

// Probes of a check (FTR.NAB.CHAT-0001 R9): image sends a small picture and
// asks for its colour, tools offers a tool and expects the call.
const (
	ProbeImage = "image"
	ProbeTools = "tools"
)

// Results of a probe.
const (
	ProbeConfirmed   = "confirmed"
	ProbeUnconfirmed = "unconfirmed"
	ProbeSkipped     = "skipped"
)

// LLMCheckRequest is the body of POST /v1/checks/llm.
type LLMCheckRequest struct {
	Model   ModelSpec `json:"model"`
	Secrets Secrets   `json:"secrets"`
	// Probes are run on every model that answered: image only on models
	// that declare images, tools on every model.
	Probes []string `json:"probes,omitempty"`
}

// CompactRequest is the body of POST /v1/sessions/{id}/compact.
type CompactRequest struct {
	Instructions string `json:"instructions,omitempty"`
}

// CompactResponse is the answer of a compaction.
type CompactResponse struct {
	Summary       string `json:"summary"`
	TokensBefore  int64  `json:"tokensBefore"`
	TokensAfter   int64  `json:"tokensAfter"`
	ContextWindow int64  `json:"contextWindow"`
	Usage         Usage  `json:"usage"`
}

// LLMCheckResult is the result for one model.
type LLMCheckResult struct {
	Model      string     `json:"model"`
	OK         bool       `json:"ok"`
	LatencyMs  int64      `json:"latencyMs,omitempty"`
	ErrorClass ErrorClass `json:"errorClass,omitempty"`
	HTTPStatus int        `json:"httpStatus,omitempty"`
	Message    string     `json:"message,omitempty"`
	// Embeddings: the model does not chat but answers /embeddings — the core
	// sets it, the operator never does.
	Embeddings bool `json:"embeddings,omitempty"`
	// Caps are the results of the probes: capability → confirmed |
	// unconfirmed | skipped.
	Caps map[string]string `json:"caps,omitempty"`
}

// LLMCheckResponse is the answer of POST /v1/checks/llm.
type LLMCheckResponse struct {
	Results []LLMCheckResult `json:"results"`
}

// MCPCheckRequest is the body of POST /v1/checks/mcp.
type MCPCheckRequest struct {
	Server  MCPServer         `json:"server"`
	Headers map[string]string `json:"headers,omitempty"`
}

// MCPTool is a tool reported by an MCP server.
type MCPTool struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	ReadOnly    bool   `json:"readOnly"`
	Destructive bool   `json:"destructive"`
	// StringArgs are the properties of the input schema of type string: the
	// arguments an administrator may mark as files of the space
	// (FTR.NAB.CMN-0006 R14, tech §3.2).
	StringArgs []string `json:"stringArgs,omitempty"`
	// StringListArgs are the properties that are arrays of strings: a list of
	// files of the space (FTR.NAB.CHAT-0003 R11).
	StringListArgs []string `json:"stringListArgs,omitempty"`
}

// MCPCheckResponse is the answer of POST /v1/checks/mcp.
type MCPCheckResponse struct {
	OK    bool      `json:"ok"`
	Tools []MCPTool `json:"tools"`
	Error string    `json:"error,omitempty"`
}

// ConnectionProblem reports whether the class marks the connection as broken
// (shown to administrators in the connection's state).
func (c ErrorClass) ConnectionProblem() bool { return c == ErrInsufficientBalance || c == ErrAuth }

// StringListArgs lists the properties of a JSON schema of tool input that
// are arrays of strings, sorted (FTR.NAB.CHAT-0003 R11).
func StringListArgs(schema json.RawMessage) []string {
	var s struct {
		Properties map[string]struct {
			Type  json.RawMessage `json:"type"`
			Items struct {
				Type json.RawMessage `json:"type"`
			} `json:"items"`
		} `json:"properties"`
	}
	if json.Unmarshal(schema, &s) != nil {
		return nil
	}
	is := func(raw json.RawMessage, want string) bool {
		var one string
		var many []string
		return (json.Unmarshal(raw, &one) == nil && one == want) || (json.Unmarshal(raw, &many) == nil && slices.Contains(many, want))
	}
	var out []string
	for name, p := range s.Properties {
		if is(p.Type, "array") && is(p.Items.Type, "string") {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// StringArgs lists the properties of a JSON schema of tool input whose type is
// string, sorted (FTR.NAB.CMN-0006 CAT-15).
func StringArgs(schema json.RawMessage) []string {
	var s struct {
		Properties map[string]struct {
			Type json.RawMessage `json:"type"`
		} `json:"properties"`
	}
	if json.Unmarshal(schema, &s) != nil {
		return nil
	}
	var out []string
	for name, p := range s.Properties {
		var one string
		var many []string
		switch {
		case json.Unmarshal(p.Type, &one) == nil && one == "string":
			out = append(out, name)
		case json.Unmarshal(p.Type, &many) == nil && slices.Contains(many, "string"):
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
