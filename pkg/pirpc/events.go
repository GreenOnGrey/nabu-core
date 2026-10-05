package pirpc

import "encoding/json"

// Event types of Pi's session stream that callers commonly handle. Unknown
// types arrive in Events() unchanged.
const (
	EventAgentStart          = "agent_start"
	EventAgentEnd            = "agent_end"
	EventAgentSettled        = "agent_settled"
	EventTurnStart           = "turn_start"
	EventTurnEnd             = "turn_end"
	EventMessageStart        = "message_start"
	EventMessageUpdate       = "message_update"
	EventMessageEnd          = "message_end"
	EventToolExecutionStart  = "tool_execution_start"
	EventToolExecutionUpdate = "tool_execution_update"
	EventToolExecutionEnd    = "tool_execution_end"
	EventAutoRetryStart      = "auto_retry_start"
	EventAutoRetryEnd        = "auto_retry_end"
	EventCompactionStart     = "compaction_start"
	EventCompactionEnd       = "compaction_end"
	EventExtensionError      = "extension_error"
)

// AssistantMessageEvent is the nested event of message_update.
type AssistantMessageEvent struct {
	Type         string          `json:"type"` // text_delta, thinking_delta, toolcall_start, toolcall_end, ...
	ContentIndex int             `json:"contentIndex"`
	Delta        string          `json:"delta,omitempty"`
	ID           string          `json:"id,omitempty"`
	ToolName     string          `json:"toolName,omitempty"`
	ToolCall     json.RawMessage `json:"toolCall,omitempty"`
}

// MessageUpdate is a message_update event.
type MessageUpdate struct {
	Usage                 Usage                 `json:"usage"`
	AssistantMessageEvent AssistantMessageEvent `json:"assistantMessageEvent"`
}

// Message is a conversation message; assistant fields are set for role "assistant".
type Message struct {
	Role         string          `json:"role"`
	Content      json.RawMessage `json:"content,omitempty"`
	Provider     string          `json:"provider,omitempty"`
	Model        string          `json:"model,omitempty"`
	Usage        *Usage          `json:"usage,omitempty"`
	StopReason   string          `json:"stopReason,omitempty"` // stop, length, toolUse, error, aborted, ...
	ErrorMessage string          `json:"errorMessage,omitempty"`
}

// Text returns the concatenated text blocks of the message.
func (m Message) Text() string {
	var s string
	if json.Unmarshal(m.Content, &s) == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(m.Content, &blocks) != nil {
		return ""
	}
	out := ""
	for _, b := range blocks {
		if b.Type == "text" {
			out += b.Text
		}
	}
	return out
}

// MessageEnd is a message_start or message_end event.
type MessageEnd struct {
	Message Message `json:"message"`
}

// AgentEnd is an agent_end event.
type AgentEnd struct {
	WillRetry bool `json:"willRetry"`
}

// ToolExecution is a tool_execution_start, _update or _end event.
type ToolExecution struct {
	ToolCallID string          `json:"toolCallId"`
	ToolName   string          `json:"toolName"`
	Args       json.RawMessage `json:"args,omitempty"`
	Result     json.RawMessage `json:"result,omitempty"`
	IsError    bool            `json:"isError,omitempty"`
}

// AutoRetry is an auto_retry_start or auto_retry_end event.
type AutoRetry struct {
	Attempt      int    `json:"attempt"`
	MaxAttempts  int    `json:"maxAttempts,omitempty"`
	DelayMs      int64  `json:"delayMs,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	Success      bool   `json:"success,omitempty"`
	FinalError   string `json:"finalError,omitempty"`
}

// Compaction is a compaction_start or compaction_end event.
type Compaction struct {
	Reason       string         `json:"reason"`
	Result       *CompactResult `json:"result,omitempty"`
	Aborted      bool           `json:"aborted,omitempty"`
	WillRetry    bool           `json:"willRetry,omitempty"`
	ErrorMessage string         `json:"errorMessage,omitempty"`
}
