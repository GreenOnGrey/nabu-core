package pirpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Image is an image content block of a prompt.
type Image struct {
	Type     string `json:"type"` // always "image"
	Data     string `json:"data"` // base64
	MimeType string `json:"mimeType"`
}

// PromptParams is the prompt command.
type PromptParams struct {
	Message string
	Images  []Image
	// StreamingBehavior is "steer" or "followUp"; required while a run is active.
	StreamingBehavior string
}

// Disposition is what Pi did with a prompt: started, queued or handled.
type Disposition string

// Prompt dispositions.
const (
	DispositionStarted Disposition = "started"
	DispositionQueued  Disposition = "queued"
	DispositionHandled Disposition = "handled"
)

// Cost is a price in US dollars per million tokens.
type Cost struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cacheRead"`
	CacheWrite float64 `json:"cacheWrite"`
}

// Model is a configured model.
type Model struct {
	ID            string   `json:"id"`
	Name          string   `json:"name,omitempty"`
	API           string   `json:"api,omitempty"`
	Provider      string   `json:"provider"`
	BaseURL       string   `json:"baseUrl,omitempty"`
	Reasoning     bool     `json:"reasoning,omitempty"`
	Input         []string `json:"input,omitempty"`
	ContextWindow int64    `json:"contextWindow,omitempty"`
	MaxTokens     int64    `json:"maxTokens,omitempty"`
	Cost          Cost     `json:"cost"`
}

// State is the get_state result.
type State struct {
	Model         *Model `json:"model,omitempty"`
	ThinkingLevel string `json:"thinkingLevel"`
	IsStreaming   bool   `json:"isStreaming"`
	IsCompacting  bool   `json:"isCompacting"`
	SessionFile   string `json:"sessionFile"`
	SessionID     string `json:"sessionId"`
	MessageCount  int    `json:"messageCount"`
}

// Tokens are token counters.
type Tokens struct {
	Input      int64 `json:"input"`
	Output     int64 `json:"output"`
	CacheRead  int64 `json:"cacheRead"`
	CacheWrite int64 `json:"cacheWrite"`
	Total      int64 `json:"total"`
}

// SessionStats is the get_session_stats result: totals of the whole session.
type SessionStats struct {
	SessionFile  string  `json:"sessionFile"`
	SessionID    string  `json:"sessionId"`
	Tokens       Tokens  `json:"tokens"`
	Cost         float64 `json:"cost"`
	ContextUsage *struct {
		Tokens        *int64   `json:"tokens"`
		ContextWindow int64    `json:"contextWindow"`
		Percent       *float64 `json:"percent"`
	} `json:"contextUsage,omitempty"`
}

// Usage is the usage of one model call (assistant message, compaction).
type Usage struct {
	Input       int64 `json:"input"`
	Output      int64 `json:"output"`
	CacheRead   int64 `json:"cacheRead"`
	CacheWrite  int64 `json:"cacheWrite"`
	TotalTokens int64 `json:"totalTokens"`
	Cost        struct {
		Total float64 `json:"total"`
	} `json:"cost"`
}

// CompactResult is the compact result.
type CompactResult struct {
	Summary              string `json:"summary"`
	TokensBefore         int64  `json:"tokensBefore"`
	EstimatedTokensAfter int64  `json:"estimatedTokensAfter"`
	Usage                *Usage `json:"usage,omitempty"`
}

func decode[T any](data json.RawMessage, err error) (T, error) {
	var v T
	if err != nil {
		return v, err
	}
	if len(data) == 0 || string(data) == "null" {
		return v, nil
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return v, fmt.Errorf("pirpc: decode response: %w", err)
	}
	return v, nil
}

// Prompt sends a prompt and returns its disposition; the run continues in events.
func (c *Client) Prompt(ctx context.Context, p PromptParams) (Disposition, error) {
	f := map[string]any{"message": p.Message}
	if len(p.Images) > 0 {
		imgs := make([]Image, len(p.Images))
		for i, im := range p.Images {
			im.Type = "image"
			imgs[i] = im
		}
		f["images"] = imgs
	}
	if p.StreamingBehavior != "" {
		f["streamingBehavior"] = p.StreamingBehavior
	}
	r, err := decode[struct {
		Disposition Disposition `json:"disposition"`
	}](c.Command(ctx, "prompt", f))
	return r.Disposition, err
}

// PromptAndWait sends a prompt and delivers the session events to onEvent
// until agent_settled. It subscribes before sending, so a fast run is not
// missed. On ctx cancellation it aborts the run.
func (c *Client) PromptAndWait(ctx context.Context, p PromptParams, onEvent func(Event)) error {
	events, cancel := c.Subscribe()
	defer cancel()
	disp, err := c.Prompt(ctx, p)
	if err != nil {
		return err
	}
	if disp == DispositionHandled {
		return nil
	}
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return c.exitError()
			}
			if onEvent != nil {
				onEvent(ev)
			}
			if ev.Type == "agent_settled" {
				return nil
			}
		case <-ctx.Done():
			actx, acancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			_ = c.Abort(actx)
			acancel()
			return ctx.Err()
		}
	}
}

// Abort aborts the current run and waits until the session is idle.
func (c *Client) Abort(ctx context.Context) error {
	_, err := c.Command(ctx, "abort", nil)
	return err
}

// GetState returns the session state.
func (c *Client) GetState(ctx context.Context) (State, error) {
	return decode[State](c.Command(ctx, "get_state", nil))
}

// SetModel switches the model.
func (c *Client) SetModel(ctx context.Context, provider, modelID string) (Model, error) {
	return decode[Model](c.Command(ctx, "set_model", map[string]any{"provider": provider, "modelId": modelID}))
}

// SetThinkingLevel sets the reasoning level (off, minimal, low, medium, high, xhigh, max).
func (c *Client) SetThinkingLevel(ctx context.Context, level string) error {
	_, err := c.Command(ctx, "set_thinking_level", map[string]any{"level": level})
	return err
}

// GetAvailableModels lists the configured models.
func (c *Client) GetAvailableModels(ctx context.Context) ([]Model, error) {
	r, err := decode[struct {
		Models []Model `json:"models"`
	}](c.Command(ctx, "get_available_models", nil))
	return r.Models, err
}

// GetAvailableThinkingLevels lists the levels of the current model.
func (c *Client) GetAvailableThinkingLevels(ctx context.Context) ([]string, error) {
	r, err := decode[struct {
		Levels []string `json:"levels"`
	}](c.Command(ctx, "get_available_thinking_levels", nil))
	return r.Levels, err
}

// GetSessionStats returns the token and cost totals of the session.
func (c *Client) GetSessionStats(ctx context.Context) (SessionStats, error) {
	return decode[SessionStats](c.Command(ctx, "get_session_stats", nil))
}

// Compact compacts the conversation context.
func (c *Client) Compact(ctx context.Context, instructions string) (CompactResult, error) {
	var f map[string]any
	if instructions != "" {
		f = map[string]any{"customInstructions": instructions}
	}
	return decode[CompactResult](c.Command(ctx, "compact", f))
}

// ErrSwitchCancelled means an extension cancelled a session switch.
var ErrSwitchCancelled = errors.New("pirpc: session switch cancelled")

// SwitchSession loads a session file.
func (c *Client) SwitchSession(ctx context.Context, path string) error {
	r, err := decode[struct {
		Cancelled bool `json:"cancelled"`
	}](c.Command(ctx, "switch_session", map[string]any{"sessionPath": path}))
	if err == nil && r.Cancelled {
		return ErrSwitchCancelled
	}
	return err
}

// SetAutoRetry enables or disables automatic retry of transient errors.
func (c *Client) SetAutoRetry(ctx context.Context, enabled bool) error {
	_, err := c.Command(ctx, "set_auto_retry", map[string]any{"enabled": enabled})
	return err
}

// SetAutoCompaction enables or disables automatic compaction.
func (c *Client) SetAutoCompaction(ctx context.Context, enabled bool) error {
	_, err := c.Command(ctx, "set_auto_compaction", map[string]any{"enabled": enabled})
	return err
}
