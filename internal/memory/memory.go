// Package memory keeps the long-term memory of personal agents
// (FTR.NAB.CMN-0001 R6, arch §9, tech §3.3): the agent saves facts with the
// memory_save tool, the user adds, edits and deletes records on the site.
// Deleting a record removes it from the instructions of new sessions and of
// open ones from the next message.
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
)

// MaxText bounds a record.
const MaxText = 2000

// InstructionLimit is how many records go into the instructions (arch §4.2).
const InstructionLimit = 50

// Record is one memory record.
type Record struct {
	ID        uuid.UUID `json:"id"`
	Text      string    `json:"text"`
	Source    string    `json:"source"` // agent | user
	Pinned    bool      `json:"pinned"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Service is the memory store.
type Service struct {
	Pool   *pgxpool.Pool
	Events events.Publisher
}

func (s *Service) changed(ctx context.Context, uid uuid.UUID) {
	if s.Events != nil {
		s.Events.Publish(ctx, events.Event{Type: events.MemoryChanged, UserID: &uid, Data: map[string]any{}})
	}
}

// List lists records; q searches the full-text index.
func (s *Service) List(ctx context.Context, uid uuid.UUID, q string, page httpx.Page) (httpx.List[Record], error) {
	args := []any{uid, strings.TrimSpace(q), page.Limit + 1}
	cond := ""
	if page.Cursor != nil {
		cond = ` AND (updated_at, id) < ($4, $5)`
		args = append(args, page.Cursor.T, page.Cursor.ID)
	}
	rows, err := s.Pool.Query(ctx, `SELECT id, text, source, pinned, created_at, updated_at FROM memories
		WHERE user_id = $1 AND ($2 = '' OR search_vector @@ plainto_tsquery('simple', $2) OR text ILIKE '%' || $2 || '%')`+cond+`
		ORDER BY updated_at DESC, id DESC LIMIT $3`, args...)
	if err != nil {
		return httpx.List[Record]{}, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.ID, &r.Text, &r.Source, &r.Pinned, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return httpx.List[Record]{}, err
		}
		out = append(out, r)
	}
	return httpx.NewList(out, page.Limit, func(r Record) (time.Time, string) { return r.UpdatedAt, r.ID.String() }), rows.Err()
}

// ForInstructions returns the pinned and the latest records (up to 50).
func (s *Service) ForInstructions(ctx context.Context, uid uuid.UUID) ([]Record, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id, text, source, pinned, created_at, updated_at FROM memories
		WHERE user_id = $1 ORDER BY pinned DESC, updated_at DESC LIMIT $2`, uid, InstructionLimit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.ID, &r.Text, &r.Source, &r.Pinned, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func clean(text string) (string, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", apperr.Unprocessable("invalid_text", "the text is required").With("field", "text")
	}
	if len([]rune(text)) > MaxText {
		return "", apperr.Unprocessable("invalid_text", fmt.Sprintf("the text is longer than %d characters", MaxText)).With("field", "text")
	}
	return text, nil
}

// Add adds a record.
func (s *Service) Add(ctx context.Context, uid uuid.UUID, text, source string, pinned bool, conv *uuid.UUID) (*Record, error) {
	text, err := clean(text)
	if err != nil {
		return nil, err
	}
	var r Record
	err = s.Pool.QueryRow(ctx, `INSERT INTO memories (user_id, text, source, pinned, conversation_id) VALUES ($1,$2,$3,$4,$5)
		RETURNING id, text, source, pinned, created_at, updated_at`, uid, text, source, pinned, conv).
		Scan(&r.ID, &r.Text, &r.Source, &r.Pinned, &r.CreatedAt, &r.UpdatedAt)
	if err == nil {
		s.changed(ctx, uid)
	}
	return &r, err
}

// Update edits a record of the user.
func (s *Service) Update(ctx context.Context, uid, id uuid.UUID, text *string, pinned *bool) (*Record, error) {
	if text != nil {
		t, err := clean(*text)
		if err != nil {
			return nil, err
		}
		text = &t
	}
	var r Record
	err := s.Pool.QueryRow(ctx, `UPDATE memories SET text = COALESCE($3, text), pinned = COALESCE($4, pinned), updated_at = now()
		WHERE user_id = $1 AND id = $2 RETURNING id, text, source, pinned, created_at, updated_at`, uid, id, text, pinned).
		Scan(&r.ID, &r.Text, &r.Source, &r.Pinned, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return nil, apperr.NotFound("not_found", "record not found")
	}
	s.changed(ctx, uid)
	return &r, nil
}

// Delete deletes a record of the user.
func (s *Service) Delete(ctx context.Context, uid, id uuid.UUID) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM memories WHERE user_id = $1 AND id = $2`, uid, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return apperr.NotFound("not_found", "record not found")
	}
	s.changed(ctx, uid)
	return nil
}

// Clear deletes all records; confirm must be "all" (MEM-03).
func (s *Service) Clear(ctx context.Context, uid uuid.UUID, confirm string) error {
	if confirm != "all" {
		return apperr.BadRequest("confirmation_required", `send {"confirm":"all"} to clear the memory`)
	}
	_, err := s.Pool.Exec(ctx, `DELETE FROM memories WHERE user_id = $1`, uid)
	if err == nil {
		s.changed(ctx, uid)
	}
	return err
}

// Hash is a fingerprint of the records in the instructions: a session whose
// instructions are older gets the update with the next message.
func Hash(rs []Record) string {
	var b strings.Builder
	for _, r := range rs {
		b.WriteString(r.ID.String())
		b.WriteString(r.UpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	return b.String()
}

// Routes mounts /memories (tech §3.3).
func (s *Service) Routes(r chi.Router) {
	r.Get("/memories", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		page, err := httpx.ParsePage(r)
		if err != nil {
			return err
		}
		l, err := s.List(r.Context(), p.UserID, r.URL.Query().Get("q"), page)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, l)
		return nil
	}))
	r.Post("/memories", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		var in struct {
			Text   string `json:"text"`
			Pinned bool   `json:"pinned"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		rec, err := s.Add(r.Context(), p.UserID, in.Text, "user", in.Pinned, nil)
		if err != nil {
			return err
		}
		httpx.JSON(w, http.StatusCreated, rec)
		return nil
	}))
	r.Patch("/memories/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		var in struct {
			Text   *string `json:"text"`
			Pinned *bool   `json:"pinned"`
		}
		if err := httpx.Decode(r, &in); err != nil {
			return err
		}
		rec, err := s.Update(r.Context(), p.UserID, id, in.Text, in.Pinned)
		if err != nil {
			return err
		}
		httpx.JSON(w, 200, rec)
		return nil
	}))
	r.Delete("/memories/{id}", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		id, err := httpx.ParamUUID(r, "id")
		if err != nil {
			return err
		}
		if err := s.Delete(r.Context(), p.UserID, id); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
	r.Delete("/memories", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		p, err := httpx.MustPrincipal(r)
		if err != nil {
			return err
		}
		var in struct {
			Confirm string `json:"confirm"`
		}
		if r.ContentLength != 0 {
			if err := httpx.Decode(r, &in); err != nil {
				return err
			}
		}
		if err := s.Clear(r.Context(), p.UserID, in.Confirm); err != nil {
			return err
		}
		httpx.NoContent(w)
		return nil
	}))
}

// Tools are memory_save, memory_search and memory_delete of the built-in MCP.
func (s *Service) Tools() []mcp.Tool {
	return []mcp.Tool{
		{Name: "memory_save", Description: "Save a durable fact or preference of the user to long-term memory. " +
			"Use it only for information that stays true across conversations (names, roles, preferences, recurring context). The user sees every saved record.",
			InputSchema: mcp.Schema(map[string]any{
				"text":   map[string]any{"type": "string", "description": "The fact, one or two sentences."},
				"pinned": map[string]any{"type": "boolean", "description": "Always keep it in the instructions."},
			}, "text"),
			Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
				var in struct {
					Text   string `json:"text"`
					Pinned bool   `json:"pinned"`
				}
				if err := json.Unmarshal(args, &in); err != nil {
					return "", &mcp.ToolError{Msg: "invalid arguments"}
				}
				var conv *uuid.UUID
				if g.Conversation != uuid.Nil {
					conv = &g.Conversation
				}
				r, err := s.Add(ctx, g.UserID, in.Text, "agent", in.Pinned, conv)
				if err != nil {
					return "", &mcp.ToolError{Msg: err.Error()}
				}
				return fmt.Sprintf("Saved to memory (id %s): %s", r.ID, r.Text), nil
			}},
		{Name: "memory_search", Description: "Search the user's long-term memory by words.", ReadOnly: true,
			InputSchema: mcp.Schema(map[string]any{"query": map[string]any{"type": "string"}}, "query"),
			Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
				var in struct {
					Query string `json:"query"`
				}
				_ = json.Unmarshal(args, &in)
				l, err := s.List(ctx, g.UserID, in.Query, httpx.Page{Limit: 20})
				if err != nil {
					return "", err
				}
				if len(l.Items) == 0 {
					return "No records found.", nil
				}
				var b strings.Builder
				for _, r := range l.Items {
					fmt.Fprintf(&b, "- [%s] %s\n", r.ID, r.Text)
				}
				return b.String(), nil
			}},
		{Name: "memory_delete", Description: "Delete a record of the user's memory by id (when the user asks to forget something).",
			InputSchema: mcp.Schema(map[string]any{"id": map[string]any{"type": "string"}}, "id"),
			Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
				var in struct {
					ID string `json:"id"`
				}
				_ = json.Unmarshal(args, &in)
				id, err := uuid.Parse(in.ID)
				if err != nil {
					return "", &mcp.ToolError{Msg: "invalid id"}
				}
				if err := s.Delete(ctx, g.UserID, id); err != nil {
					return "", &mcp.ToolError{Msg: err.Error()}
				}
				return "Deleted.", nil
			}},
	}
}
