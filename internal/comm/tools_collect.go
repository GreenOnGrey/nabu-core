package comm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/GreenOnGrey/nabu-core/internal/collect"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// UserInfo is what the collector needs to know of the user.
type UserInfo struct {
	Email    string
	Timezone string
}

func denied(msg string) error {
	metrics.MemoryDeniedTools.Inc()
	return &mcp.ToolError{Msg: msg}
}

// CollectTools are the tools of a run of the collector agent (tech §5.1):
// comm_batch, comm_note_save, commitment_save and commitment_close. They
// exist only in the scope of a collector run. The user and the run come from
// the token; every argument is checked against the batch of the run, so the
// agent cannot write for another user or for a thread it was not given
// (R22, R26). The links of messages are built by the adapter, not the model.
func (s *Service) CollectTools(user func(ctx context.Context, uid uuid.UUID) UserInfo) []mcp.Tool {
	// load finds the running batch of the grant and the adapter of its item.
	load := func(ctx context.Context, g mcp.Grant) (*Batch, collect.Adapter, error) {
		if g.RunID == uuid.Nil {
			return nil, nil, denied("this session is not a run of the collector")
		}
		b, err := s.RunningBatch(ctx, g.UserID, g.RunID)
		if err != nil {
			return nil, nil, &mcp.ToolError{Msg: "the batch could not be read"}
		}
		if b == nil {
			return nil, nil, denied("the run is over: its batch is closed")
		}
		ad, ok := s.AdapterOf(ctx, b.ItemID)
		if !ok {
			return nil, nil, &mcp.ToolError{Msg: "the source of the batch is no longer a source of communications"}
		}
		return b, ad, nil
	}
	tool := func(name, desc string, schema map[string]any, h func(ctx context.Context, g mcp.Grant, b *Batch, ad collect.Adapter, args json.RawMessage) (string, error)) mcp.Tool {
		return mcp.Tool{Name: name, Description: desc, Scope: mcp.ScopeMemoryRun, InputSchema: schema,
			Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
				b, ad, err := load(ctx, g)
				if err != nil {
					return "", err
				}
				return h(ctx, g, b, ad, args)
			}}
	}
	return []mcp.Tool{
		tool("comm_batch", "Get the batch of this run: the threads to process, how to read each, the note already saved for a thread, the user's topics to choose from and the open commitments with the participants. Call it first.",
			mcp.Schema(map[string]any{}),
			func(ctx context.Context, g mcp.Grant, b *Batch, ad collect.Adapter, _ json.RawMessage) (string, error) {
				info := UserInfo{Timezone: "UTC"}
				if user != nil {
					info = user(ctx, g.UserID)
				}
				loc, err := time.LoadLocation(info.Timezone)
				if err != nil {
					loc = time.UTC
				}
				type threadOut struct {
					ThreadKey       string           `json:"threadKey"`
					DayKey          string           `json:"dayKey"`
					Source          string           `json:"source"`
					ReadWith        collect.ToolCall `json:"readWith"`
					Participants    []string         `json:"participants"`
					NewMessageIDs   []string         `json:"newMessageIds"`
					KnownMessageIDs []string         `json:"knownMessageIds"`
					Note            any              `json:"note"`
				}
				out := struct {
					UserEmail       string           `json:"userEmail"`
					Today           string           `json:"today"`
					Timezone        string           `json:"timezone"`
					Threads         []threadOut      `json:"threads"`
					Topics          []string         `json:"topics"`
					OpenCommitments []map[string]any `json:"openCommitments"`
				}{UserEmail: info.Email, Today: s.clock().In(loc).Format(dateLayout), Timezone: loc.String(), Topics: []string{}, OpenCommitments: []map[string]any{}}
				people := map[string]bool{}
				err = s.as(ctx, g.UserID, func(tx pgx.Tx) error {
					for _, t := range b.Threads {
						to := threadOut{ThreadKey: t.Key, DayKey: t.DayKey, Source: b.Source, ReadWith: ad.ReadCall(b.Source, t), Participants: t.Participants,
							NewMessageIDs: []string{}, KnownMessageIDs: []string{}}
						for _, p := range t.Participants {
							people[strings.ToLower(p)] = true
						}
						var noteID uuid.UUID
						var title, summary string
						var edited bool
						err := tx.QueryRow(ctx, `SELECT id, title, summary, user_edited FROM comm_notes WHERE user_id = $1 AND item_id = $2 AND source = $3 AND thread_key = $4 AND day_key = $5`,
							g.UserID, b.ItemID, b.Source, t.Key, t.DayKey).Scan(&noteID, &title, &summary, &edited)
						known := map[string]bool{}
						if err == nil {
							to.Note = map[string]any{"title": title, "summary": summary, "userEdited": edited}
							rows, err := tx.Query(ctx, `SELECT message_id FROM comm_note_messages WHERE user_id = $1 AND note_id = $2`, g.UserID, noteID)
							if err != nil {
								return err
							}
							for rows.Next() {
								var id string
								if err := rows.Scan(&id); err != nil {
									rows.Close()
									return err
								}
								known[id] = true
							}
							rows.Close()
						}
						for _, m := range t.Messages {
							if known[m.ID] {
								to.KnownMessageIDs = append(to.KnownMessageIDs, m.ID)
							} else {
								to.NewMessageIDs = append(to.NewMessageIDs, m.ID)
							}
						}
						out.Threads = append(out.Threads, to)
					}
					return nil
				})
				if err != nil {
					return "", &mcp.ToolError{Msg: "the batch could not be read"}
				}
				if topics, err := s.Tags(ctx, g.UserID, KindTopic, "", 30); err == nil {
					for _, t := range topics {
						out.Topics = append(out.Topics, t.Label)
					}
				}
				if open, err := s.Commitments(ctx, g.UserID, CommitmentFilter{State: Open}, 200); err == nil {
					for _, c := range open {
						if c.Person == nil || !people[strings.ToLower(c.Person.Email)] {
							continue
						}
						item := map[string]any{"id": c.ID, "text": c.Text, "direction": c.Direction, "person": c.Person.Email}
						if c.Due != nil {
							item["due"] = c.Due.Format(dateLayout)
						}
						out.OpenCommitments = append(out.OpenCommitments, item)
					}
				}
				raw, _ := json.Marshal(out)
				return string(raw), nil
			}),
		tool("comm_note_save", "Save the note of a thread of the batch: what was discussed, what was decided, what stays open. Never copy passwords, keys or tokens. "+
			"Choose topics from the batch's list when one fits; create a new one only when none does; give none when the thread has no project or matter.",
			mcp.Schema(map[string]any{
				"threadKey": map[string]any{"type": "string"},
				"dayKey":    map[string]any{"type": "string", "description": "As given in the batch; empty for mail"},
				"title":     map[string]any{"type": "string", "description": "A short title of the conversation"},
				"summary":   map[string]any{"type": "string", "description": "Up to 4000 characters, in the language of the conversation"},
				"people": map[string]any{"type": "array", "description": "The participants other than the user", "items": map[string]any{"type": "object",
					"properties": map[string]any{"email": map[string]any{"type": "string"}, "name": map[string]any{"type": "string"}}, "required": []string{"email"}}},
				"topics": map[string]any{"type": "array", "description": "From 0 to 5 topics; empty for automated notices and one-off messages", "items": map[string]any{"type": "string"}},
			}, "threadKey", "title", "summary", "topics"),
			func(ctx context.Context, g mcp.Grant, b *Batch, ad collect.Adapter, args json.RawMessage) (string, error) {
				var in struct {
					ThreadKey, DayKey, Title, Summary string
					People                            []PersonIn
					Topics                            []string
				}
				if err := json.Unmarshal(args, &in); err != nil {
					return "", &mcp.ToolError{Msg: "invalid arguments"}
				}
				t := b.thread(in.ThreadKey, in.DayKey)
				if t == nil {
					return "", denied("the thread " + in.ThreadKey + " is not in the batch of this run")
				}
				// the participants are those the source named: the model cannot add an address
				allowed := map[string]bool{}
				for _, p := range t.Participants {
					allowed[strings.ToLower(p)] = true
				}
				var people []PersonIn
				for _, p := range in.People {
					if allowed[strings.ToLower(strings.TrimSpace(p.Email))] {
						people = append(people, p)
					}
				}
				for _, p := range t.Participants { // those the model left out still are participants
					found := false
					for _, x := range people {
						if strings.EqualFold(x.Email, p) {
							found = true
						}
					}
					if !found {
						people = append(people, PersonIn{Email: p})
					}
				}
				msgs := make([]Message, 0, len(t.Messages))
				for _, m := range t.Messages {
					url := m.URL
					if url == "" {
						url = ad.Link(b.Source, *t, m.ID)
					}
					msgs = append(msgs, Message{ID: m.ID, Author: m.Author, SentAt: m.SentAt, URL: url})
				}
				info := UserInfo{}
				if user != nil {
					info = user(ctx, g.UserID)
				}
				res, err := s.SaveNote(ctx, g.UserID, SaveNote{ItemID: b.ItemID, Source: b.Source, ThreadKey: t.Key, DayKey: t.DayKey, Title: in.Title, Summary: in.Summary,
					People: people, Topics: in.Topics, Messages: msgs, TimePrecision: ad.TimePrecision(b.Source), UserEmail: info.Email})
				if err != nil {
					return "", &mcp.ToolError{Msg: err.Error()}
				}
				if res.Skipped {
					return "Skipped: the user deleted the note of this thread, it is not collected again.", nil
				}
				notes := 0
				if res.Created {
					notes = 1
				}
				_ = s.as(ctx, g.UserID, func(tx pgx.Tx) error { return s.countBatch(ctx, tx, g.UserID, b.ID, notes, res.NewMessages) })
				return fmt.Sprintf("Saved the note of %s (new messages: %d).", t.Key, res.NewMessages), nil
			}),
		tool("commitment_save", "Save a commitment found in a thread of the batch, after its note is saved: who owes what to whom. direction is mine when the user owes, theirs when it is owed to the user.",
			mcp.Schema(map[string]any{
				"threadKey":      map[string]any{"type": "string"},
				"dayKey":         map[string]any{"type": "string"},
				"text":           map[string]any{"type": "string", "description": "What to do, one sentence"},
				"direction":      map[string]any{"type": "string", "enum": []string{Mine, Theirs}},
				"personEmail":    map[string]any{"type": "string", "description": "The other side: a participant of the thread"},
				"due":            map[string]any{"type": "string", "description": "The due day YYYY-MM-DD when the text names it; omit otherwise"},
				"basisMessageId": map[string]any{"type": "string", "description": "The message the commitment comes from"},
			}, "threadKey", "text", "direction"),
			func(ctx context.Context, g mcp.Grant, b *Batch, _ collect.Adapter, args json.RawMessage) (string, error) {
				var in struct {
					ThreadKey, DayKey, Text, Direction, PersonEmail, Due, BasisMessageID string
				}
				if err := json.Unmarshal(args, &in); err != nil {
					return "", &mcp.ToolError{Msg: "invalid arguments"}
				}
				t := b.thread(in.ThreadKey, in.DayKey)
				if t == nil {
					return "", denied("the thread " + in.ThreadKey + " is not in the batch of this run")
				}
				sc := SaveCommitment{ItemID: b.ItemID, Source: b.Source, ThreadKey: t.Key, DayKey: t.DayKey, Text: in.Text, Direction: in.Direction}
				for _, p := range t.Participants {
					if strings.EqualFold(p, strings.TrimSpace(in.PersonEmail)) {
						sc.PersonEmail = p
					}
				}
				for _, m := range t.Messages {
					if m.ID == in.BasisMessageID {
						sc.BasisMessageID = m.ID
					}
				}
				if in.Due != "" {
					d, err := ParseDate(in.Due)
					if err != nil {
						return "", &mcp.ToolError{Msg: "due is YYYY-MM-DD"}
					}
					sc.Due = &d
				}
				id, err := s.SaveCommitment(ctx, g.UserID, sc)
				if err != nil {
					return "", &mcp.ToolError{Msg: err.Error()}
				}
				return "Saved the commitment " + id.String() + ".", nil
			}),
		tool("commitment_close", "Close an open commitment from the batch's list when a new message shows it is done; name the message.",
			mcp.Schema(map[string]any{
				"commitmentId":   map[string]any{"type": "string"},
				"threadKey":      map[string]any{"type": "string", "description": "The thread of the message that shows it is done"},
				"dayKey":         map[string]any{"type": "string"},
				"basisMessageId": map[string]any{"type": "string"},
			}, "commitmentId", "threadKey", "basisMessageId"),
			func(ctx context.Context, g mcp.Grant, b *Batch, _ collect.Adapter, args json.RawMessage) (string, error) {
				var in struct {
					CommitmentID, ThreadKey, DayKey, BasisMessageID string
				}
				if err := json.Unmarshal(args, &in); err != nil {
					return "", &mcp.ToolError{Msg: "invalid arguments"}
				}
				id, err := uuid.Parse(in.CommitmentID)
				if err != nil {
					return "", &mcp.ToolError{Msg: "invalid commitmentId"}
				}
				t := b.thread(in.ThreadKey, in.DayKey)
				if t == nil {
					return "", denied("the thread " + in.ThreadKey + " is not in the batch of this run")
				}
				basis := ""
				for _, m := range t.Messages {
					if m.ID == in.BasisMessageID {
						basis = m.ID
					}
				}
				if basis == "" {
					return "", denied("the message is not in the thread: a commitment is closed only by a message of the batch")
				}
				// only an open commitment with a participant of the batch — what comm_batch listed
				people := map[string]bool{}
				for _, th := range b.Threads {
					for _, p := range th.Participants {
						people[strings.ToLower(p)] = true
					}
				}
				open, err := s.Commitments(ctx, g.UserID, CommitmentFilter{State: Open}, 500)
				if err != nil {
					return "", &mcp.ToolError{Msg: "the commitments could not be read"}
				}
				listed := false
				for _, c := range open {
					if c.ID == id && c.Person != nil && people[strings.ToLower(c.Person.Email)] {
						listed = true
					}
				}
				if !listed {
					return "", denied("the commitment is not among the open commitments of this batch")
				}
				done := Done
				c, err := s.UpdateCommitment(ctx, g.UserID, id, CommitmentPatch{State: &done, CloseBasisMessageID: basis}, ByCollector)
				if err != nil {
					return "", &mcp.ToolError{Msg: err.Error()}
				}
				return "Closed: " + c.Text, nil
			}),
	}
}
