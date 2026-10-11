package comm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
)

// Tools are comm_search, commitment_list and commitment_update of the
// built-in MCP: how the personal agent uses what was collected (R12, R20,
// tech §5.2). They are offered only to a user who consented, so group and
// service agents never see them (R22). The user
// comes from the token of the session, never from the arguments.
// noteURL builds the link that opens a note in the Memory section; today
// gives the current day of the user.
func (s *Service) Tools(noteURL func(id uuid.UUID) string, today func(ctx context.Context, uid uuid.UUID) time.Time) []mcp.Tool {
	// offered to a user who consented — also after the function was switched
	// off: what is collected stays available to the user and the agent (R24)
	available := func(ctx context.Context, g mcp.Grant) bool {
		ok, err := s.Consented(ctx, g.UserID)
		return err == nil && ok
	}
	tagID := func(ctx context.Context, uid uuid.UUID, kind, name string) (*uuid.UUID, error) {
		if strings.TrimSpace(name) == "" {
			return nil, nil
		}
		l, err := s.Tags(ctx, uid, kind, name, 5)
		if err != nil {
			return nil, err
		}
		if len(l) == 0 {
			return nil, &mcp.ToolError{Msg: fmt.Sprintf("No %s like %q in the collected communications.", kind, name)}
		}
		return &l[0].ID, nil
	}
	return []mcp.Tool{
		{Name: "comm_search", ReadOnly: true, Available: available,
			Description: "Search the notes of the user's work communications (mail threads and chats collected into memory): what was discussed and agreed, with whom and on what topic. " +
				"Use it when the user asks about past correspondence or agreements. Give the links from the result in your answer.",
			InputSchema: mcp.Schema(map[string]any{
				"query":  map[string]any{"type": "string", "description": "What to look for, in the user's words"},
				"person": map[string]any{"type": "string", "description": "A name or an email of a participant"},
				"topic":  map[string]any{"type": "string", "description": "A topic"},
				"source": map[string]any{"type": "string", "description": "A source, e.g. gmail"},
				"from":   map[string]any{"type": "string", "description": "From this date, YYYY-MM-DD"},
				"to":     map[string]any{"type": "string", "description": "Before this date, YYYY-MM-DD"},
				"limit":  map[string]any{"type": "integer", "description": "Up to 20, default 8"},
			}),
			Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
				var in struct {
					Query, Person, Topic, Source, From, To string
					Limit                                  int
				}
				if err := json.Unmarshal(args, &in); err != nil {
					return "", &mcp.ToolError{Msg: "invalid arguments"}
				}
				f := Filter{Query: in.Query, Source: in.Source}
				var err error
				if f.Person, err = tagID(ctx, g.UserID, KindPerson, in.Person); err != nil {
					return "", err
				}
				if f.Topic, err = tagID(ctx, g.UserID, KindTopic, in.Topic); err != nil {
					return "", err
				}
				for _, d := range []struct {
					v   string
					dst **time.Time
				}{{in.From, &f.From}, {in.To, &f.To}} {
					if d.v != "" {
						t, err := ParseDate(d.v)
						if err != nil {
							return "", &mcp.ToolError{Msg: "dates are YYYY-MM-DD"}
						}
						*d.dst = &t.Time
					}
				}
				if in.Limit <= 0 || in.Limit > 20 {
					in.Limit = 8
				}
				var notes []Note
				if strings.TrimSpace(f.Query) == "" {
					l, err := s.List(ctx, g.UserID, f, pageOf(in.Limit))
					if err != nil {
						return "", &mcp.ToolError{Msg: err.Error()}
					}
					notes = l.Items
				} else if notes, err = s.Search(ctx, g.UserID, f, in.Limit); err != nil {
					return "", &mcp.ToolError{Msg: err.Error()}
				}
				if len(notes) == 0 {
					return "No notes found.", nil
				}
				var b strings.Builder
				for _, n := range notes {
					full, err := s.Get(ctx, g.UserID, n.ID)
					if err != nil {
						continue
					}
					fmt.Fprintf(&b, "## %s\n%s · %s · messages: %d\n", full.Title, full.LastAt.Format(dateLayout), full.Source, full.MessageCount)
					if len(full.People) > 0 {
						b.WriteString("People: " + labels(full.People) + "\n")
					}
					if len(full.Topics) > 0 {
						b.WriteString("Topics: " + labels(full.Topics) + "\n")
					}
					b.WriteString(*full.Summary + "\n")
					for _, a := range full.Appended {
						b.WriteString("Added later: " + a.Text + "\n")
					}
					for _, c := range full.Commitments {
						if c.State == Open {
							b.WriteString("Open commitment: " + describeCommitment(&c) + "\n")
						}
					}
					if noteURL != nil {
						b.WriteString("Note: " + noteURL(full.ID) + "\n")
					}
					for i, m := range full.Messages {
						if m.URL == "" || i >= 3 {
							continue
						}
						b.WriteString("Source: " + m.URL + "\n")
					}
					b.WriteString("\n")
				}
				return b.String(), nil
			}},
		{Name: "commitment_list", ReadOnly: true, Available: available,
			Description: "List the user's commitments found in their communications: what the user owes (mine) and what is owed to the user (theirs), with due days.",
			InputSchema: mcp.Schema(map[string]any{
				"state":     map[string]any{"type": "string", "enum": []string{"open", "closed", "all"}, "description": "Default open"},
				"direction": map[string]any{"type": "string", "enum": []string{"mine", "theirs"}},
				"person":    map[string]any{"type": "string", "description": "A name or an email"},
				"dueBefore": map[string]any{"type": "string", "description": "Due before this date, YYYY-MM-DD"},
			}),
			Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
				var in struct{ State, Direction, Person, DueBefore string }
				if err := json.Unmarshal(args, &in); err != nil {
					return "", &mcp.ToolError{Msg: "invalid arguments"}
				}
				f := CommitmentFilter{State: in.State, Direction: in.Direction}
				if today != nil {
					f.Today = today(ctx, g.UserID)
				}
				var err error
				if f.Person, err = tagID(ctx, g.UserID, KindPerson, in.Person); err != nil {
					return "", err
				}
				if in.DueBefore != "" {
					d, err := ParseDate(in.DueBefore)
					if err != nil {
						return "", &mcp.ToolError{Msg: "dueBefore is YYYY-MM-DD"}
					}
					f.DueBefore = &d.Time
				}
				l, err := s.Commitments(ctx, g.UserID, f, 100)
				if err != nil {
					return "", &mcp.ToolError{Msg: err.Error()}
				}
				if len(l) == 0 {
					return "No commitments.", nil
				}
				var b strings.Builder
				for i := range l {
					fmt.Fprintf(&b, "- [%s] %s\n", l[i].ID, describeCommitment(&l[i]))
				}
				return b.String(), nil
			}},
		{Name: "commitment_update", Available: available,
			Description: "Close, cancel or reopen a commitment of the user, or change its due day — when the user says something is done (\"I sent the report\", \"1 and 3 are done\" in reply to a digest), " +
				"is no longer needed, or moved. Find the id with commitment_list first; after the change name what you changed.",
			InputSchema: mcp.Schema(map[string]any{
				"id":    map[string]any{"type": "string"},
				"state": map[string]any{"type": "string", "enum": []string{"open", "done", "cancelled"}},
				"due":   map[string]any{"type": "string", "description": "A new due day YYYY-MM-DD, or an empty string for no due day"},
			}, "id"),
			Handler: func(ctx context.Context, g mcp.Grant, args json.RawMessage) (string, error) {
				var in struct {
					ID    string  `json:"id"`
					State *string `json:"state"`
					Due   *string `json:"due"`
				}
				if err := json.Unmarshal(args, &in); err != nil {
					return "", &mcp.ToolError{Msg: "invalid arguments"}
				}
				id, err := uuid.Parse(in.ID)
				if err != nil {
					return "", &mcp.ToolError{Msg: "invalid id"}
				}
				p := CommitmentPatch{State: in.State}
				if in.Due != nil {
					p.SetDue = true
					if *in.Due != "" {
						d, err := ParseDate(*in.Due)
						if err != nil {
							return "", &mcp.ToolError{Msg: "due is YYYY-MM-DD"}
						}
						p.Due = &d
					}
				}
				c, err := s.UpdateCommitment(ctx, g.UserID, id, p, ByAgent)
				if err != nil {
					return "", &mcp.ToolError{Msg: err.Error()}
				}
				return "Updated: " + describeCommitment(c), nil
			}},
	}
}

func labels(tags []Tag) string {
	out := make([]string, len(tags))
	for i, t := range tags {
		out[i] = t.Label
	}
	return strings.Join(out, ", ")
}

func describeCommitment(c *Commitment) string {
	who := ""
	if c.Person != nil {
		who = ", with " + c.Person.Label
	}
	due := "no due day"
	if c.Due != nil {
		due = "due " + c.Due.Format(dateLayout)
		if c.Overdue {
			due += " (overdue)"
		}
	}
	return fmt.Sprintf("%s — %s%s, %s, %s", c.Text, c.Direction, who, due, c.State)
}

func pageOf(limit int) httpx.Page { return httpx.Page{Limit: limit} }
