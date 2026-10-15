package collect

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Mail is the adapter of a mailbox reached over IMAP through the imap-mcp
// server of the stand (FTR.NAB.CMN-0006 R20–R22, FTR.NAB.CMN-0003 arch §7.3):
// one adapter for every provider — Yandex Mail, or any other with OAuth for
// IMAP. What differs between providers (the IMAP host, the address of the
// mailbox, the link to a message) is set in the deployment of the server; the
// server answers with JSON, so nothing here knows the provider.
//
// IMAP has no thread id: a thread is the Message-ID of its first message
// (References, In-Reply-To), which the server gives as threadId. Messages
// are told apart by Message-ID, so a message seen in two folders is one.
type Mail struct {
	// Overlap is how far before the cursor the search starts (MEMORY_GMAIL_OVERLAP):
	// IMAP searches by day, so at least a day.
	Overlap time.Duration
}

const (
	mailSource   = "mail"
	mailSearch   = "search_messages"
	mailFolders  = "list_folders"
	mailThread   = "read_thread"
	mailPageSize = 50
	mailMaxPages = 40
)

func init() { Register(&Mail{Overlap: 24 * time.Hour}) }

// Kind is "mail".
func (m *Mail) Kind() string { return "mail" }

// Sources: one mailbox; its title in the interface is the title of the item.
func (m *Mail) Sources() []Source { return []Source{{Key: mailSource, Title: ""}} }

// ReadTools: the thread with its bodies.
func (m *Mail) ReadTools() []string { return []string{mailThread} }

// ReadCall reads the whole thread; since — the day before its first known
// message: a server without search by headers (Yandex) reads from that day.
func (m *Mail) ReadCall(_ string, t Thread) ToolCall {
	args := map[string]any{"thread_id": t.Key}
	var first *time.Time
	for _, msg := range t.Messages {
		if msg.SentAt != nil && (first == nil || msg.SentAt.Before(*first)) {
			first = msg.SentAt
		}
	}
	if first != nil {
		args["since"] = first.UTC().AddDate(0, 0, -1).Format("2006-01-02")
	}
	return ToolCall{Tool: mailThread, Args: args}
}

// ThreadOf reads thread_id of a read_thread call.
func (m *Mail) ThreadOf(tool string, args json.RawMessage) (string, bool) {
	if tool != mailThread {
		return "", false
	}
	var a struct {
		ThreadID string `json:"thread_id"`
	}
	if err := json.Unmarshal(args, &a); err != nil || a.ThreadID == "" {
		return "", false
	}
	return a.ThreadID, true
}

// Link: the link to a message comes with the message from the server (Msg.URL).
func (m *Mail) Link(string, Thread, string) string { return "" }

// TimePrecision: the Date header gives the time.
func (m *Mail) TimePrecision(string) string { return "minute" }

// CheckTools needs the folders, the search and the thread.
func (m *Mail) CheckTools(tools []string) error {
	return missing(tools, []string{mailFolders, mailSearch, mailThread})
}

type mailFolder struct {
	Name    string  `json:"name"`
	Special *string `json:"special"`
}

type mailMessage struct {
	From      string   `json:"from"`
	To        []string `json:"to"`
	Cc        []string `json:"cc"`
	Date      string   `json:"date"`
	MessageID string   `json:"messageId"`
	ThreadID  string   `json:"threadId"`
	Bulk      bool     `json:"bulk"`
	WebLink   string   `json:"webLink"`
}

// folders are the folders to collect from: the whole mailbox («All Mail» when
// there is one), without trash, spam, drafts and the folders the user excluded.
func (m *Mail) folders(ctx context.Context, call Caller, ex Exclusions) ([]string, error) {
	raw, err := call(ctx, mailFolders, map[string]any{})
	if err != nil {
		return nil, err
	}
	var list []mailFolder
	if err := json.Unmarshal(raw, &list); err != nil {
		return nil, fmt.Errorf("mail %s: unexpected answer: %w", mailFolders, err)
	}
	excluded := map[string]bool{}
	for _, f := range ex.Folders {
		excluded[strings.ToLower(strings.TrimSpace(f))] = true
	}
	var all, out []string
	for _, f := range list {
		role := ""
		if f.Special != nil {
			role = *f.Special
		}
		if excluded[strings.ToLower(f.Name)] {
			continue
		}
		switch role {
		case "All":
			all = append(all, f.Name)
		case "Trash", "Junk", "Spam", "Drafts":
		default:
			out = append(out, f.Name)
		}
	}
	if len(all) > 0 && len(ex.Folders) == 0 {
		return all, nil // «All Mail» holds every message; with exclusions the folders are read one by one
	}
	return out, nil
}

// search reads the messages of [from, to) in a folder page by page; full —
// the window did not fit mailMaxPages.
func (m *Mail) search(ctx context.Context, call Caller, folder string, from, to time.Time) (msgs []mailMessage, full bool, err error) {
	args := map[string]any{"folder": folder, "since": from.UTC().Format("2006-01-02"), "before": to.UTC().AddDate(0, 0, 1).Format("2006-01-02"),
		"limit": mailPageSize}
	for page := 0; page < mailMaxPages; page++ {
		args["offset"] = page * mailPageSize
		raw, err := call(ctx, mailSearch, args)
		if err != nil {
			return nil, false, err
		}
		var res []mailMessage
		if err := json.Unmarshal(raw, &res); err != nil {
			return nil, false, fmt.Errorf("mail %s: unexpected answer: %w", mailSearch, err)
		}
		msgs = append(msgs, res...)
		if len(res) < mailPageSize {
			return msgs, false, nil
		}
	}
	return msgs, true, nil
}

// Changes searches every folder from the cursor minus the overlap (by day:
// IMAP searches by dates), halving a window too large for one tick like the
// adapter of Gmail; what was already collected is told apart by Message-ID.
func (m *Mail) Changes(ctx context.Context, call Caller, _ string, cursor string, since, now time.Time, ex Exclusions) (ChangeSet, error) {
	from := since
	if cursor != "" {
		t, err := time.Parse(time.RFC3339, cursor)
		if err != nil {
			return ChangeSet{}, fmt.Errorf("invalid mail cursor %q", cursor)
		}
		from = t.Add(-max(m.Overlap, 24*time.Hour))
	}
	folders, err := m.folders(ctx, call, ex)
	if err != nil {
		return ChangeSet{}, err
	}
	to := now
	var found []mailMessage
	for halvings := 0; ; halvings++ {
		found = found[:0]
		full := false
		for _, f := range folders {
			msgs, ff, err := m.search(ctx, call, f, from, to)
			if err != nil {
				return ChangeSet{}, err
			}
			found = append(found, msgs...)
			full = full || ff
		}
		if !full || halvings == gmailMaxHalvings || to.Sub(from) <= 48*time.Hour {
			break
		}
		to = from.Add(to.Sub(from) / 2)
	}
	out := ChangeSet{Cursor: to.UTC().Format(time.RFC3339)}
	excluded := map[string]bool{}
	for _, s := range ex.Senders {
		excluded[strings.ToLower(strings.TrimSpace(s))] = true
	}
	bot := map[string]bool{}
	for _, b := range ex.BotAddresses {
		bot[strings.ToLower(strings.TrimSpace(b))] = true
	}
	threads := map[string]*Thread{}
	people := map[string]map[string]bool{}
	seen := map[string]bool{}
	var order []string
	for _, msg := range found {
		if msg.MessageID == "" || seen[msg.MessageID] {
			continue
		}
		seen[msg.MessageID] = true
		sender := address(msg.From)
		rcpts := append(append([]string{}, msg.To...), msg.Cc...)
		skip := msg.Bulk || msg.ThreadID == "" || automatic(sender) || excluded[sender] || bot[sender]
		for _, a := range rcpts {
			skip = skip || bot[address(a)] // a letter to the bot is a conversation with the agent, not with people
		}
		if skip {
			out.Skipped++
			continue
		}
		t := threads[msg.ThreadID]
		if t == nil {
			t = &Thread{Key: msg.ThreadID}
			threads[msg.ThreadID] = t
			people[msg.ThreadID] = map[string]bool{}
			order = append(order, msg.ThreadID)
		}
		item := Msg{ID: msg.MessageID, Author: sender, URL: msg.WebLink}
		if d, err := time.Parse(time.RFC3339, msg.Date); err == nil {
			d = d.UTC()
			item.SentAt = &d
		}
		t.Messages = append(t.Messages, item)
		for _, a := range append([]string{sender}, rcpts...) {
			if a = address(a); a != "" && !automatic(a) {
				people[msg.ThreadID][a] = true
			}
		}
	}
	for _, key := range order {
		t := threads[key]
		for a := range people[key] {
			t.Participants = append(t.Participants, a)
		}
		sort.Strings(t.Participants)
		sort.SliceStable(t.Messages, func(i, j int) bool {
			a, b := t.Messages[i].SentAt, t.Messages[j].SentAt
			return a != nil && b != nil && a.Before(*b)
		})
		out.Threads = append(out.Threads, *t)
	}
	return out, nil
}
