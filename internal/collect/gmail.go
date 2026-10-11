package collect

import (
	"context"
	"encoding/json"
	"fmt"
	"net/mail"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Gmail is the adapter of Google's Gmail MCP server
// (gmailmcp.googleapis.com; FTR.NAB.CMN-0003 arch §7.2, tech §6.2). The
// server has no history cursor, so the changes are a search by time with an
// overlap; what was already collected is told apart by message ids.
type Gmail struct {
	// Overlap is how far before the cursor the search starts (MEMORY_GMAIL_OVERLAP).
	Overlap time.Duration
}

const (
	gmailSource   = "gmail"
	gmailPageSize = 50
	gmailMaxPages = 40
)

func init() { Register(&Gmail{Overlap: 24 * time.Hour}) }

// Kind is "gmail".
func (g *Gmail) Kind() string { return "gmail" }

// Sources: one mailbox.
func (g *Gmail) Sources() []Source { return []Source{{Key: gmailSource, Title: "Gmail"}} }

// ReadTools: the thread with its bodies.
func (g *Gmail) ReadTools() []string { return []string{"get_thread"} }

// ReadCall reads the thread with full content.
func (g *Gmail) ReadCall(_ string, t Thread) ToolCall {
	return ToolCall{Tool: "get_thread", Args: map[string]any{"threadId": t.Key, "messageFormat": "FULL_CONTENT"}}
}

// ThreadOf reads threadId of a get_thread call.
func (g *Gmail) ThreadOf(tool string, args json.RawMessage) (string, bool) {
	if tool != "get_thread" {
		return "", false
	}
	var a struct {
		ThreadID string `json:"threadId"`
	}
	if err := json.Unmarshal(args, &a); err != nil || a.ThreadID == "" {
		return "", false
	}
	return a.ThreadID, true
}

// Link opens the thread: the server gives no link to a single message.
func (g *Gmail) Link(_ string, t Thread, _ string) string {
	return "https://mail.google.com/mail/#all/" + t.Key
}

// TimePrecision: the server gives the date of a message without the time.
func (g *Gmail) TimePrecision(string) string { return "day" }

// CheckTools needs the search and the thread.
func (g *Gmail) CheckTools(tools []string) error {
	return missing(tools, []string{"search_threads", "get_thread"})
}

// gmailQuery builds the search: everything after the time, without drafts,
// chats, automatic categories, the excluded senders and labels and the
// letters to and from the Nabu bot (R4).
func gmailQuery(after time.Time, ex Exclusions) string {
	q := []string{"after:" + strconv.FormatInt(after.Unix(), 10), "-in:draft", "-in:chat",
		"-category:promotions", "-category:social", "-category:updates", "-category:forums"}
	for _, s := range ex.Senders {
		if s = gmailTerm(s); s != "" {
			q = append(q, "-from:"+s)
		}
	}
	for _, f := range ex.Folders {
		if f = gmailTerm(f); f != "" {
			q = append(q, "-label:"+f)
		}
	}
	for _, b := range ex.BotAddresses {
		if b = gmailTerm(b); b != "" {
			q = append(q, "-from:"+b, "-to:"+b, "-cc:"+b)
		}
	}
	return strings.Join(q, " ")
}

// gmailTerm keeps a value from breaking out of its operator.
func gmailTerm(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || strings.ContainsAny(s, " \t\n\"(){}") {
		return ""
	}
	return s
}

var automaticLocal = []string{"noreply", "no-reply", "no_reply", "donotreply", "do-not-reply", "mailer-daemon", "postmaster", "notifications", "notification", "bounce", "bounces"}

// automatic reports an address systems write from (R4, FTR.NAB.CMN-0002 R7).
func automatic(addr string) bool {
	local, _, _ := strings.Cut(strings.ToLower(addr), "@")
	for _, a := range automaticLocal {
		if local == a || strings.HasPrefix(local, a+"+") || strings.HasPrefix(local, a+"-") || strings.HasPrefix(local, a+".") {
			return true
		}
	}
	return false
}

func address(s string) string {
	if a, err := mail.ParseAddress(s); err == nil {
		return strings.ToLower(a.Address)
	}
	return strings.ToLower(strings.Trim(strings.TrimSpace(s), "<>"))
}

// SetGmailOverlap sets how far before the cursor the search of the Gmail
// adapter starts (MEMORY_GMAIL_OVERLAP).
func SetGmailOverlap(d time.Duration) {
	if g, ok := registry["gmail"].(*Gmail); ok && d > 0 {
		g.Overlap = d
	}
}

// gmailMaxHalvings bounds how many times a window too large for one tick is halved.
const gmailMaxHalvings = 10

type gmailThread struct {
	ID       string `json:"id"`
	Messages []struct {
		ID           string   `json:"id"`
		Sender       string   `json:"sender"`
		ToRecipients []string `json:"toRecipients"`
		CcRecipients []string `json:"ccRecipients"`
		Date         string   `json:"date"`
		LabelIDs     []string `json:"labelIds"`
	} `json:"messages"`
}

// search reads the threads with messages in [from, to), page by page, with
// metadata only; full reports that the window did not fit gmailMaxPages.
func (g *Gmail) search(ctx context.Context, call Caller, from, to time.Time, ex Exclusions) (threads []gmailThread, full bool, err error) {
	args := map[string]any{"query": gmailQuery(from, ex) + " before:" + strconv.FormatInt(to.Unix(), 10), "pageSize": gmailPageSize, "view": "THREAD_VIEW_METADATA_ONLY"}
	for page := 0; page < gmailMaxPages; page++ {
		raw, err := call(ctx, "search_threads", args)
		if err != nil {
			return nil, false, err
		}
		var res struct {
			Threads       []gmailThread `json:"threads"`
			NextPageToken string        `json:"nextPageToken"`
		}
		if err := json.Unmarshal(raw, &res); err != nil {
			return nil, false, fmt.Errorf("gmail search_threads: unexpected answer: %w", err)
		}
		threads = append(threads, res.Threads...)
		if res.NextPageToken == "" {
			return threads, false, nil
		}
		args["pageToken"] = res.NextPageToken
	}
	return threads, true, nil
}

// Changes searches the threads with messages after the cursor minus the
// overlap. A window with more threads than one tick reads is halved until it
// fits: the cursor then moves to the end of what was read and the next tick
// goes on from there, so a large mailbox is collected in steps and nothing is
// skipped (R15). The exclusions of the user and the letters of the Nabu bot
// are left out by the query and once more by the metadata of every message,
// so a thread found by another letter does not bring them in (R4).
func (g *Gmail) Changes(ctx context.Context, call Caller, _ string, cursor string, since, now time.Time, ex Exclusions) (ChangeSet, error) {
	from := since
	if cursor != "" {
		t, err := time.Parse(time.RFC3339, cursor)
		if err != nil {
			return ChangeSet{}, fmt.Errorf("invalid gmail cursor %q", cursor)
		}
		from = t.Add(-g.Overlap)
	}
	to := now.Add(time.Second) // before: is exclusive
	var found []gmailThread
	for halvings := 0; ; halvings++ {
		threads, full, err := g.search(ctx, call, from, to, ex)
		if err != nil {
			return ChangeSet{}, err
		}
		if !full || halvings == gmailMaxHalvings || to.Sub(from) <= time.Hour {
			found = threads
			break
		}
		to = from.Add(to.Sub(from) / 2)
	}
	end := now
	if to.Before(now) {
		end = to
	}
	out := ChangeSet{Cursor: end.UTC().Format(time.RFC3339)}
	excluded := map[string]bool{}
	for _, s := range ex.Senders {
		excluded[strings.ToLower(strings.TrimSpace(s))] = true
	}
	bot := map[string]bool{}
	for _, b := range ex.BotAddresses {
		bot[strings.ToLower(strings.TrimSpace(b))] = true
	}
	labels := map[string]bool{}
	for _, f := range ex.Folders {
		labels[strings.TrimSpace(f)] = true
	}
	for _, th := range found {
		t := Thread{Key: th.ID}
		people := map[string]bool{}
		for _, m := range th.Messages {
			sender := address(m.Sender)
			skip := m.ID == "" || automatic(sender) || excluded[sender] || bot[sender] || has(m.LabelIDs, "DRAFT") || has(m.LabelIDs, "SPAM") || has(m.LabelIDs, "TRASH")
			for _, l := range m.LabelIDs {
				skip = skip || labels[l]
			}
			for _, a := range append(append([]string{}, m.ToRecipients...), m.CcRecipients...) {
				skip = skip || bot[address(a)] // a letter to the bot is a conversation with the agent, not with people
			}
			if skip {
				out.Skipped++
				continue
			}
			msg := Msg{ID: m.ID, Author: sender}
			if d, err := time.Parse("2006-01-02", m.Date); err == nil {
				msg.SentAt = &d
			}
			t.Messages = append(t.Messages, msg)
			for _, a := range append(append([]string{m.Sender}, m.ToRecipients...), m.CcRecipients...) {
				if a = address(a); a != "" && !automatic(a) {
					people[a] = true
				}
			}
		}
		if th.ID == "" || len(t.Messages) == 0 {
			continue
		}
		for a := range people {
			t.Participants = append(t.Participants, a)
		}
		sort.Strings(t.Participants)
		out.Threads = append(out.Threads, t)
	}
	return out, nil
}

func has(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
