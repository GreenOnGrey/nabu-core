package collect

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// VKWorkSpace is the adapter of the official MCP server of VK WorkSpace
// (FTR.NAB.ADM-0001 arch §3.2, tech §4.2): one server gives the messenger
// VK Teams and the mail VK WorkMail. The server has no feed of changes by a
// cursor: the chats are listed by the time of their last message and read
// back to the cursor, the mail is searched by date. The server returns the
// text of messages with their metadata; the adapter keeps the metadata only.
type VKWorkSpace struct {
	// Overlap of VK Teams (MEMORY_VK_OVERLAP); mail overlaps by a day at least.
	Overlap time.Duration
	// MailOverlap is MEMORY_GMAIL_OVERLAP, at least a day.
	MailOverlap time.Duration
	// ChatLimit is how many messages of one chat a run reads (MEMORY_VK_CHAT_LIMIT, R10).
	ChatLimit int
}

const (
	vkTeamsSource = "vkteams"
	vkMailSource  = "vkmail"

	vkListChats   = "messenger-list-chats"
	vkListMsgs    = "messenger-list-messages"
	vkCurrentUser = "messenger-get-current-user"
	vkMailFolders = "mail-list-folders"
	vkMailSearch  = "mail-search"
	vkMailThread  = "mail-read-thread"

	vkChatsPage   = 100
	vkMsgsPage    = 200
	vkMailPage    = 50
	vkMailPages   = 40
	vkChatRetries = 1 // a restart of the listing when the cursor of a page is refused; the Caller retries every call itself
	vkMailWeb     = "https://e.mail.ru"
)

// vkMoscow is the zone of the dates of mail-search.
var vkMoscow = time.FixedZone("MSK", 3*3600)

func init() {
	Register(&VKWorkSpace{Overlap: time.Hour, MailOverlap: 24 * time.Hour, ChatLimit: 1000})
}

// SetVKSettings sets the overlap of VK Teams and the limit of a chat from the configuration.
func SetVKSettings(overlap time.Duration, chatLimit int) {
	if a, ok := registry["vkworkspace"].(*VKWorkSpace); ok {
		if overlap > 0 {
			a.Overlap = overlap
		}
		if chatLimit > 0 {
			a.ChatLimit = chatLimit
		}
	}
}

// Kind is "vkworkspace".
func (v *VKWorkSpace) Kind() string { return "vkworkspace" }

// Sources: the messenger and the mail.
func (v *VKWorkSpace) Sources() []Source {
	return []Source{{Key: vkTeamsSource, Title: "VK Teams"}, {Key: vkMailSource, Title: "Почта VK WorkMail"}}
}

// ReadTools: the messages of a chat or a discussion, the thread of mail.
func (v *VKWorkSpace) ReadTools() []string { return []string{vkListMsgs, vkMailThread} }

// vkCursor is the cursor of messenger-list-messages that starts before a
// message: the server pages back from {"from_msg_id": N} (tech decision 3).
func vkCursor(msgID string) string {
	for _, c := range msgID {
		if c < '0' || c > '9' {
			return ""
		}
	}
	if msgID == "" {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(`{"from_msg_id":` + msgID + `}`))
}

// ReadCall reads a chat day from its end; a mail thread whole.
func (v *VKWorkSpace) ReadCall(source string, t Thread) ToolCall {
	if source == vkMailSource {
		return ToolCall{Tool: vkMailThread, Args: map[string]any{"id": t.Key, "limit": 100}}
	}
	args := map[string]any{"chat_sn": t.Key, "limit": vkMsgsPage}
	if c := vkCursor(t.ReadFrom); c != "" {
		args["cursor"] = c
	}
	return ToolCall{Tool: vkListMsgs, Args: args}
}

// ReadPartCalls read the discussions of a chat day.
func (v *VKWorkSpace) ReadPartCalls(source string, t Thread) []ToolCall {
	if source != vkTeamsSource {
		return nil
	}
	out := make([]ToolCall, 0, len(t.Parts))
	for _, p := range t.Parts {
		out = append(out, ToolCall{Tool: vkListMsgs, Args: map[string]any{"chat_sn": p, "limit": vkMsgsPage}})
	}
	return out
}

// ThreadOf names the chat or the mail thread of a read call.
func (v *VKWorkSpace) ThreadOf(tool string, args json.RawMessage) (string, bool) {
	var a struct {
		ChatSN string `json:"chat_sn"`
		ID     string `json:"id"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return "", false
	}
	switch tool {
	case vkListMsgs:
		return a.ChatSN, a.ChatSN != ""
	case vkMailThread:
		return a.ID, a.ID != ""
	}
	return "", false
}

// Link: a message of VK Teams carries its link (Msg.URL); a letter opens by
// its folder and id: https://e.mail.ru/<folder>/0:<id>:<folder>/ (R12).
func (v *VKWorkSpace) Link(source string, t Thread, messageID string) string {
	if source != vkMailSource {
		return ""
	}
	i := strings.LastIndex(t.Key, ":")
	if i < 0 || messageID == "" {
		return ""
	}
	f := t.Key[i+1:]
	return fmt.Sprintf("%s/%s/0:%s:%s/", vkMailWeb, f, messageID, f)
}

// TimePrecision: both give the time of a message.
func (v *VKWorkSpace) TimePrecision(string) string { return "minute" }

func vkSourceTools(source string) []string {
	if source == vkMailSource {
		return []string{vkMailFolders, vkMailSearch, vkMailThread}
	}
	return []string{vkListChats, vkListMsgs, vkCurrentUser}
}

// CheckSourceTools checks the tools of one source.
func (v *VKWorkSpace) CheckSourceTools(source string, tools []string) error {
	return missing(tools, vkSourceTools(source))
}

// CheckTools fails when neither source has its tools; a source without its
// own is not offered (CheckSourceTools).
func (v *VKWorkSpace) CheckTools(tools []string) error {
	errTeams := v.CheckSourceTools(vkTeamsSource, tools)
	errMail := v.CheckSourceTools(vkMailSource, tools)
	if errTeams != nil && errMail != nil {
		return errTeams
	}
	return nil
}

// Changes lists what changed in the source since the cursor.
func (v *VKWorkSpace) Changes(ctx context.Context, call Caller, source, cursor string, since, now time.Time, ex Exclusions) (ChangeSet, error) {
	if source == vkMailSource {
		return v.mailChanges(ctx, call, cursor, since, now, ex)
	}
	return v.teamsChanges(ctx, call, cursor, since, now, ex)
}

// ─── VK Teams ───────────────────────────────────────────────────────

type vkChat struct {
	ChatSN        string  `json:"chat_sn"`
	Name          string  `json:"name"`
	Type          string  `json:"type"`
	LastMessageAt *string `json:"last_message_at"`
	Notifications *bool   `json:"notifications_enabled"`
}

// muted: a group without the field is not muted.
func (c vkChat) muted() bool { return c.Notifications != nil && !*c.Notifications }

type vkMessage struct {
	TeamsID  string   `json:"teams_id"`
	SenderID string   `json:"sender_id"`
	Mentions []string `json:"mentions"`
	TimeSent string   `json:"time_sent"`
	URL      string   `json:"url"`
	ThreadID string   `json:"thread_id"`
	sent     time.Time
}

// vkHuman: people come as an e-mail address, bots as a number (R7, tech decision 4).
func vkHuman(id string) bool { return strings.Contains(id, "@") }

func vkTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}

func vkStart(cursor string, overlap time.Duration, since time.Time) (time.Time, error) {
	if cursor == "" {
		return since, nil
	}
	t, err := time.Parse(time.RFC3339, cursor)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid VK cursor %q", cursor)
	}
	return t.Add(-overlap), nil
}

// chats lists the chats with a message after from; a failed page with a
// cursor restarts the listing (the cursor is bound to a snapshot).
func (v *VKWorkSpace) chats(ctx context.Context, call Caller, from time.Time) ([]vkChat, error) {
	var lastErr error
	for attempt := 0; attempt <= vkChatRetries; attempt++ {
		var out []vkChat
		next := ""
		failed := false
		for page := 0; page < 50; page++ {
			args := map[string]any{"include_types": []string{"private", "group"}, "limit": vkChatsPage}
			if next != "" {
				args["cursor"] = next
			}
			raw, err := call(ctx, vkListChats, args)
			if err != nil {
				if next == "" || errors.Is(err, ErrAccess) || errors.Is(err, ErrRateLimited) {
					return nil, err
				}
				lastErr, failed = err, true
				break
			}
			var res struct {
				Chats      []vkChat `json:"chats"`
				NextCursor *string  `json:"next_cursor"`
			}
			if err := json.Unmarshal(raw, &res); err != nil {
				return nil, fmt.Errorf("vk %s: unexpected answer: %w", vkListChats, err)
			}
			older := len(res.Chats) == 0
			for i, c := range res.Chats {
				t, ok := vkTime(deref(c.LastMessageAt))
				if ok && !t.Before(from) {
					out = append(out, c)
				}
				if i == len(res.Chats)-1 {
					older = !ok || t.Before(from) // the list goes from the latest chat: the rest is older
				}
			}
			if older || res.NextCursor == nil || *res.NextCursor == "" {
				return out, nil
			}
			next = *res.NextCursor
		}
		if !failed {
			return out, nil
		}
	}
	return nil, lastErr
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// messages reads a chat or a discussion back to from, at most limit
// messages; full — it stopped at the limit.
func (v *VKWorkSpace) messages(ctx context.Context, call Caller, chatSN string, from time.Time, limit int) (msgs []vkMessage, full bool, err error) {
	next := ""
	for len(msgs) < limit {
		args := map[string]any{"chat_sn": chatSN, "limit": vkMsgsPage}
		if next != "" {
			args["cursor"] = next
		}
		raw, err := call(ctx, vkListMsgs, args)
		if err != nil {
			return nil, false, err
		}
		var res struct {
			Messages   []vkMessage `json:"messages"`
			NextCursor *string     `json:"next_cursor"`
		}
		if err := json.Unmarshal(raw, &res); err != nil {
			return nil, false, fmt.Errorf("vk %s: unexpected answer: %w", vkListMsgs, err)
		}
		reached := len(res.Messages) == 0
		// a page goes from the oldest message to the latest
		for i := len(res.Messages) - 1; i >= 0; i-- {
			m := res.Messages[i]
			t, ok := vkTime(m.TimeSent)
			if !ok || m.TeamsID == "" {
				continue
			}
			if t.Before(from) {
				reached = true
				break
			}
			if len(msgs) == limit {
				return msgs, true, nil
			}
			m.sent = t.UTC()
			msgs = append(msgs, m)
		}
		if reached || res.NextCursor == nil || *res.NextCursor == "" {
			return msgs, false, nil
		}
		next = *res.NextCursor
	}
	return msgs, true, nil
}

func (v *VKWorkSpace) teamsChanges(ctx context.Context, call Caller, cursor string, since, now time.Time, ex Exclusions) (ChangeSet, error) {
	from, err := vkStart(cursor, v.Overlap, since)
	if err != nil {
		return ChangeSet{}, err
	}
	loc := ex.Location
	if loc == nil {
		loc = time.UTC
	}
	raw, err := call(ctx, vkCurrentUser, map[string]any{})
	if err != nil {
		return ChangeSet{}, err
	}
	var me struct {
		Email string `json:"email"`
	}
	if err := json.Unmarshal(raw, &me); err != nil || me.Email == "" {
		return ChangeSet{}, fmt.Errorf("vk %s: no address of the user", vkCurrentUser)
	}
	self := strings.ToLower(me.Email)
	chats, err := v.chats(ctx, call, from)
	if err != nil {
		return ChangeSet{}, err
	}
	lower := func(list []string) map[string]bool {
		m := map[string]bool{}
		for _, x := range list {
			m[strings.ToLower(strings.TrimSpace(x))] = true
		}
		return m
	}
	excludedChats, includeMuted, excludedSenders, bot := lower(ex.Chats), lower(ex.IncludeMuted), lower(ex.Senders), lower(ex.BotAddresses)
	out := ChangeSet{Cursor: now.UTC().Format(time.RFC3339)}
	limit := v.ChatLimit
	if limit <= 0 {
		limit = 1000
	}
	for _, c := range chats {
		sn := strings.ToLower(c.ChatSN)
		switch {
		case excludedChats[sn]:
			continue
		case c.Type == "private" && (!vkHuman(c.ChatSN) || excludedSenders[sn] || bot[sn]): // R7, R4
			continue
		case c.Type == "group" && c.muted() && !includeMuted[sn]: // R8
			continue
		case c.Type != "private" && c.Type != "group": // channels (R6)
			continue
		}
		msgs, full, err := v.messages(ctx, call, c.ChatSN, from, limit)
		if err != nil {
			return ChangeSet{}, err
		}
		read := len(msgs)
		// the discussions of the messages read (R9): their messages belong to the day of the message that began them
		type part struct {
			key  string
			day  string
			msgs []vkMessage
		}
		var parts []part
		for _, m := range msgs {
			if m.ThreadID == "" || m.ThreadID == c.ChatSN || read >= limit {
				continue
			}
			tm, tfull, err := v.messages(ctx, call, m.ThreadID, from, limit-read)
			if err != nil {
				return ChangeSet{}, err
			}
			read += len(tm)
			full = full || tfull
			if len(tm) > 0 {
				parts = append(parts, part{key: m.ThreadID, day: m.sent.In(loc).Format("2006-01-02"), msgs: tm})
			}
		}
		if full {
			if out.Truncated == nil {
				out.Truncated = map[string]int{}
			}
			name := c.Name
			if _, taken := out.Truncated[name]; name == "" || taken {
				name = strings.TrimSpace(name + " " + c.ChatSN) // two chats of one name stay two
			}
			out.Truncated[name] = read
		}
		days := map[string]*Thread{}
		qualifies := map[string]bool{}
		people := map[string]map[string]bool{}
		var order []string
		day := func(d string) *Thread {
			t := days[d]
			if t == nil {
				t = &Thread{Key: c.ChatSN, DayKey: d}
				days[d] = t
				people[d] = map[string]bool{}
				order = append(order, d)
			}
			return t
		}
		add := func(d string, m vkMessage) {
			sender := strings.ToLower(m.SenderID)
			if sender == self || slicesHasFold(m.Mentions, self) {
				qualifies[d] = true
			}
			if !vkHuman(m.SenderID) || excludedSenders[sender] || bot[sender] {
				out.Skipped++
				return
			}
			t := day(d)
			sent := m.sent
			t.Messages = append(t.Messages, Msg{ID: m.TeamsID, Author: sender, SentAt: &sent, URL: m.URL})
			people[d][sender] = true
		}
		for _, m := range msgs {
			add(m.sent.In(loc).Format("2006-01-02"), m)
		}
		for _, p := range parts {
			before := len(day(p.day).Messages)
			for _, m := range p.msgs {
				add(p.day, m)
			}
			if len(days[p.day].Messages) > before {
				days[p.day].Parts = append(days[p.day].Parts, p.key)
			}
		}
		// the message after a day starts the reading of the day (tech §4.2)
		sort.Strings(order)
		firstOf := map[string]string{}
		for _, m := range msgs {
			d := m.sent.In(loc).Format("2006-01-02")
			if cur, ok := firstOf[d]; !ok || lessID(m.TeamsID, cur) {
				firstOf[d] = m.TeamsID
			}
		}
		for i, d := range order {
			t := days[d]
			if len(t.Messages) == 0 || (c.Type == "group" && !qualifies[d]) {
				continue // R6: a day of a group only when the user wrote or was named
			}
			for j := i + 1; j < len(order); j++ {
				if id, ok := firstOf[order[j]]; ok {
					t.ReadFrom = id
					break
				}
			}
			if c.Type == "private" {
				people[d][sn] = true
			}
			for a := range people[d] {
				t.Participants = append(t.Participants, a)
			}
			sort.Strings(t.Participants)
			sort.Slice(t.Messages, func(a, b int) bool { return t.Messages[a].SentAt.Before(*t.Messages[b].SentAt) })
			out.Threads = append(out.Threads, *t)
		}
	}
	return out, nil
}

// lessID compares numeric ids of VK Teams.
func lessID(a, b string) bool {
	if len(a) != len(b) {
		return len(a) < len(b)
	}
	return a < b
}

func slicesHasFold(list []string, v string) bool {
	for _, x := range list {
		if strings.EqualFold(strings.TrimSpace(x), v) {
			return true
		}
	}
	return false
}

// ─── VK WorkMail ────────────────────────────────────────────────────

type vkFolder struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Parent string `json:"parent"`
	Type   string `json:"type"`
}

type vkAddr struct {
	Email string `json:"email"`
}

type vkLetter struct {
	ID            string `json:"id"`
	ThreadID      string `json:"thread_id"`
	Date          int64  `json:"date"`
	Correspondent struct {
		From []vkAddr `json:"from"`
		To   []vkAddr `json:"to"`
		Cc   []vkAddr `json:"cc"`
	} `json:"correspondents"`
	Flags struct {
		Newsletter bool `json:"newsletter"`
	} `json:"flags"`
}

// mailFolders are the own folders of the mailbox (R11): without spam,
// drafts, trash and the excluded ones, and without a shared mailbox — a
// folder of the top level with an inbox of its own (tech decision 5).
func (v *VKWorkSpace) mailFolders(ctx context.Context, call Caller, ex Exclusions) ([]string, error) {
	raw, err := call(ctx, vkMailFolders, map[string]any{})
	if err != nil {
		return nil, err
	}
	var res struct {
		Folders []vkFolder `json:"folders"`
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("vk %s: unexpected answer: %w", vkMailFolders, err)
	}
	byID := map[string]vkFolder{}
	for _, f := range res.Folders {
		byID[f.ID] = f
	}
	top := func(f vkFolder) string {
		for i := 0; i < 20 && f.Parent != "" && f.Parent != "-1"; i++ {
			p, ok := byID[f.Parent]
			if !ok {
				break
			}
			f = p
		}
		return f.ID
	}
	shared := map[string]bool{}
	for _, f := range res.Folders {
		if f.Type == "inbox" && f.Parent != "" && f.Parent != "-1" {
			shared[top(f)] = true
		}
	}
	excluded := map[string]bool{}
	for _, x := range ex.Folders {
		excluded[strings.ToLower(strings.TrimSpace(x))] = true
	}
	var out []string
	for _, f := range res.Folders {
		switch {
		case shared[top(f)]:
		case f.Type == "spam" || f.Type == "drafts" || f.Type == "trash":
		case excluded[strings.ToLower(f.ID)] || excluded[strings.ToLower(f.Name)]:
		default:
			out = append(out, f.ID)
		}
	}
	return out, nil
}

func (v *VKWorkSpace) mailChanges(ctx context.Context, call Caller, cursor string, since, now time.Time, ex Exclusions) (ChangeSet, error) {
	from, err := vkStart(cursor, max(v.MailOverlap, 24*time.Hour), since)
	if err != nil {
		return ChangeSet{}, err
	}
	folders, err := v.mailFolders(ctx, call, ex)
	if err != nil {
		return ChangeSet{}, err
	}
	out := ChangeSet{Cursor: now.UTC().Format(time.RFC3339)}
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
	for _, folder := range folders {
		for page := 0; page < vkMailPages; page++ {
			raw, err := call(ctx, vkMailSearch, map[string]any{"folder": folder, "date_from": from.In(vkMoscow).Format("2006-01-02"),
				"limit": vkMailPage, "offset": page * vkMailPage})
			if err != nil {
				return ChangeSet{}, err
			}
			var res struct {
				Count    int        `json:"count"`
				Messages []vkLetter `json:"messages"`
			}
			if err := json.Unmarshal(raw, &res); err != nil {
				return ChangeSet{}, fmt.Errorf("vk %s: unexpected answer: %w", vkMailSearch, err)
			}
			for _, l := range res.Messages {
				if l.ID == "" || l.ThreadID == "" || seen[l.ID] || l.Date < from.Unix() {
					continue
				}
				seen[l.ID] = true
				sender := ""
				if len(l.Correspondent.From) > 0 {
					sender = strings.ToLower(l.Correspondent.From[0].Email)
				}
				rcpts := append(append([]vkAddr{}, l.Correspondent.To...), l.Correspondent.Cc...)
				skip := l.Flags.Newsletter || automatic(sender) || excluded[sender] || bot[sender]
				for _, a := range rcpts {
					skip = skip || bot[strings.ToLower(a.Email)]
				}
				if skip {
					out.Skipped++
					continue
				}
				t := threads[l.ThreadID]
				if t == nil {
					t = &Thread{Key: l.ThreadID}
					threads[l.ThreadID] = t
					people[l.ThreadID] = map[string]bool{}
					order = append(order, l.ThreadID)
				}
				sent := time.Unix(l.Date, 0).UTC()
				// the letter opens in the folder it was found in: a reply of a thread lies in Sent (R12)
				t.Messages = append(t.Messages, Msg{ID: l.ID, Author: sender, SentAt: &sent, URL: fmt.Sprintf("%s/%s/0:%s:%s/", vkMailWeb, folder, l.ID, folder)})
				for _, a := range append([]vkAddr{{Email: sender}}, rcpts...) {
					if e := strings.ToLower(a.Email); e != "" && !automatic(e) {
						people[l.ThreadID][e] = true
					}
				}
			}
			if len(res.Messages) < vkMailPage || (page+1)*vkMailPage >= res.Count {
				break
			}
		}
	}
	for _, key := range order {
		t := threads[key]
		for a := range people[key] {
			t.Participants = append(t.Participants, a)
		}
		sort.Strings(t.Participants)
		sort.Slice(t.Messages, func(a, b int) bool { return t.Messages[a].SentAt.Before(*t.Messages[b].SentAt) })
		out.Threads = append(out.Threads, *t)
	}
	return out, nil
}
