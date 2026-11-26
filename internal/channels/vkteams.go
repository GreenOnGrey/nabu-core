package channels

// The test cases named in this file are also of FTR.NAB.CMN-0002.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	botgolang "github.com/mail-ru-im/bot-golang"
	"github.com/sirupsen/logrus"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/render"
)

// VKTeamsLimit bounds one message of VK Teams.
const VKTeamsLimit = 4000

// VKTeamsSettings are the settings of the VK Teams channel (tech §2.1).
type VKTeamsSettings struct {
	// APIURL is the bot API of the corporate VK Teams server: https://<server>/bot/v1.
	APIURL string `json:"apiUrl"`
	// BotNick and BotID are read from self/get.
	BotNick string `json:"botNick,omitempty"`
	BotID   string `json:"botId,omitempty"`
}

// ValidateVKTeams normalizes the settings of VK Teams.
func ValidateVKTeams(raw json.RawMessage) (json.RawMessage, error) {
	var s VKTeamsSettings
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, apperr.Unprocessable("invalid_settings", "the settings are not valid").With("field", "settings")
	}
	s.APIURL = strings.TrimRight(strings.TrimSpace(s.APIURL), "/")
	if s.APIURL != "" && !strings.HasPrefix(s.APIURL, "https://") && !strings.HasPrefix(s.APIURL, "http://") {
		return nil, apperr.Unprocessable("invalid_settings", "the API address must be an http(s) URL").With("field", "apiUrl")
	}
	return json.Marshal(s)
}

// VKTeams is the VK Teams bot adapter (R12; tech §8) on mail-ru-im/bot-golang.
type VKTeams struct {
	mu     sync.RWMutex
	client *botgolang.Client
	token  string
	botID  string
	http   *http.Client
}

// quietLogger: the library logs request URLs, which carry the token.
func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	l.SetLevel(logrus.PanicLevel)
	return l
}

// NewVKTeams creates the adapter.
func NewVKTeams() *VKTeams {
	return &VKTeams{http: &http.Client{Timeout: 70 * time.Second}}
}

// Configure sets the API address and the token.
func (v *VKTeams) Configure(apiURL, token, botID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.token, v.botID = token, botID
	if apiURL == "" || token == "" {
		v.client = nil
		return
	}
	v.client = botgolang.NewCustomClient(v.http, apiURL, token, quietLogger())
}

func (v *VKTeams) get() (*botgolang.Client, string, string) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.client, v.token, v.botID
}

func (v *VKTeams) scrub(err error) error {
	_, token, _ := v.get()
	if err == nil || token == "" {
		return err
	}
	return fmt.Errorf("vkteams: %s", strings.ReplaceAll(err.Error(), token, "***"))
}

var errVKNotConfigured = fmt.Errorf("vkteams: the bot is not configured")

// Channel implements Adapter.
func (v *VKTeams) Channel() string { return domain.ChannelVKTeams }

// Me reads the bot (self/get) — the check of the channel.
func (v *VKTeams) Me(context.Context) (*botgolang.BotInfo, error) {
	c, _, _ := v.get()
	if c == nil {
		return nil, errVKNotConfigured
	}
	info, err := c.GetInfo()
	if err != nil {
		return nil, v.scrub(err)
	}
	v.mu.Lock()
	v.botID = info.ID
	v.mu.Unlock()
	return info, nil
}

// Send implements Adapter: HTML; tables as pre blocks (VK-02). A part VK Teams refuses by its format goes as plain
// text: the answer reaches the user whatever markup it carries (FTR.NAB.CHAT-0001 R6).
func (v *VKTeams) Send(ctx context.Context, chatID, md string) error {
	c, _, _ := v.get()
	if c == nil {
		return errVKNotConfigured
	}
	return v.sendParts(ctx, md, func(text string, html bool) error {
		m := &botgolang.Message{Chat: botgolang.Chat{ID: chatID}, Text: text}
		if html {
			m.ParseMode = botgolang.ParseModeHTML
		}
		return c.SendTextMessage(m)
	})
}

// sendParts delivers an answer part by part; send puts one message and returns the error of the messenger. The
// subset of tags of VK Teams is narrower than what the render writes, and an answer with a table or a quote was
// refused as a whole ("Format error"): such a part is sent again as plain text, then in smaller plain chunks. One
// part the messenger refuses does not take the other parts with it.
func (v *VKTeams) sendParts(ctx context.Context, md string, send func(text string, html bool) error) error {
	var partial error
	for _, part := range render.HTMLParts(render.Parse(md), VKTeamsLimit) {
		err := v.sendPart(ctx, part, send)
		if err == nil {
			continue
		}
		var refusal refuser
		if !errors.As(err, &refusal) || !refusal.Permanent() {
			return err // transient: the consumer retries the delivery
		}
		if partial == nil {
			partial = err // the answer is incomplete: the rest of it still goes
		}
	}
	return partial
}

// sendPart sends one part: as HTML, then as plain text, then as smaller plain
// chunks. A part of markup only (a rule, a spacer) has nothing to say and is
// skipped: sending its HTML as plain text is a refusal of the format again.
func (v *VKTeams) sendPart(ctx context.Context, part string, send func(text string, html bool) error) error {
	err := send(vkBreaks(part), true)
	if err == nil {
		outbound("ok")
		return nil
	}
	if !formatRefusal(err) {
		return v.failed(err)
	}
	slog.WarnContext(ctx, "vkteams refused the markup of a part, sending it as text", "err", v.scrub(err))
	outbound("fallback")
	plain := plainText(part)
	if plain == "" {
		slog.InfoContext(ctx, "vkteams: a part carries no text, skipped")
		return nil
	}
	if err = send(plain, false); err == nil {
		outbound("ok")
		return nil
	}
	if !formatRefusal(err) {
		return v.failed(err)
	}
	// The text is refused too. A smaller message may pass, and one bad chunk
	// does not take the rest of the part with it: the answer of a digest stays
	// whole for the user even when one of its lines breaks the format.
	chunks := chunkText(plain, VKTeamsLimit/2)
	slog.ErrorContext(ctx, "vkteams refused a plain part, sending it in chunks",
		"len", utf8.RuneCountInString(plain), "chunks", len(chunks), "err", v.scrub(err))
	failed := 0
	for _, chunk := range chunks {
		cerr := send(chunk, false)
		if cerr == nil {
			outbound("ok")
			continue
		}
		if !formatRefusal(cerr) {
			return v.scrub(cerr) // transient: the consumer retries the delivery
		}
		failed++
		outbound("dropped")
		slog.ErrorContext(ctx, "vkteams refused a chunk of a part, it is lost",
			"len", utf8.RuneCountInString(chunk), "err", v.scrub(cerr))
	}
	if failed == 0 {
		return nil
	}
	if failed == len(chunks) {
		return permanentRefusal{v.scrub(err)}
	}
	return permanentRefusal{fmt.Errorf("vkteams dropped %d of %d chunks of a part: %w", failed, len(chunks), v.scrub(err))}
}

// plainText is the text of an HTML part without markup: control characters
// that break the format of the messenger are dropped, lines stay.
func plainText(part string) string {
	s := strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\t':
			return r
		case unicode.IsControl(r):
			return -1
		}
		return r
	}, render.StripHTML(part))
	return strings.TrimSpace(s)
}

// chunkText cuts plain text into pieces of at most limit runes at a line end,
// else at a space, else anywhere: nothing is added and nothing is dropped.
func chunkText(s string, limit int) []string {
	if limit <= 0 || utf8.RuneCountInString(s) <= limit {
		return []string{s}
	}
	var out []string
	rest := s
	for utf8.RuneCountInString(rest) > limit {
		cut := cutAt(rest, limit)
		if p := strings.TrimRight(rest[:cut], " \t"); p != "" {
			out = append(out, p)
		}
		rest = strings.TrimLeft(rest[cut:], " \t")
	}
	if p := strings.TrimSpace(rest); p != "" {
		out = append(out, p)
	}
	return out
}

// cutAt is the byte index of a cut of at most limit runes: a line end, else a
// space, else the limit itself.
func cutAt(s string, limit int) int {
	i, n, lineEnd, space := 0, 0, -1, -1
	for i < len(s) && n < limit {
		r, size := utf8.DecodeRuneInString(s[i:])
		i += size
		n++
		switch r {
		case '\n':
			lineEnd = i
		case ' ':
			space = i
		}
	}
	switch {
	case lineEnd > 0:
		return lineEnd
	case space > 0:
		return space
	}
	return i
}

// formatRefusal reports the answer of the messenger to markup it cannot read: "error status from API: Format error".
func formatRefusal(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "format error") || strings.Contains(s, "invalid markup")
}

// permanentRefusal is a failure the consumer does not retry: the same message would be refused again, and eight
// attempts hold the partition for minutes.
type permanentRefusal struct{ err error }

func (p permanentRefusal) Error() string   { return p.err.Error() }
func (p permanentRefusal) Unwrap() error   { return p.err }
func (p permanentRefusal) Permanent() bool { return true }

// refuser is a failure the consumer does not retry; HandleOutbound reads it.
type refuser interface{ Permanent() bool }

// outbound counts one delivered, fallen back or dropped piece of an answer.
func outbound(result string) { metrics.ChannelOutbound.WithLabelValues("vkteams", result).Inc() }

// failed counts an attempt that is neither a format refusal nor a delivery:
// the consumer retries it.
func (v *VKTeams) failed(err error) error {
	outbound("error")
	return v.scrub(err)
}

// SendText sends plain text.
func (v *VKTeams) SendText(_ context.Context, chatID, text string) error {
	c, _, _ := v.get()
	if c == nil {
		return errVKNotConfigured
	}
	return v.scrub(c.SendTextMessage(&botgolang.Message{Chat: botgolang.Chat{ID: chatID}, Text: text}))
}

// Typing implements Adapter.
func (v *VKTeams) Typing(_ context.Context, chatID string) {
	if c, _, _ := v.get(); c != nil {
		_ = c.SendChatActions(chatID, botgolang.TypingAction)
	}
}

// Leave leaves a group.
func (v *VKTeams) Leave(_ context.Context, chatID string) error {
	c, _, botID := v.get()
	if c == nil {
		return errVKNotConfigured
	}
	return v.scrub(c.DeleteChatMembers(chatID, []string{botID}))
}

// FetchFile implements Adapter: files/getInfo and the download.
func (v *VKTeams) FetchFile(ctx context.Context, fileID string) (io.ReadCloser, string, error) {
	c, _, _ := v.get()
	if c == nil {
		return nil, "", errVKNotConfigured
	}
	f, err := c.GetFileInfo(fileID)
	if err != nil {
		return nil, "", v.scrub(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.URL, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := v.http.Do(req)
	if err != nil {
		return nil, "", v.scrub(err)
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, "", fmt.Errorf("vkteams file: %d", resp.StatusCode)
	}
	return resp.Body, f.Name, nil
}

// SendFile implements FileSender: messages/sendFile (FTR.NAB.CHAT-0001 R33).
// The library uploads an *os.File under its base name, so the file goes
// through a temporary directory.
func (v *VKTeams) SendFile(ctx context.Context, chatID string, of OutFile) error {
	c, _, _ := v.get()
	if c == nil {
		return errVKNotConfigured
	}
	r, err := of.Open(ctx)
	if err != nil {
		return err
	}
	defer r.Close()
	name := of.Name
	dir, err := os.MkdirTemp("", "nabu-vk-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	f, err := os.Create(filepath.Join(dir, filepath.Base(name)))
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	m := &botgolang.Message{Chat: botgolang.Chat{ID: chatID}}
	m.AttachNewFile(f)
	if err := c.UploadFile(m); err != nil {
		metrics.ChannelOutbound.WithLabelValues("vkteams", "error").Inc()
		return v.scrub(err)
	}
	metrics.ChannelOutbound.WithLabelValues("vkteams", "ok").Inc()
	return nil
}

// Events reads events after lastID with long polling.
func (v *VKTeams) Events(ctx context.Context, lastID, pollSeconds int) ([]*botgolang.Event, error) {
	c, _, _ := v.get()
	if c == nil {
		return nil, errVKNotConfigured
	}
	ev, err := c.GetEventsWithContext(ctx, lastID, pollSeconds)
	return ev, v.scrub(err)
}

// ─── the poller (arch §4) ───────────────────────────────────────────

// UserByEmail finds an active Nabu user by email (VK Teams knows users by email).
type UserByEmail func(ctx context.Context, email string) (uuid.UUID, bool)

// VKPoller reads the events of the bot in one worker instance.
type VKPoller struct {
	Bot         *VKTeams
	Pool        *pgxpool.Pool
	Registry    *Registry
	Users       Users
	ByEmail     UserByEmail
	Inbox       Inbox
	Groups      Groups
	Attachments Attachments
	MaxFile     int64
}

const vkLastEventKey = "vkteams.lastEventId"

func (p *VKPoller) lastID(ctx context.Context) int {
	var raw []byte
	if err := p.Pool.QueryRow(ctx, `SELECT value FROM settings WHERE key = $1`, vkLastEventKey).Scan(&raw); err != nil {
		return 0
	}
	n, _ := strconv.Atoi(strings.Trim(string(raw), `"`))
	return n
}

func (p *VKPoller) setLastID(ctx context.Context, id int) {
	_, _ = p.Pool.Exec(ctx, `INSERT INTO settings (key, value, updated_at) VALUES ($1, to_jsonb($2::int), now())
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`, vkLastEventKey, id)
}

// Run polls under the advisory lock nabu:vkteams; another instance takes
// over within the check interval when this one stops (VK-03). The position
// is kept in the database, so no event is skipped on the takeover.
func (p *VKPoller) Run(ctx context.Context) {
	postgres.RunLocked(ctx, p.Pool, "nabu:vkteams", 15*time.Second, p.poll)
}

func (p *VKPoller) poll(ctx context.Context) {
	last := p.lastID(ctx)
	backoff := time.Second
	for ctx.Err() == nil {
		if !p.Registry.Enabled(ctx, domain.ChannelVKTeams) {
			sleep(ctx, 15*time.Second)
			continue
		}
		evs, err := p.Bot.Events(ctx, last, 30)
		if err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "vkteams events", "err", err)
				p.Registry.SetStatus(ctx, domain.ChannelVKTeams, "error", err.Error())
			}
			sleep(ctx, backoff)
			if backoff < time.Minute {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		metrics.VKTeamsPollLag.Set(0)
		for _, e := range evs {
			p.handle(ctx, e)
			if e.EventID > last {
				last = e.EventID
				p.setLastID(ctx, last)
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (p *VKPoller) say(ctx context.Context, chat, lang, key string, args ...any) {
	_ = p.Bot.SendText(ctx, chat, T(lang, key, args...))
}

func (p *VKPoller) handle(ctx context.Context, e *botgolang.Event) {
	_, _, botID := p.Bot.get()
	pl := e.Payload
	switch e.Type {
	case botgolang.NEW_CHAT_MEMBERS:
		if p.Groups == nil || !containsContact(pl.NewMembers, botID) {
			return
		}
		adder, _ := p.ByEmail(ctx, pl.AddedBy.ID)
		msg, ok := p.Groups.Added(ctx, GroupEvent{Channel: domain.ChannelVKTeams, ChatID: pl.Chat.ID, Title: pl.Chat.Title, AdderID: adder})
		if msg != "" {
			_ = p.Bot.SendText(ctx, pl.Chat.ID, msg)
		}
		if !ok {
			_ = p.Bot.Leave(ctx, pl.Chat.ID)
		}
	case botgolang.LEFT_CHAT_MEMBERS:
		if p.Groups != nil && containsContact(pl.LeftMembers, botID) {
			p.Groups.Removed(ctx, domain.ChannelVKTeams, pl.Chat.ID)
		}
	case botgolang.NEW_MESSAGE:
		metrics.ChannelInbound.WithLabelValues("vkteams", "received").Inc()
		if pl.Chat.Type == botgolang.Group || pl.Chat.Type == botgolang.Channel {
			p.group(ctx, pl, botID)
			return
		}
		p.private(ctx, pl)
	}
}

func containsContact(cs []botgolang.Contact, id string) bool {
	for _, c := range cs {
		if c.ID == id {
			return true
		}
	}
	return false
}

// private: the user is found by the email of the account, no binding (VK-01).
func (p *VKPoller) private(ctx context.Context, pl botgolang.EventPayload) {
	chat := pl.Chat.ID
	uid, ok := p.ByEmail(ctx, pl.From.ID)
	if !ok || !p.Registry.Open(ctx, uid, domain.ChannelVKTeams) {
		metrics.ChannelInbound.WithLabelValues("vkteams", "unavailable").Inc()
		lang := ""
		if ok {
			lang = p.Users.Language(ctx, uid)
		}
		p.say(ctx, chat, lang, "channel.unavailable")
		return
	}
	// the chat of the user: tasks deliver here (R5: not before the first message)
	_, _ = p.Pool.Exec(ctx, `INSERT INTO channel_contacts (channel, user_id, chat_id) VALUES ('vkteams',$1,$2)
		ON CONFLICT (channel, user_id) DO UPDATE SET chat_id = EXCLUDED.chat_id, updated_at = now()`, uid, chat)
	text := strings.TrimSpace(pl.Text)
	caption := ""
	var atts []uuid.UUID
	var files []string // the ids of the parts that came as attachments
	for _, part := range pl.Parts {
		// R21: a voice message is a part of its own type, not a file
		if part.Type != botgolang.FILE && part.Type != botgolang.VOICE {
			continue
		}
		rc, name, err := p.Bot.FetchFile(ctx, part.Payload.FileID)
		if err != nil {
			slog.WarnContext(ctx, "vkteams file", "err", err)
			continue
		}
		mime := ""
		if part.Type == botgolang.VOICE {
			name, mime = voiceFile(name) // the recognizer knows a recording by its type
		}
		id, err := p.Attachments.Store(ctx, uid, name, mime, rc, -1)
		rc.Close()
		switch {
		case err == nil:
			atts = append(atts, id)
			files = append(files, part.Payload.FileID)
		case tooLarge(err):
			p.say(ctx, chat, p.Users.Language(ctx, uid), "tg.too_large")
		default:
			slog.WarnContext(ctx, "vkteams attachment", "err", err)
		}
		if caption == "" {
			caption = strings.TrimSpace(part.Payload.Caption)
		}
	}
	text = stripFileLinks(text, files)
	if text == "" {
		text = caption
	}
	if text == "" && len(atts) == 0 {
		return
	}
	p.Bot.Typing(ctx, chat)
	metrics.ChannelInbound.WithLabelValues("vkteams", "accepted").Inc()
	if err := p.Inbox.Receive(ctx, Inbound{UserID: uid, Channel: domain.ChannelVKTeams, Text: text, Attachments: atts}); err != nil {
		slog.ErrorContext(ctx, "vkteams inbound", "err", err)
		p.say(ctx, chat, p.Users.Language(ctx, uid), "tg.not_delivered")
	}
}

// group: only mentions of the bot and replies to it (tech §8).
func (p *VKPoller) group(ctx context.Context, pl botgolang.EventPayload, botID string) {
	if p.Groups == nil {
		return
	}
	addressed := false
	quote := ""
	for _, part := range pl.Parts {
		switch part.Type {
		case botgolang.MENTION:
			if part.Payload.UserID == botID {
				addressed = true
			}
		case botgolang.REPLY:
			if part.Payload.PartMessage.From.ID == botID {
				addressed = true
			} else {
				quote = part.Payload.PartMessage.Text
			}
		}
	}
	if !addressed {
		return // GR-03
	}
	author, _ := p.ByEmail(ctx, pl.From.ID)
	text := strings.TrimSpace(strings.ReplaceAll(pl.Text, "@["+botID+"]", ""))
	gm := GroupMessage{Channel: domain.ChannelVKTeams, ChatID: pl.Chat.ID, Title: pl.Chat.Title, AuthorID: author,
		AuthorName: strings.TrimSpace(pl.From.FirstName + " " + pl.From.LastName), Text: text, Quote: quote}
	if reply := p.Groups.Message(ctx, gm); reply != "" {
		_ = p.Bot.SendText(ctx, pl.Chat.ID, reply)
		return
	}
	p.Bot.Typing(ctx, pl.Chat.ID)
}

// audioTypes maps the name of a recording to its type: a voice part of VK
// Teams carries no type, and the recognizer knows a recording by the type or
// by the extension of its name (FTR.NAB.CHAT-0001 R21).
var audioTypes = map[string]string{".ogg": "audio/ogg", ".oga": "audio/ogg", ".opus": "audio/ogg",
	".m4a": "audio/mp4", ".aac": "audio/aac", ".mp3": "audio/mpeg", ".wav": "audio/wav", ".amr": "audio/amr", ".webm": "audio/webm"}

// voiceFile names the recording of a voice part: the name the file service
// gave it, or "voice.ogg" when that name says nothing about the type.
func voiceFile(name string) (string, string) {
	if mime, ok := audioTypes[strings.ToLower(filepath.Ext(name))]; ok {
		return name, mime
	}
	return "voice.ogg", "audio/ogg"
}

// urlToken is a link in the text of a message.
var urlToken = regexp.MustCompile(`https?://\S+`)

// stripFileLinks removes the links of the files that came as attachments: VK
// Teams puts them into the text of the message, and the agent opens the link
// instead of the file in its space (FTR.NAB.CHAT-0001 R21).
func stripFileLinks(text string, ids []string) string {
	if len(ids) == 0 || text == "" {
		return text
	}
	out := urlToken.ReplaceAllStringFunc(text, func(tok string) string {
		if fileLink(tok, ids) {
			return ""
		}
		return tok
	})
	if out == text {
		return text
	}
	out = spaceRun.ReplaceAllString(out, " ")
	return strings.TrimSpace(out)
}

// spaceRun is the run of spaces left where a link was taken out of the text.
var spaceRun = regexp.MustCompile(`[ \t]{2,}`)

// fileLink reports a link of one of the files of the message: it carries the
// id of the file, or it is the file service of the messenger.
func fileLink(tok string, ids []string) bool {
	u, err := url.Parse(tok)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	for _, id := range ids {
		if id != "" && strings.Contains(tok, id) {
			return true
		}
	}
	return strings.HasPrefix(u.Hostname(), "files.") && strings.Contains(u.Path, "/get/")
}

// vkBreaks turns the line breaks of the answer into <br>: the HTML of VK Teams
// reads a newline of the source as a space, and the commands of /start came in
// one line (FTR.NAB.CHAT-0001 R6). A paragraph gap becomes two breaks.
func vkBreaks(s string) string { return strings.ReplaceAll(s, "\n", "<br>") }
