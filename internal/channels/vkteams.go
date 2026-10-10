package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

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

// Send implements Adapter: HTML; tables as pre blocks (VK-02).
func (v *VKTeams) Send(ctx context.Context, chatID, md string) error {
	c, _, _ := v.get()
	if c == nil {
		return errVKNotConfigured
	}
	for _, part := range render.HTMLParts(render.Parse(md), VKTeamsLimit) {
		m := &botgolang.Message{Chat: botgolang.Chat{ID: chatID}, Text: part, ParseMode: botgolang.ParseModeHTML}
		if err := c.SendTextMessage(m); err != nil {
			metrics.ChannelOutbound.WithLabelValues("vkteams", "error").Inc()
			return v.scrub(err)
		}
		metrics.ChannelOutbound.WithLabelValues("vkteams", "ok").Inc()
	}
	return nil
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
	var atts []uuid.UUID
	for _, part := range pl.Parts {
		if part.Type != botgolang.FILE {
			continue
		}
		rc, name, err := p.Bot.FetchFile(ctx, part.Payload.FileID)
		if err != nil {
			slog.WarnContext(ctx, "vkteams file", "err", err)
			continue
		}
		id, err := p.Attachments.Store(ctx, uid, name, "", io.LimitReader(rc, p.MaxFile), -1)
		rc.Close()
		if err == nil {
			atts = append(atts, id)
		}
		if text == "" {
			text = strings.TrimSpace(part.Payload.Caption)
		}
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
