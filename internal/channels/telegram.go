package channels

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/render"
)

// TelegramLimit is the length limit of a plain Telegram message.
const TelegramLimit = 4096

// TelegramSettings are the settings of the Telegram channel (tech §2.1).
type TelegramSettings struct {
	// Username is read from getMe.
	Username string `json:"username,omitempty"`
}

// Telegram is the Telegram bot adapter (R13–R14; tech §7). The token and
// the webhook secret come from the channel secrets and may change at runtime.
type Telegram struct {
	APIURL string // https://api.telegram.org
	// TableMaxCols: wider tables go as a code block (TG_TABLE_MAX_COLS).
	TableMaxCols int

	mu     sync.RWMutex
	token  string
	secret string
	name   string
	http   *http.Client
}

// NewTelegram creates the adapter.
func NewTelegram(apiURL, token, secret string) *Telegram {
	return &Telegram{APIURL: strings.TrimRight(apiURL, "/"), token: token, secret: secret, TableMaxCols: 8,
		http: &http.Client{Timeout: 30 * time.Second}}
}

// Configure replaces the token and the webhook secret.
func (t *Telegram) Configure(token, secret string) {
	t.mu.Lock()
	t.token, t.secret = token, secret
	t.mu.Unlock()
}

// Configured reports whether the bot has a token.
func (t *Telegram) Configured() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.token != ""
}

func (t *Telegram) creds() (string, string) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.token, t.secret
}

// Channel implements Adapter.
func (t *Telegram) Channel() string { return domain.ChannelTelegram }

func (t *Telegram) call(ctx context.Context, method string, in any, out any) error {
	token, _ := t.creds()
	if token == "" {
		return &TelegramError{Status: http.StatusServiceUnavailable, Description: "the bot is not configured"}
	}
	b, _ := json.Marshal(in)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.APIURL+"/bot"+token+"/"+method, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.http.Do(req)
	if err != nil {
		return fmt.Errorf("telegram %s: %w", method, scrub(err, token))
	}
	defer resp.Body.Close()
	var r struct {
		OK          bool            `json:"ok"`
		Description string          `json:"description"`
		Result      json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&r); err != nil {
		return &TelegramError{Status: resp.StatusCode, Description: "unreadable answer"}
	}
	if !r.OK {
		return &TelegramError{Status: resp.StatusCode, Description: r.Description}
	}
	if out != nil {
		return json.Unmarshal(r.Result, out)
	}
	return nil
}

// TelegramError is an error answer of the Bot API.
type TelegramError struct {
	Status      int
	Description string
}

func (e *TelegramError) Error() string {
	return fmt.Sprintf("telegram %d: %s", e.Status, e.Description)
}

// Permanent reports a refusal a repeated call does not change (a blocked
// bot, a wrong chat); 429 and 5xx are worth a retry.
func (e *TelegramError) Permanent() bool {
	return e.Status >= 400 && e.Status < 500 && e.Status != http.StatusTooManyRequests
}

func scrub(err error, token string) error {
	if token == "" {
		return err
	}
	return fmt.Errorf("%s", strings.ReplaceAll(err.Error(), token, "***"))
}

// Me reads the bot (getMe) — the check of the channel.
func (t *Telegram) Me(ctx context.Context) (int64, string, error) {
	var me struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
	}
	if err := t.call(ctx, "getMe", map[string]any{}, &me); err != nil {
		return 0, "", err
	}
	t.mu.Lock()
	t.name = me.Username
	t.mu.Unlock()
	return me.ID, me.Username, nil
}

// Setup reads the bot and registers the webhook (tech §7): messages and the
// changes of the bot's membership in groups.
func (t *Telegram) Setup(ctx context.Context, publicAPIURL string) error {
	if _, _, err := t.Me(ctx); err != nil {
		return err
	}
	_, secret := t.creds()
	return t.call(ctx, "setWebhook", map[string]any{"url": publicAPIURL + "/hooks/v1/telegram/" + secret, "secret_token": secret,
		"allowed_updates": []string{"message", "my_chat_member"}}, nil)
}

// Username is the bot's username after Setup.
func (t *Telegram) Username() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.name
}

func isRefusal(err error) bool {
	te, ok := err.(*TelegramError)
	return ok && (te.Status == http.StatusBadRequest || te.Status == http.StatusNotFound)
}

// Send implements Adapter (R14): rich messages built from the blocks of the
// answer; when the rich method refuses (400, 404), HTML messages; a part
// Telegram cannot parse goes as plain text.
func (t *Telegram) Send(ctx context.Context, chatID, md string) error {
	blocks := render.Parse(md)
	if len(blocks) == 0 {
		return nil
	}
	sent, err := t.sendRich(ctx, chatID, blocks)
	if err == nil {
		return nil
	}
	if !isRefusal(err) {
		return err
	}
	slog.InfoContext(ctx, "telegram rich message refused, sending HTML", "err", err)
	metrics.ChannelOutbound.WithLabelValues("telegram", "fallback").Inc()
	return t.sendHTML(ctx, chatID, blocks[sent:]) // the delivered messages are not repeated
}

// sendRich returns the number of blocks of the answer it delivered.
func (t *Telegram) sendRich(ctx context.Context, chatID string, blocks []render.Block) (int, error) {
	sent := 0
	for _, msg := range render.TelegramRich(blocks, t.TableMaxCols) {
		if err := t.call(ctx, "sendRichMessage", map[string]any{"chat_id": chatID,
			"rich_message": map[string]any{"blocks": msg, "skip_entity_detection": false}}, nil); err != nil {
			return sent, err
		}
		sent += len(msg) // one rich block per block of the answer
		metrics.ChannelOutbound.WithLabelValues("telegram", "ok").Inc()
	}
	return sent, nil
}

func (t *Telegram) sendHTML(ctx context.Context, chatID string, blocks []render.Block) error {
	for _, part := range render.HTMLParts(blocks, TelegramLimit) {
		err := t.call(ctx, "sendMessage", map[string]any{"chat_id": chatID, "text": part, "parse_mode": "HTML",
			"link_preview_options": map[string]bool{"is_disabled": true}}, nil)
		if err != nil && isRefusal(err) {
			err = t.SendText(ctx, chatID, render.StripHTML(part))
		}
		if err != nil {
			metrics.ChannelOutbound.WithLabelValues("telegram", "error").Inc()
			return err
		}
		metrics.ChannelOutbound.WithLabelValues("telegram", "ok").Inc()
	}
	return nil
}

// SendText sends plain text (service messages of the bot).
func (t *Telegram) SendText(ctx context.Context, chatID, text string) error {
	for _, p := range splitRunes(text, TelegramLimit) {
		if err := t.call(ctx, "sendMessage", map[string]any{"chat_id": chatID, "text": p}, nil); err != nil {
			return err
		}
	}
	return nil
}

func splitRunes(s string, n int) []string {
	r := []rune(s)
	var out []string
	for len(r) > n {
		out = append(out, string(r[:n]))
		r = r[n:]
	}
	return append(out, string(r))
}

// Draft implements Drafter: the streamed answer of a private chat
// (sendMessageDraft; a draft lives 30 seconds, the final message follows).
func (t *Telegram) Draft(ctx context.Context, chatID string, draftID int64, text string) error {
	if utf8.RuneCountInString(text) > TelegramLimit {
		r := []rune(text)
		text = "…" + string(r[len(r)-TelegramLimit+1:])
	}
	return t.call(ctx, "sendMessageDraft", map[string]any{"chat_id": chatID, "draft_id": draftID, "text": text}, nil)
}

// Typing implements Adapter.
func (t *Telegram) Typing(ctx context.Context, chatID string) {
	_ = t.call(ctx, "sendChatAction", map[string]any{"chat_id": chatID, "action": "typing"}, nil)
}

// Leave leaves a group.
func (t *Telegram) Leave(ctx context.Context, chatID string) error {
	return t.call(ctx, "leaveChat", map[string]any{"chat_id": chatID}, nil)
}

// MemberCount counts the members of a group.
func (t *Telegram) MemberCount(ctx context.Context, chatID string) int {
	var n int
	_ = t.call(ctx, "getChatMemberCount", map[string]any{"chat_id": chatID}, &n)
	return n
}

// FetchFile implements Adapter: getFile and the download.
func (t *Telegram) FetchFile(ctx context.Context, fileID string) (io.ReadCloser, string, error) {
	var f struct {
		FilePath string `json:"file_path"`
	}
	if err := t.call(ctx, "getFile", map[string]any{"file_id": fileID}, &f); err != nil {
		return nil, "", err
	}
	token, _ := t.creds()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.APIURL+"/file/bot"+token+"/"+f.FilePath, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return nil, "", scrub(err, token)
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, "", fmt.Errorf("telegram file: %d", resp.StatusCode)
	}
	return resp.Body, f.FilePath, nil
}

// ─── updates ────────────────────────────────────────────────────────

// TGUser is a Telegram account.
type TGUser struct {
	ID           int64  `json:"id"`
	IsBot        bool   `json:"is_bot"`
	Username     string `json:"username"`
	First        string `json:"first_name"`
	Last         string `json:"last_name"`
	LanguageCode string `json:"language_code"`
}

// TGChat is a chat.
type TGChat struct {
	ID    int64  `json:"id"`
	Type  string `json:"type"` // private | group | supergroup | channel
	Title string `json:"title"`
}

// TGEntity is a special entity of a text: offsets in UTF-16 units.
type TGEntity struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
	Length int    `json:"length"`
}

// TGMessage is the part of a message Nabu reads.
type TGMessage struct {
	MessageID int64      `json:"message_id"`
	From      *TGUser    `json:"from"`
	Chat      TGChat     `json:"chat"`
	Text      string     `json:"text"`
	Caption   string     `json:"caption"`
	Entities  []TGEntity `json:"entities"`
	// CaptionEntities are the entities of the caption of a media message.
	CaptionEntities []TGEntity `json:"caption_entities"`
	ReplyTo         *TGMessage `json:"reply_to_message"`
	Document        *struct {
		FileID   string `json:"file_id"`
		FileName string `json:"file_name"`
		MimeType string `json:"mime_type"`
		FileSize int64  `json:"file_size"`
	} `json:"document"`
	Photo []struct {
		FileID   string `json:"file_id"`
		FileSize int64  `json:"file_size"`
	} `json:"photo"`
	Voice *struct {
		FileID   string `json:"file_id"`
		MimeType string `json:"mime_type"`
	} `json:"voice"`
}

// Update is the part of a Telegram update Nabu reads.
type Update struct {
	Message      *TGMessage `json:"message"`
	MyChatMember *struct {
		Chat          TGChat `json:"chat"`
		From          TGUser `json:"from"`
		OldChatMember struct {
			Status string `json:"status"`
		} `json:"old_chat_member"`
		NewChatMember struct {
			Status string `json:"status"`
		} `json:"new_chat_member"`
	} `json:"my_chat_member"`
}

// Mentions reports whether a group message addresses the bot: a mention of
// its username or a reply to its message (tech §7).
func (m *TGMessage) Mentions(botUsername string, botID int64) bool {
	if m.ReplyTo != nil && m.ReplyTo.From != nil && m.ReplyTo.From.ID == botID {
		return true
	}
	text, entities := m.Text, m.Entities
	if text == "" {
		text, entities = m.Caption, m.CaptionEntities
	}
	u16 := utf16Units(text)
	for _, e := range entities {
		if e.Type != "mention" || e.Offset < 0 || e.Offset+e.Length > len(u16) {
			continue
		}
		if strings.EqualFold(fromUTF16(u16[e.Offset:e.Offset+e.Length]), "@"+botUsername) {
			return true
		}
	}
	return false
}

// ─── the webhook ────────────────────────────────────────────────────

// Users is what the channels need to know of users.
type Users interface {
	Language(ctx context.Context, uid uuid.UUID) string
	Status(ctx context.Context, uid uuid.UUID) string
}

// Webhook handles updates of the bot.
type Webhook struct {
	Bot         *Telegram
	Keys        *Keys
	Registry    *Registry
	Users       Users
	Inbox       Inbox
	Groups      Groups
	Attachments Attachments
	MaxFile     int64
	WebURL      string
	botID       atomic.Int64
}

// Route mounts POST /hooks/v1/telegram/{secret}.
func (h *Webhook) Route(r chi.Router) {
	r.Post("/hooks/v1/telegram/{secret}", h.serve)
}

func (h *Webhook) serve(w http.ResponseWriter, r *http.Request) {
	_, secret := h.Bot.creds()
	// CH-03: without the secret of the path and of the header — 401.
	if secret == "" || subtle.ConstantTimeCompare([]byte(chi.URLParam(r, "secret")), []byte(secret)) != 1 ||
		subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Telegram-Bot-Api-Secret-Token")), []byte(secret)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var u Update
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&u); err != nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	// Answer Telegram at once; the agent's answer comes as a new message.
	w.WriteHeader(http.StatusOK)
	ctx := context.WithoutCancel(r.Context())
	go h.Handle(ctx, u)
}

func (h *Webhook) say(ctx context.Context, chat string, lang, key string, args ...any) {
	_ = h.Bot.SendText(ctx, chat, T(lang, key, args...))
}

// Handle processes one update.
func (h *Webhook) Handle(ctx context.Context, u Update) {
	if !h.Registry.Enabled(ctx, domain.ChannelTelegram) {
		return
	}
	if m := u.MyChatMember; m != nil {
		h.membership(ctx, m.Chat, m.From, m.OldChatMember.Status, m.NewChatMember.Status)
		return
	}
	m := u.Message
	if m == nil || m.From == nil || m.From.IsBot {
		return
	}
	metrics.ChannelInbound.WithLabelValues("telegram", "received").Inc()
	if m.Chat.Type != "private" {
		h.group(ctx, m)
		return
	}
	h.private(ctx, m)
}

func (h *Webhook) private(ctx context.Context, m *TGMessage) {
	chat := strconv.FormatInt(m.Chat.ID, 10)
	text := strings.TrimSpace(m.Text)
	lang := m.From.LanguageCode
	uid, bound, err := h.Keys.UserOf(ctx, m.From.ID)
	if err != nil {
		slog.ErrorContext(ctx, "telegram binding lookup", "err", err)
		return
	}
	if !bound {
		// R13: the bot asks for the key; a message like a key is checked.
		if !LooksLikeKey(text) {
			h.say(ctx, chat, lang, "tg.send_key", h.WebURL+"/connections")
			return
		}
		res, err := h.Keys.Bind(ctx, m.From.ID, m.From.Username, text)
		switch {
		case err != nil:
			slog.ErrorContext(ctx, "telegram bind", "err", err)
			return
		case res.Blocked:
			h.say(ctx, chat, lang, "tg.blocked")
			return
		case !res.OK:
			h.say(ctx, chat, lang, "tg.wrong_key")
			return
		}
		lang = h.Users.Language(ctx, res.UserID)
		if res.Previous != 0 {
			h.say(ctx, strconv.FormatInt(res.Previous, 10), lang, "tg.unbound")
		}
		if !h.Registry.Open(ctx, res.UserID, domain.ChannelTelegram) {
			h.say(ctx, chat, lang, "channel.unavailable")
			return
		}
		h.say(ctx, chat, lang, "tg.bound")
		return
	}
	lang = h.Users.Language(ctx, uid)
	if !h.Registry.Open(ctx, uid, domain.ChannelTelegram) { // R2, AR-04
		metrics.ChannelInbound.WithLabelValues("telegram", "unavailable").Inc()
		h.say(ctx, chat, lang, "channel.unavailable")
		return
	}
	if text == "/start" {
		h.say(ctx, chat, lang, "tg.ready")
		return
	}
	if text == "" {
		text = strings.TrimSpace(m.Caption)
	}
	atts := h.fetchAll(ctx, uid, chat, lang, m)
	if text == "" && len(atts) == 0 {
		return
	}
	h.Bot.Typing(ctx, chat)
	metrics.ChannelInbound.WithLabelValues("telegram", "accepted").Inc()
	if err := h.Inbox.Receive(ctx, Inbound{UserID: uid, Channel: domain.ChannelTelegram, Text: text, Attachments: atts}); err != nil {
		slog.ErrorContext(ctx, "telegram inbound", "err", err)
		h.say(ctx, chat, lang, "tg.not_delivered")
	}
}

func (h *Webhook) fetchAll(ctx context.Context, uid uuid.UUID, chat, lang string, m *TGMessage) []uuid.UUID {
	var atts []uuid.UUID
	fetch := func(fileID, name, mime string, size int64) {
		if size > h.MaxFile {
			h.say(ctx, chat, lang, "tg.too_large")
			return
		}
		rc, path, err := h.Bot.FetchFile(ctx, fileID)
		if err != nil {
			slog.WarnContext(ctx, "telegram file", "err", err)
			return
		}
		defer rc.Close()
		if name == "" {
			name = path[strings.LastIndex(path, "/")+1:]
		}
		if size <= 0 {
			size = -1 // unknown: the store reads to the end and refuses a file over the limit
		}
		id, err := h.Attachments.Store(ctx, uid, name, mime, rc, size)
		if err != nil {
			if tooLarge(err) {
				h.say(ctx, chat, lang, "tg.too_large")
				return
			}
			slog.WarnContext(ctx, "telegram attachment", "err", err)
			return
		}
		atts = append(atts, id)
	}
	if d := m.Document; d != nil {
		fetch(d.FileID, d.FileName, d.MimeType, d.FileSize)
	}
	if len(m.Photo) > 0 {
		p := m.Photo[len(m.Photo)-1]
		fetch(p.FileID, "", "image/jpeg", p.FileSize)
	}
	if v := m.Voice; v != nil {
		fetch(v.FileID, "voice.ogg", v.MimeType, 0)
	}
	return atts
}

func tooLarge(err error) bool {
	e, ok := apperr.As(err)
	return ok && e.Code == "too_large"
}

func (h *Webhook) botIdentity(ctx context.Context) (int64, string) {
	if h.botID.Load() == 0 || h.Bot.Username() == "" {
		if id, _, err := h.Bot.Me(ctx); err == nil {
			h.botID.Store(id)
		}
	}
	return h.botID.Load(), h.Bot.Username()
}

// membership handles the bot added to or removed from a group (arch §5.3).
func (h *Webhook) membership(ctx context.Context, chat TGChat, from TGUser, old, status string) {
	if chat.Type == "private" || chat.Type == "channel" || h.Groups == nil {
		return
	}
	chatID := strconv.FormatInt(chat.ID, 10)
	switch status {
	case "member", "administrator":
		if old == "member" || old == "administrator" {
			return // the rights of the bot changed; it was not added
		}
		adder, _, _ := h.Keys.UserOf(ctx, from.ID)
		ev := GroupEvent{Channel: domain.ChannelTelegram, ChatID: chatID, Title: chat.Title, AdderID: adder,
			Members: h.Bot.MemberCount(ctx, chatID), Language: from.LanguageCode}
		msg, ok := h.Groups.Added(ctx, ev)
		if msg != "" {
			_ = h.Bot.SendText(ctx, chatID, msg)
		}
		if !ok {
			_ = h.Bot.Leave(ctx, chatID)
		}
	case "left", "kicked":
		h.Groups.Removed(ctx, domain.ChannelTelegram, chatID)
	}
}

// group passes an address to the bot in a group to the group agent (R16).
func (h *Webhook) group(ctx context.Context, m *TGMessage) {
	if h.Groups == nil {
		return
	}
	botID, username := h.botIdentity(ctx)
	if !m.Mentions(username, botID) {
		return // GR-03: neither passed to the agent nor stored
	}
	chatID := strconv.FormatInt(m.Chat.ID, 10)
	author, _, _ := h.Keys.UserOf(ctx, m.From.ID)
	text := strings.TrimSpace(strings.ReplaceAll(m.Text+" "+m.Caption, "@"+username, ""))
	gm := GroupMessage{Channel: domain.ChannelTelegram, ChatID: chatID, Title: m.Chat.Title, AuthorID: author,
		AuthorName: strings.TrimSpace(m.From.First + " " + m.From.Last), Text: text, Language: m.From.LanguageCode}
	if q := m.ReplyTo; q != nil && (q.From == nil || q.From.ID != botID) {
		gm.Quote = strings.TrimSpace(q.Text + " " + q.Caption)
	}
	if reply := h.Groups.Message(ctx, gm); reply != "" {
		_ = h.Bot.SendText(ctx, chatID, reply)
		return
	}
	h.Bot.Typing(ctx, chatID)
}

func utf16Units(s string) []uint16 {
	var out []uint16
	for _, r := range s {
		if r >= 0x10000 {
			r -= 0x10000
			out = append(out, uint16(0xD800+(r>>10)), uint16(0xDC00+(r&0x3FF)))
		} else {
			out = append(out, uint16(r))
		}
	}
	return out
}

func fromUTF16(u []uint16) string {
	var b strings.Builder
	for i := 0; i < len(u); i++ {
		c := rune(u[i])
		if c >= 0xD800 && c < 0xDC00 && i+1 < len(u) {
			c = 0x10000 + (c-0xD800)<<10 + (rune(u[i+1]) - 0xDC00)
			i++
		}
		b.WriteRune(c)
	}
	return b.String()
}
