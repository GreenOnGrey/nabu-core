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
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// Telegram is the Telegram bot adapter.
type Telegram struct {
	APIURL string // https://api.telegram.org
	Token  string
	// Secret is the path secret of the webhook and its secret_token header.
	Secret string
	http   *http.Client
	name   string
}

// NewTelegram creates the adapter.
func NewTelegram(apiURL, token, secret string) *Telegram {
	return &Telegram{APIURL: strings.TrimRight(apiURL, "/"), Token: token, Secret: secret, http: &http.Client{Timeout: 30 * time.Second}}
}

// Channel implements Adapter.
func (t *Telegram) Channel() string { return domain.ChannelTelegram }

func (t *Telegram) call(ctx context.Context, method string, in any, out any) error {
	b, _ := json.Marshal(in)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.APIURL+"/bot"+t.Token+"/"+method, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.http.Do(req)
	if err != nil {
		return fmt.Errorf("telegram %s: %w", method, scrub(err, t.Token))
	}
	defer resp.Body.Close()
	var r struct {
		OK          bool            `json:"ok"`
		Description string          `json:"description"`
		Result      json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&r); err != nil {
		return fmt.Errorf("telegram %s: %d", method, resp.StatusCode)
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

func scrub(err error, token string) error {
	return fmt.Errorf("%s", strings.ReplaceAll(err.Error(), token, "***"))
}

// Setup registers the webhook (tech §8) and reads the bot's username.
func (t *Telegram) Setup(ctx context.Context, publicAPIURL string) error {
	var me struct {
		Username string `json:"username"`
	}
	if err := t.call(ctx, "getMe", map[string]any{}, &me); err != nil {
		return err
	}
	t.name = me.Username
	return t.call(ctx, "setWebhook", map[string]any{"url": publicAPIURL + "/hooks/v1/telegram/" + t.Secret, "secret_token": t.Secret,
		"allowed_updates": []string{"message"}}, nil)
}

// Username is the bot's username after Setup.
func (t *Telegram) Username() string { return t.name }

// Send implements Adapter: MarkdownV2, split into messages (CH-01); a part
// Telegram refuses to parse goes as plain text.
func (t *Telegram) Send(ctx context.Context, chatID, md string) error {
	for _, part := range Split(md, TelegramLimit) {
		err := t.call(ctx, "sendMessage", map[string]any{"chat_id": chatID, "text": ToMarkdownV2(part), "parse_mode": "MarkdownV2",
			"link_preview_options": map[string]bool{"is_disabled": true}}, nil)
		var te *TelegramError
		if err != nil && errorsAs(err, &te) && te.Status == http.StatusBadRequest {
			for _, plain := range splitPlain(part, TelegramLimit) {
				err = t.call(ctx, "sendMessage", map[string]any{"chat_id": chatID, "text": plain}, nil)
			}
		}
		if err != nil {
			return err
		}
		metrics.ChannelMessages.WithLabelValues("telegram", "out").Inc()
	}
	return nil
}

func splitPlain(s string, n int) []string {
	r := []rune(s)
	var out []string
	for len(r) > n {
		out = append(out, string(r[:n]))
		r = r[n:]
	}
	return append(out, string(r))
}

// Typing implements Adapter.
func (t *Telegram) Typing(ctx context.Context, chatID string) {
	_ = t.call(ctx, "sendChatAction", map[string]any{"chat_id": chatID, "action": "typing"}, nil)
}

// FetchFile implements Adapter: getFile and the download.
func (t *Telegram) FetchFile(ctx context.Context, fileID string) (io.ReadCloser, string, error) {
	var f struct {
		FilePath string `json:"file_path"`
	}
	if err := t.call(ctx, "getFile", map[string]any{"file_id": fileID}, &f); err != nil {
		return nil, "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.APIURL+"/file/bot"+t.Token+"/"+f.FilePath, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := t.http.Do(req)
	if err != nil {
		return nil, "", scrub(err, t.Token)
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, "", fmt.Errorf("telegram file: %d", resp.StatusCode)
	}
	return resp.Body, f.FilePath, nil
}

// Update is the part of a Telegram update Nabu reads.
type Update struct {
	Message *struct {
		MessageID int64 `json:"message_id"`
		From      *struct {
			ID       int64  `json:"id"`
			Username string `json:"username"`
			First    string `json:"first_name"`
		} `json:"from"`
		Chat struct {
			ID   int64  `json:"id"`
			Type string `json:"type"`
		} `json:"chat"`
		Text     string `json:"text"`
		Caption  string `json:"caption"`
		Document *struct {
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
	} `json:"message"`
}

// Webhook handles updates of the bot.
type Webhook struct {
	Bot         *Telegram
	Links       *Links
	Inbox       Inbox
	Attachments Attachments
	MaxFile     int64
	WebURL      string
}

// Route mounts POST /hooks/v1/telegram/{secret}.
func (h *Webhook) Route(r chi.Router) {
	r.Post("/hooks/v1/telegram/{secret}", h.serve)
}

func (h *Webhook) serve(w http.ResponseWriter, r *http.Request) {
	// CH-03: without the secret of the path and of the header — 401.
	if subtle.ConstantTimeCompare([]byte(chi.URLParam(r, "secret")), []byte(h.Bot.Secret)) != 1 ||
		subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Telegram-Bot-Api-Secret-Token")), []byte(h.Bot.Secret)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var u Update
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&u); err != nil || u.Message == nil || u.Message.From == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	// Answer Telegram at once; the agent's answer comes as a new message.
	w.WriteHeader(http.StatusOK)
	ctx := context.WithoutCancel(r.Context())
	go h.handle(ctx, u)
}

func (h *Webhook) handle(ctx context.Context, u Update) {
	m := u.Message
	metrics.ChannelMessages.WithLabelValues("telegram", "in").Inc()
	if m.Chat.Type != "private" {
		return // personal agents talk in private chats only
	}
	ext := strconv.FormatInt(m.From.ID, 10)
	chat := strconv.FormatInt(m.Chat.ID, 10)
	text := strings.TrimSpace(m.Text)
	if code, ok := strings.CutPrefix(text, "/start"); ok && strings.TrimSpace(code) != "" {
		account := m.From.Username
		if account != "" {
			account = "@" + account
		} else {
			account = m.From.First
		}
		if _, err := h.Links.Link(ctx, domain.ChannelTelegram, strings.TrimSpace(code), ext, chat, account); err != nil {
			_ = h.Bot.Send(ctx, chat, "The link code is invalid or expired. Get a new one on the Nabu site: Connections → Telegram.")
			return
		}
		_ = h.Bot.Send(ctx, chat, "Telegram is linked to your Nabu account. Write to your agent here.")
		return
	}
	link, err := h.Links.ByExternal(ctx, domain.ChannelTelegram, ext)
	if err != nil {
		slog.ErrorContext(ctx, "telegram link lookup", "err", err)
		return
	}
	if link == nil { // AUTH-10: an unlinked account gets the instruction, the agent is not called
		_ = h.Bot.Send(ctx, chat, "This Telegram account is not linked to Nabu. Open "+h.WebURL+"/connections, get a link code and send it here as /start <code>.")
		return
	}
	if link.Status == "blocked" { // AUTH-11
		_ = h.Bot.Send(ctx, chat, "Access to the agent is closed for your account.")
		return
	}
	if text == "/start" {
		_ = h.Bot.Send(ctx, chat, "You are linked. Write to your agent here.")
		return
	}
	if text == "" {
		text = strings.TrimSpace(m.Caption)
	}
	var atts []uuidLike
	fetch := func(fileID, name, mime string, size int64) {
		if size > h.MaxFile {
			_ = h.Bot.Send(ctx, chat, "The file is too large for Nabu.")
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
		id, err := h.Attachments.Store(ctx, link.UserID, name, mime, io.LimitReader(rc, h.MaxFile), size)
		if err != nil {
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
	if text == "" && len(atts) == 0 {
		return
	}
	h.Bot.Typing(ctx, chat)
	if err := h.Inbox.Receive(ctx, Inbound{UserID: link.UserID, Channel: domain.ChannelTelegram, Text: text, Attachments: atts}); err != nil {
		slog.ErrorContext(ctx, "telegram inbound", "err", err)
		_ = h.Bot.Send(ctx, chat, "The message could not be delivered to the agent, try again later.")
	}
}
