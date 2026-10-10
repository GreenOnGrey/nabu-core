package email

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
	"github.com/emersion/go-message/mail"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/GreenOnGrey/nabu-core/internal/channels"
	"github.com/GreenOnGrey/nabu-core/internal/domain"
	"github.com/GreenOnGrey/nabu-core/internal/platform/events"
	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/platform/postgres"
	"github.com/GreenOnGrey/nabu-core/internal/render"
)

// Results of the mail log.
const (
	Accepted    = "accepted"
	Rejected    = "rejected"
	Ignored     = "ignored"
	Unavailable = "unavailable"
)

// ProcessedFlag and ProcessedFolder mark handled letters (arch §3.1).
const (
	ProcessedFlag   = imap.Flag("$NabuProcessed")
	ProcessedFolder = "Nabu/Processed"
)

// UnreadEvent is the SSE event of an unread mail answer (tech §3).
const UnreadEvent = "conversation.unread"

// Space gives the files the agent created during a turn (R8).
type Space interface {
	AgentFiles(ctx context.Context, uid uuid.UUID, since time.Time) ([]SpaceFile, error)
	Open(ctx context.Context, uid uuid.UUID, path string) (io.ReadCloser, int64, error)
}

// SpaceFile is a file of the space.
type SpaceFile struct {
	Path string
	Size int64
}

// Account is the user of a sender.
type Account struct {
	ID       uuid.UUID
	Status   string
	Language string
}

// Accounts finds users by email.
type Accounts func(ctx context.Context, email string) (*Account, error)

// Mail is the mail channel.
type Mail struct {
	Pool        *pgxpool.Pool
	Registry    *channels.Registry
	Accounts    Accounts
	Inbox       channels.Inbox
	Attachments channels.Attachments
	Space       Space
	Events      events.Publisher
	WebURL      string
	ReplyLimit  int   // EMAIL_REPLY_LIMIT_PER_HOUR
	AttachMax   int64 // EMAIL_ATTACH_MAX
	InboundMax  int64 // EMAIL_INBOUND_MAX
	// Pending reports whether calls of the conversation wait for the user
	// since the start of the turn: the letter gets «Confirm in Nabu» (R9).
	Pending func(ctx context.Context, conv uuid.UUID, since time.Time) bool
	// Send delivers a letter; nil — SMTP of the channel (tests replace it).
	Send func(ctx context.Context, c Credentials, from string, to []string, msg []byte) error

	mu    sync.Mutex
	reset chan struct{}
	now   func() time.Time
	// handle replaces Process in tests of the receiver.
	handle func(ctx context.Context, c Credentials, raw []byte, key string) string
}

func (m *Mail) status(ctx context.Context, status, reason string) {
	if m.Registry != nil {
		m.Registry.SetStatus(ctx, domain.ChannelEmail, status, reason)
	}
}

func (m *Mail) clock() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

// Credentials reads the settings and the secrets of the channel.
func (m *Mail) Credentials(ctx context.Context) (Credentials, bool, error) {
	var s Settings
	sec, enabled, err := m.Registry.Config(ctx, domain.ChannelEmail, &s)
	if err != nil {
		return Credentials{}, false, err
	}
	c := Credentials{Settings: s, AppPassword: sec[SecretAppPassword]}
	if s.Provider == Google {
		if sec[SecretServiceAccount] == "" {
			return c, enabled, errors.New("the key of the Google service account is not set")
		}
		g, err := NewGoogleToken(sec[SecretServiceAccount], s.Mailbox)
		if err != nil {
			return c, enabled, err
		}
		c.Google = g
	} else if c.AppPassword == "" {
		return c, enabled, errors.New("the app password of the mailbox is not set")
	}
	return c, enabled, nil
}

// Check signs in to IMAP and SMTP.
func (m *Mail) Check(ctx context.Context) error {
	c, _, err := m.Credentials(ctx)
	if err != nil {
		return err
	}
	return c.Check(ctx)
}

// Reset makes the receiver reconnect with the new settings.
func (m *Mail) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reset != nil {
		select {
		case m.reset <- struct{}{}:
		default:
		}
	}
}

func (m *Mail) log(ctx context.Context, l *Letter, key, result, reason string, conv *uuid.UUID) {
	from, subject := "", ""
	if l != nil {
		from, subject = l.From, l.Subject
	}
	_, _ = m.Pool.Exec(ctx, `INSERT INTO email_log (message_id, from_addr, subject, result, reason, conversation_id) VALUES ($1,$2,$3,$4,NULLIF($5,''),$6)`,
		key, from, clip(subject, 300), result, reason, conv)
	label := result
	if result == Ignored {
		label = "automatic"
	}
	metrics.ChannelInbound.WithLabelValues("email", label).Inc()
	slog.InfoContext(ctx, "mail", "result", result, "reason", reason, "from", from)
}

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// processed reports whether a letter was already handled (ML-06: once).
func (m *Mail) processed(ctx context.Context, key string) bool {
	var ok bool
	_ = m.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM email_log WHERE message_id = $1)`, key).Scan(&ok)
	return ok
}

// take counts one letter of the kind to the address within the window;
// false — the limit is reached (ML-15, ML-21).
func (m *Mail) take(ctx context.Context, addr, kind string, window time.Duration, limit int) bool {
	start := m.clock().Truncate(window)
	var n int
	err := m.Pool.QueryRow(ctx, `INSERT INTO email_rate (address, kind, window_start, count) VALUES ($1,$2,$3,1)
		ON CONFLICT (address, kind, window_start) DO UPDATE SET count = email_rate.count + 1 RETURNING count`, addr, kind, start).Scan(&n)
	if err != nil {
		return false
	}
	if n > limit {
		_, _ = m.Pool.Exec(ctx, `UPDATE email_rate SET count = count - 1 WHERE address = $1 AND kind = $2 AND window_start = $3`, addr, kind, start)
		return false
	}
	return true
}

// Process handles one letter of the mailbox (arch §3.2). key identifies it
// (Message-ID, or UIDVALIDITY/UID without one).
func (m *Mail) Process(ctx context.Context, c Credentials, raw []byte, key string) string {
	l, err := Parse(raw, m.InboundMax)
	if err != nil {
		m.log(ctx, nil, key, Rejected, "unreadable letter", nil)
		return Rejected
	}
	if l.MessageID != "" {
		key = l.MessageID
	} else {
		l.MessageID = key
	}
	if m.processed(ctx, key) {
		return ""
	}
	s := c.Settings
	if s.Bot(l.From) {
		m.log(ctx, l, key, Ignored, "own letter", nil)
		return Ignored
	}
	if auto, why := l.Automatic(); auto { // ML-09
		m.log(ctx, l, key, Ignored, why, nil)
		return Ignored
	}
	if ok, why := l.Authentic(s); !ok { // ML-07, ML-08, ML-22
		m.log(ctx, l, key, Rejected, why, nil)
		metrics.EmailAuthRejected.WithLabelValues(rejectLabel(why)).Inc()
		return Rejected
	}
	mode := l.Mode(s)
	acc, err := m.Accounts(ctx, l.From)
	if err != nil {
		slog.ErrorContext(ctx, "mail account", "err", err)
		return ""
	}
	if acc == nil || acc.Status != "active" || !m.Registry.Open(ctx, acc.ID, domain.ChannelEmail) {
		// R10: a short answer, once a day, only when the bot is the only recipient.
		m.log(ctx, l, key, Unavailable, "no account or the channel is not available", nil)
		if mode == "direct" && m.take(ctx, l.From, "unavailable", 24*time.Hour, 1) {
			lang := ""
			if acc != nil {
				lang = acc.Language
			}
			m.notifyUnavailable(ctx, c, l, lang)
		}
		return Unavailable
	}
	conv, err := m.thread(ctx, acc.ID, l, mode)
	if err != nil {
		slog.ErrorContext(ctx, "mail thread", "err", err)
		return ""
	}
	var atts []uuid.UUID
	for _, f := range l.Files {
		id, err := m.Attachments.Store(ctx, acc.ID, f.Name, f.MimeType, bytes.NewReader(f.Data), int64(len(f.Data)))
		if err != nil {
			slog.WarnContext(ctx, "mail attachment", "err", err)
			continue
		}
		atts = append(atts, id)
	}
	text := l.Text
	if text == "" && len(atts) == 0 {
		text = "(" + CleanSubject(l.Subject) + ")"
	}
	meta := map[string]any{"email": map[string]any{"from": l.From, "fromName": l.FromName, "to": l.To, "cc": l.Cc,
		"subject": l.Subject, "mode": mode, "quoted": clip(l.Quoted, 6000), "tooLarge": l.TooLarge}}
	mctx, _ := json.Marshal(meta)
	if err := m.Inbox.Receive(ctx, channels.Inbound{UserID: acc.ID, Channel: domain.ChannelEmail, Text: clip(text, 30000),
		Attachments: atts, Conversation: &conv, Context: mctx}); err != nil {
		slog.ErrorContext(ctx, "mail inbound", "err", err)
		return ""
	}
	m.log(ctx, l, key, Accepted, "", &conv)
	return Accepted
}

func rejectLabel(why string) string {
	switch {
	case strings.HasPrefix(why, "no Authentication-Results"):
		return "no_header"
	case strings.HasPrefix(why, "domain"):
		return "domain"
	case strings.HasPrefix(why, "neither"):
		return "fail"
	}
	return "other"
}

// thread finds the topic of a thread by In-Reply-To and References or
// starts a new one titled by the subject (R6, ML-01, ML-02).
func (m *Mail) thread(ctx context.Context, uid uuid.UUID, l *Letter, mode string) (uuid.UUID, error) {
	var conv uuid.UUID
	err := postgres.InTx(ctx, m.Pool, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `SELECT t.conversation_id FROM email_threads t JOIN conversations c ON c.id = t.conversation_id
			WHERE c.user_id = $1 AND t.message_ids && $2::text[] LIMIT 1`, uid, l.IDs()).Scan(&conv)
		if postgres.IsNoRows(err) {
			if err := tx.QueryRow(ctx, `INSERT INTO conversations (user_id, kind, title, source, writes_require_confirmation)
				VALUES ($1,'topic',$2,'email',true) RETURNING id`, uid, clip(CleanSubject(l.Subject), 120)).Scan(&conv); err != nil {
				return err
			}
			root := l.MessageID
			if len(l.References) > 0 {
				root = l.References[0]
			}
			_, err = tx.Exec(ctx, `INSERT INTO email_threads (conversation_id, root_message_id, message_ids, subject) VALUES ($1,$2,$3,$4)`,
				conv, root, append(l.IDs(), l.MessageID), CleanSubject(l.Subject))
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE email_threads SET message_ids = array_append(message_ids, $2), reply_to = $3, reply_mode = $4,
			last_message_id = $2 WHERE conversation_id = $1 AND NOT ($2 = ANY(message_ids))`, conv, l.MessageID, l.From, mode)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE email_threads SET reply_to = $2, reply_mode = $3, last_message_id = $4 WHERE conversation_id = $1`,
			conv, l.From, mode, l.MessageID)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE conversations SET archived_at = NULL WHERE id = $1`, conv)
		return err
	})
	return conv, err
}

// ─── answers (tech §5.3) ────────────────────────────────────────────

// Reply sends the answer of the agent in a mail topic: a letter in the
// thread to the sender when the bot was the only recipient (R8), otherwise
// only the topic with an unread mark. since is the start of the turn: files
// the agent created after it are attached.
func (m *Mail) Reply(ctx context.Context, uid, conv uuid.UUID, answer string, since time.Time) error {
	var to, mode, last, subject string
	var ids []string
	err := m.Pool.QueryRow(ctx, `SELECT COALESCE(reply_to,''), COALESCE(reply_mode,'web_only'), COALESCE(last_message_id,''), message_ids, subject
		FROM email_threads WHERE conversation_id = $1`, conv).Scan(&to, &mode, &last, &ids, &subject)
	if postgres.IsNoRows(err) {
		return nil
	}
	if err != nil {
		return err
	}
	sendLetter := mode == "direct" && to != "" && strings.TrimSpace(answer) != "" && m.Registry.Open(ctx, uid, domain.ChannelEmail)
	if sendLetter && !m.take(ctx, to, "reply", time.Hour, m.ReplyLimit) { // ML-15
		slog.InfoContext(ctx, "mail reply limit reached, the answer stays in the topic", "conversation", conv)
		sendLetter = false
	}
	if !sendLetter {
		return m.MarkUnread(ctx, uid, conv)
	}
	c, _, err := m.Credentials(ctx)
	if err != nil {
		_ = m.MarkUnread(ctx, uid, conv)
		return err
	}
	lang := ""
	if acc, err := m.Accounts(ctx, to); err == nil && acc != nil {
		lang = acc.Language
	}
	msgID := uuid.NewString() + "@" + Domain(c.Settings.Mailbox)
	refs := append([]string{}, ids...)
	letter, err := m.compose(ctx, c, uid, conv, to, subject, last, refs, msgID, answer, lang, since)
	if err != nil {
		return err
	}
	if err := m.send(ctx, c, []string{to}, letter); err != nil {
		metrics.ChannelOutbound.WithLabelValues("email", "error").Inc()
		_ = m.MarkUnread(ctx, uid, conv)
		return err
	}
	metrics.ChannelOutbound.WithLabelValues("email", "ok").Inc()
	_, _ = m.Pool.Exec(ctx, `UPDATE email_threads SET message_ids = array_append(message_ids, $2) WHERE conversation_id = $1`, conv, msgID)
	return nil
}

func (m *Mail) send(ctx context.Context, c Credentials, to []string, msg []byte) error {
	if m.Send != nil {
		return m.Send(ctx, c, c.Settings.Mailbox, to, msg)
	}
	return c.SMTP(ctx, c.Settings.Mailbox, to, msg)
}

// MarkUnread counts an unread answer of a topic (ML-11, UI-04).
func (m *Mail) MarkUnread(ctx context.Context, uid, conv uuid.UUID) error {
	var n int
	if err := m.Pool.QueryRow(ctx, `UPDATE conversations SET unread_count = unread_count + 1 WHERE id = $1 RETURNING unread_count`, conv).Scan(&n); err != nil {
		return err
	}
	if m.Events != nil {
		m.Events.Publish(ctx, events.Event{Type: UnreadEvent, UserID: &uid, Data: map[string]any{"conversationId": conv, "unreadCount": n}})
	}
	return nil
}

func (m *Mail) compose(ctx context.Context, c Credentials, uid, conv uuid.UUID, to, subject, inReplyTo string, refs []string, msgID, answer, lang string,
	since time.Time) ([]byte, error) {
	var h mail.Header
	h.SetDate(m.clock())
	h.SetAddressList("From", []*mail.Address{{Name: "Nabu", Address: c.Settings.Mailbox}})
	h.SetAddressList("To", []*mail.Address{{Address: to}})
	h.SetSubject("Re: " + CleanSubject(subject))
	h.SetMessageID(msgID)
	if inReplyTo != "" {
		h.SetMsgIDList("In-Reply-To", []string{inReplyTo})
	}
	if len(refs) > 0 {
		h.SetMsgIDList("References", refs)
	}
	link := m.WebURL + "/chat/" + conv.String()
	htmlBody, textBody := render.Email(render.Parse(answer))
	var attach []SpaceFile
	var links []string
	if m.Space != nil {
		files, err := m.Space.AgentFiles(ctx, uid, since)
		if err == nil {
			var total int64
			for _, f := range files {
				if total+f.Size <= m.AttachMax { // ML-14: up to 10 MB in total, the rest as links
					attach = append(attach, f)
					total += f.Size
				} else {
					links = append(links, f.Path)
				}
			}
		}
	}
	for _, p := range links {
		u := m.WebURL + "/space?path=" + url.QueryEscape(p)
		htmlBody += `<p style="margin:8px 0;">📎 <a href="` + u + `" style="color:#6d5bd0;">` + htmlEsc(p) + `</a></p>`
		textBody += "\n\n📎 " + p + ": " + u
	}
	open := channels.T(lang, "mail.open")
	if m.Pending != nil && m.Pending(ctx, conv, since) { // ML-17
		open = channels.T(lang, "mail.confirm")
	}
	htmlBody = `<div style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;font-size:14px;line-height:1.5;color:#1d1d2c;max-width:720px;">` +
		htmlBody + `<p style="margin:20px 0 0;"><a href="` + link + `" style="color:#6d5bd0;">` + htmlEsc(open) + `</a></p></div>`
	textBody += "\n\n" + open + ": " + link
	return buildLetter(h, textBody, htmlBody, func(add func(name, mime string, r io.Reader) error) error {
		for _, f := range attach {
			rc, _, err := m.Space.Open(ctx, uid, f.Path)
			if err != nil {
				continue
			}
			err = add(f.Path[strings.LastIndex(f.Path, "/")+1:], "", rc)
			rc.Close()
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func htmlEsc(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;")
	return r.Replace(s)
}

// buildLetter writes multipart/mixed with multipart/alternative and attachments.
func buildLetter(h mail.Header, text, htmlBody string, files func(add func(name, mime string, r io.Reader) error) error) ([]byte, error) {
	var buf bytes.Buffer
	w, err := mail.CreateWriter(&buf, h)
	if err != nil {
		return nil, err
	}
	alt, err := w.CreateInline()
	if err != nil {
		return nil, err
	}
	for _, p := range []struct{ ct, body string }{{"text/plain", text}, {"text/html", htmlBody}} {
		var ih mail.InlineHeader
		ih.SetContentType(p.ct, map[string]string{"charset": "utf-8"})
		pw, err := alt.CreatePart(ih)
		if err != nil {
			return nil, err
		}
		if _, err := io.WriteString(pw, p.body); err != nil {
			return nil, err
		}
		pw.Close()
	}
	alt.Close()
	if files != nil {
		err := files(func(name, mime string, r io.Reader) error {
			var ah mail.AttachmentHeader
			if mime == "" {
				mime = "application/octet-stream"
			}
			ah.SetContentType(mime, nil)
			ah.SetFilename(name)
			aw, err := w.CreateAttachment(ah)
			if err != nil {
				return err
			}
			_, err = io.Copy(aw, r)
			aw.Close()
			return err
		})
		if err != nil {
			return nil, err
		}
	}
	w.Close()
	return buf.Bytes(), nil
}

// SendTask delivers the result of a scheduled task by mail (R11): a new
// letter to the user; false — the limit of letters to the address is reached.
func (m *Mail) SendTask(ctx context.Context, to, title, text, lang string) (bool, error) {
	c, enabled, err := m.Credentials(ctx)
	if err != nil || !enabled {
		return false, err
	}
	if !m.take(ctx, to, "reply", time.Hour, m.ReplyLimit) {
		return false, nil
	}
	var h mail.Header
	h.SetDate(m.clock())
	h.SetAddressList("From", []*mail.Address{{Name: "Nabu", Address: c.Settings.Mailbox}})
	h.SetAddressList("To", []*mail.Address{{Address: to}})
	h.SetSubject("⏰ " + title)
	h.SetMessageID(uuid.NewString() + "@" + Domain(c.Settings.Mailbox))
	htmlBody, textBody := render.Email(render.Parse(text))
	link := m.WebURL + "/chat"
	open := channels.T(lang, "mail.open")
	htmlBody = `<div style="font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;font-size:14px;line-height:1.5;color:#1d1d2c;max-width:720px;">` +
		htmlBody + `<p style="margin:20px 0 0;"><a href="` + link + `" style="color:#6d5bd0;">` + htmlEsc(open) + `</a></p></div>`
	msg, err := buildLetter(h, textBody+"\n\n"+open+": "+link, htmlBody, nil)
	if err != nil {
		return false, err
	}
	if err := m.send(ctx, c, []string{to}, msg); err != nil {
		metrics.ChannelOutbound.WithLabelValues("email", "error").Inc()
		return false, err
	}
	metrics.ChannelOutbound.WithLabelValues("email", "ok").Inc()
	return true, nil
}

// notifyUnavailable sends «the channel is not available» (R10).
func (m *Mail) notifyUnavailable(ctx context.Context, c Credentials, l *Letter, lang string) {
	var h mail.Header
	h.SetDate(m.clock())
	h.SetAddressList("From", []*mail.Address{{Name: "Nabu", Address: c.Settings.Mailbox}})
	h.SetAddressList("To", []*mail.Address{{Address: l.From}})
	h.SetSubject("Re: " + CleanSubject(l.Subject))
	h.SetMessageID(uuid.NewString() + "@" + Domain(c.Settings.Mailbox))
	h.Set("Auto-Submitted", "auto-replied")
	if l.MessageID != "" {
		h.SetMsgIDList("In-Reply-To", []string{l.MessageID})
		h.SetMsgIDList("References", append(append([]string{}, l.References...), l.MessageID))
	}
	text := channels.T(lang, "mail.unavailable")
	msg, err := buildLetter(h, text, "<p>"+htmlEsc(text)+"</p>", nil)
	if err == nil {
		err = m.send(ctx, c, []string{l.From}, msg)
	}
	if err != nil {
		slog.WarnContext(ctx, "mail unavailable notice", "err", err)
	}
}

// ─── the receiver (arch §3.1, tech §5.1) ────────────────────────────

// Run receives letters under the advisory lock nabu:email (ML-06): one
// receiver per instance, IDLE with a reconciliation every 5 minutes,
// reconnection with an exponential delay.
func (m *Mail) Run(ctx context.Context) {
	m.mu.Lock()
	m.reset = make(chan struct{}, 1)
	m.mu.Unlock()
	postgres.RunLocked(ctx, m.Pool, "nabu:email", 15*time.Second, m.receive)
}

func (m *Mail) receive(ctx context.Context) {
	backoff := 5 * time.Second
	for ctx.Err() == nil {
		c, enabled, err := m.Credentials(ctx)
		if !enabled || c.Settings.Mailbox == "" {
			m.wait(ctx, 30*time.Second)
			continue
		}
		if err == nil {
			err = m.session(ctx, c)
		}
		metrics.IMAPConnected.Set(0)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.WarnContext(ctx, "mail receiver", "err", err)
			m.status(ctx, "error", err.Error())
			m.wait(ctx, backoff)
			if backoff < 5*time.Minute {
				backoff *= 2
			}
			continue
		}
		backoff = 5 * time.Second
	}
}

func (m *Mail) wait(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-m.reset:
	case <-time.After(d):
	}
}

// session holds one IMAP connection until it breaks or the settings change.
func (m *Mail) session(ctx context.Context, c Credentials) error {
	wake := make(chan struct{}, 1)
	handler := &imapclient.UnilateralDataHandler{Mailbox: func(d *imapclient.UnilateralDataMailbox) {
		if d.NumMessages != nil {
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}}
	cl, err := c.DialIMAP(ctx, handler)
	if err != nil {
		return err
	}
	defer cl.Close()
	_ = cl.Create(ProcessedFolder, nil).Wait() // exists after the first start
	sel, err := cl.Select("INBOX", nil).Wait()
	if err != nil {
		return fmt.Errorf("select INBOX: %w", err)
	}
	metrics.IMAPConnected.Set(1)
	m.status(ctx, "ok", "")
	idle := cl.Caps().Has(imap.CapIdle)
	for ctx.Err() == nil {
		if err := m.fetchNew(ctx, c, cl, sel.UIDValidity); err != nil {
			return err
		}
		reconcile := 5 * time.Minute
		if !idle {
			reconcile = time.Minute // tech §5.1: without IDLE — a poll every 60 seconds
		}
		var cmd *imapclient.IdleCommand
		if idle {
			if cmd, err = cl.Idle(); err != nil {
				return fmt.Errorf("idle: %w", err)
			}
		}
		select {
		case <-ctx.Done():
		case <-m.reset:
			if cmd != nil {
				_ = cmd.Close()
			}
			return nil
		case <-wake:
		case <-time.After(reconcile):
		case <-cl.Closed():
			return errors.New("the IMAP connection was closed")
		}
		if cmd != nil {
			if err := cmd.Close(); err != nil {
				return err
			}
			if err := cmd.Wait(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Mail) fetchNew(ctx context.Context, c Credentials, cl *imapclient.Client, uidValidity uint32) error {
	sd, err := cl.UIDSearch(&imap.SearchCriteria{NotFlag: []imap.Flag{ProcessedFlag}}, nil).Wait()
	if err != nil {
		return fmt.Errorf("search: %w", err)
	}
	uids := sd.AllUIDs()
	canMove := cl.Caps().Has(imap.CapMove)
	section := &imap.FetchItemBodySection{Peek: true}
	for len(uids) > 0 {
		batch := uids
		if len(batch) > 20 {
			batch = uids[:20]
		}
		uids = uids[len(batch):]
		set := imap.UIDSetNum(batch...)
		msgs, err := cl.Fetch(set, &imap.FetchOptions{UID: true, BodySection: []*imap.FetchItemBodySection{section}}).Collect()
		if err != nil {
			return fmt.Errorf("fetch: %w", err)
		}
		for _, msg := range msgs {
			raw := msg.FindBodySection(section)
			key := fmt.Sprintf("uid:%d/%d", uidValidity, msg.UID)
			if m.handle != nil {
				m.handle(ctx, c, raw, key)
			} else {
				m.Process(ctx, c, raw, key)
			}
			one := imap.UIDSetNum(msg.UID)
			if err := cl.Store(one, &imap.StoreFlags{Op: imap.StoreFlagsAdd, Silent: true, Flags: []imap.Flag{ProcessedFlag}}, nil).Close(); err != nil {
				return fmt.Errorf("store: %w", err)
			}
			if canMove {
				if _, err := cl.Move(one, ProcessedFolder).Wait(); err != nil {
					slog.WarnContext(ctx, "mail move", "err", err)
				}
			}
		}
	}
	return nil
}

// ─── administration ─────────────────────────────────────────────────

// LogEntry is a row of the mail log.
type LogEntry struct {
	ID             int64      `json:"id"`
	At             time.Time  `json:"at"`
	From           string     `json:"from"`
	Subject        string     `json:"subject"`
	Result         string     `json:"result"`
	Reason         *string    `json:"reason"`
	ConversationID *uuid.UUID `json:"conversationId,omitempty"`
}

// LogRoute mounts GET /channels/email/log (tech §2.1).
func (m *Mail) LogRoute(r chi.Router) {
	r.Get("/channels/email/log", httpx.Handler(func(w http.ResponseWriter, r *http.Request) error {
		q := r.URL.Query()
		page, err := httpx.ParsePage(r)
		if err != nil {
			return err
		}
		args := []any{q.Get("result"), q.Get("from"), page.Limit + 1}
		cond := ""
		if page.Cursor != nil {
			cond = ` AND (at, id) < ($4, $5::bigint)`
			args = append(args, page.Cursor.T, page.Cursor.ID)
		}
		rows, err := m.Pool.Query(r.Context(), `SELECT id, at, COALESCE(from_addr,''), COALESCE(subject,''), result, reason, conversation_id FROM email_log
			WHERE ($1 = '' OR result = $1) AND ($2 = '' OR from_addr ILIKE '%' || $2 || '%')`+cond+` ORDER BY at DESC, id DESC LIMIT $3`, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		var out []LogEntry
		for rows.Next() {
			var e LogEntry
			if err := rows.Scan(&e.ID, &e.At, &e.From, &e.Subject, &e.Result, &e.Reason, &e.ConversationID); err != nil {
				return err
			}
			out = append(out, e)
		}
		httpx.JSON(w, 200, httpx.NewList(out, page.Limit, func(e LogEntry) (time.Time, string) { return e.At, fmt.Sprint(e.ID) }))
		return rows.Err()
	}))
}
