package email

import (
	"bytes"
	"errors"
	"io"
	"mime"
	"regexp"
	"strings"

	"github.com/emersion/go-message"
	_ "github.com/emersion/go-message/charset" // koi8-r, windows-1251 and other charsets of letters
	"github.com/emersion/go-message/mail"
	"golang.org/x/net/html"
)

// Attachment is a file of a letter.
type Attachment struct {
	Name, MimeType string
	Data           []byte
}

// Letter is a parsed letter.
type Letter struct {
	MessageID  string
	InReplyTo  []string
	References []string
	Subject    string
	From       string // lower case address
	FromName   string
	To, Cc     []string
	// AuthResults are the Authentication-Results headers, the top one first.
	AuthResults []string
	// Headers of automatic mail.
	AutoSubmitted, Precedence string
	ListID, ListUnsubscribe   bool
	// Text is the own text of the sender; Quoted are quotes and forwarded letters.
	Text   string
	Quoted string
	Files  []Attachment
	// TooLarge lists attachments dropped by the limit of a letter.
	TooLarge []string
}

// Parse reads a letter. Attachments above maxAttach in total are dropped
// with a mark (tech §5.2: 25 MB per letter).
func Parse(raw []byte, maxAttach int64) (*Letter, error) {
	r, err := mail.CreateReader(bytes.NewReader(raw))
	if err != nil && r == nil {
		return nil, err
	}
	h := r.Header
	l := &Letter{}
	l.MessageID, _ = h.MessageID()
	l.InReplyTo, _ = h.MsgIDList("In-Reply-To")
	l.References, _ = h.MsgIDList("References")
	l.Subject, _ = h.Subject()
	if from, err := h.AddressList("From"); err == nil && len(from) > 0 {
		l.From, l.FromName = strings.ToLower(from[0].Address), from[0].Name
	}
	l.To = addresses(h, "To")
	l.Cc = addresses(h, "Cc")
	fields := h.FieldsByKey("Authentication-Results")
	for fields.Next() {
		v, err := fields.Text()
		if err != nil {
			v = fields.Value()
		}
		l.AuthResults = append(l.AuthResults, v)
	}
	l.AutoSubmitted = strings.ToLower(strings.TrimSpace(h.Get("Auto-Submitted")))
	l.Precedence = strings.ToLower(strings.TrimSpace(h.Get("Precedence")))
	l.ListID = h.Get("List-Id") != ""
	l.ListUnsubscribe = h.Get("List-Unsubscribe") != ""

	var plain, htmlText string
	var forwarded []string
	var size int64
	for {
		p, err := r.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if message.IsUnknownCharset(err) || message.IsUnknownEncoding(err) {
				continue
			}
			break
		}
		switch ph := p.Header.(type) {
		case *mail.InlineHeader:
			ct, _, _ := ph.ContentType()
			b, _ := io.ReadAll(io.LimitReader(p.Body, 4<<20))
			switch {
			case ct == "text/plain" && plain == "":
				plain = string(b)
			case ct == "text/html" && htmlText == "":
				htmlText = HTMLToText(string(b))
			case ct == "message/rfc822":
				forwarded = append(forwarded, string(b))
			}
		case *mail.AttachmentHeader:
			name, _ := ph.Filename()
			ct, _, _ := ph.ContentType()
			if ct == "message/rfc822" {
				b, _ := io.ReadAll(io.LimitReader(p.Body, 4<<20))
				forwarded = append(forwarded, string(b))
				continue
			}
			b, _ := io.ReadAll(io.LimitReader(p.Body, maxAttach+1))
			if name == "" {
				name = "attachment"
			}
			if size+int64(len(b)) > maxAttach {
				l.TooLarge = append(l.TooLarge, name)
				continue
			}
			size += int64(len(b))
			if ct == "" {
				ct = mime.TypeByExtension(name)
			}
			l.Files = append(l.Files, Attachment{Name: name, MimeType: ct, Data: b})
		}
	}
	body := plain
	if strings.TrimSpace(body) == "" {
		body = htmlText
	}
	l.Text, l.Quoted = SplitQuotes(body)
	for _, f := range forwarded {
		if l.Quoted != "" {
			l.Quoted += "\n\n"
		}
		l.Quoted += "[Forwarded letter]\n" + f
	}
	return l, nil
}

func addresses(h mail.Header, key string) []string {
	list, err := h.AddressList(key)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, a := range list {
		out = append(out, strings.ToLower(a.Address))
	}
	return out
}

// Domain is the domain of an address.
func Domain(addr string) string {
	if i := strings.LastIndexByte(addr, '@'); i >= 0 {
		return strings.ToLower(addr[i+1:])
	}
	return ""
}

func localPart(addr string) string {
	if i := strings.LastIndexByte(addr, '@'); i >= 0 {
		return strings.ToLower(addr[:i])
	}
	return strings.ToLower(addr)
}

// quoteHeader matches the line that opens a quoted letter.
var quoteHeader = regexp.MustCompile(`(?i)^(on .{3,200} wrote:|.{3,200} (написал|написала|пишет)( \(а\))?:|-{2,} ?(original message|forwarded message|исходное сообщение|пересылаемое сообщение) ?-{2,}|>+ ?)\s*$`)

// SplitQuotes separates the own text of a letter from quotes: lines with
// ">" and everything after "On … wrote:" or "… написал:" (tech §5.2).
func SplitQuotes(body string) (own, quoted string) {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	lines := strings.Split(body, "\n")
	var o, q []string
	for i, line := range lines {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, ">") {
			q = append(q, line)
			continue
		}
		if quoteHeader.MatchString(t) && !strings.HasPrefix(t, ">") {
			q = append(q, lines[i:]...)
			break
		}
		o = append(o, line)
	}
	return strings.TrimSpace(strings.Join(o, "\n")), strings.TrimSpace(strings.Join(q, "\n"))
}

var manyBreaks = regexp.MustCompile(`\n{3,}`)

// HTMLToText turns the HTML of a letter into text: scripts and styles are
// dropped, blocks end with line breaks, links keep the address, quotes get ">".
func HTMLToText(src string) string {
	z := html.NewTokenizer(strings.NewReader(src))
	var b strings.Builder
	skip := 0
	quote := 0
	var href []string
	nl := func() {
		if b.Len() > 0 && !strings.HasSuffix(b.String(), "\n") {
			b.WriteByte('\n')
		}
		if quote > 0 {
			b.WriteString(strings.Repeat("> ", quote))
		}
	}
	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			out := manyBreaks.ReplaceAllString(b.String(), "\n\n")
			return strings.TrimSpace(out)
		case html.TextToken:
			if skip == 0 {
				t := strings.Join(strings.Fields(string(z.Text())), " ")
				if t != "" {
					if s := b.String(); s != "" && !strings.HasSuffix(s, "\n") && !strings.HasSuffix(s, " ") && !strings.HasSuffix(s, "> ") {
						b.WriteByte(' ')
					}
					b.WriteString(t)
				}
			}
		case html.StartTagToken, html.EndTagToken, html.SelfClosingTagToken:
			name, hasAttr := z.TagName()
			tag := string(name)
			start := tt != html.EndTagToken
			switch tag {
			case "script", "style", "head", "title":
				if tt == html.StartTagToken {
					skip++
				} else if tt == html.EndTagToken && skip > 0 {
					skip--
				}
			case "br":
				nl()
			case "p", "div", "tr", "li", "h1", "h2", "h3", "h4", "table", "ul", "ol":
				nl()
				if start && tag == "li" {
					b.WriteString("• ")
				}
			case "td", "th":
				if start {
					b.WriteString(" | ")
				}
			case "blockquote":
				if start {
					quote++
				} else if quote > 0 {
					quote--
				}
				nl()
			case "a":
				if start {
					u := ""
					for hasAttr {
						var k, v []byte
						k, v, hasAttr = z.TagAttr()
						if string(k) == "href" {
							u = string(v)
						}
					}
					href = append(href, u)
				} else if n := len(href); n > 0 {
					u := href[n-1]
					href = href[:n-1]
					if strings.HasPrefix(u, "http") && !strings.Contains(b.String()[max(0, b.Len()-len(u)):], u) {
						b.WriteString(" (" + u + ")")
					}
				}
			}
		}
	}
}

// ─── classification (tech §5.2) ─────────────────────────────────────

// Automatic reports whether a letter is automatic mail with the reason (ML-09).
func (l *Letter) Automatic() (bool, string) {
	switch {
	case l.AutoSubmitted != "" && l.AutoSubmitted != "no":
		return true, "auto-submitted"
	case l.Precedence == "bulk" || l.Precedence == "list" || l.Precedence == "junk":
		return true, "precedence " + l.Precedence
	case l.ListID || l.ListUnsubscribe:
		return true, "mailing list"
	}
	switch localPart(l.From) {
	case "noreply", "no-reply", "mailer-daemon", "postmaster", "donotreply", "do-not-reply":
		return true, "service address"
	}
	return false, ""
}

// Authentic checks the top Authentication-Results of the provider and the
// domain (ML-07, ML-08); the reason explains a rejection.
func (l *Letter) Authentic(s Settings) (bool, string) {
	dom := Domain(l.From)
	if dom == "" {
		return false, "no sender"
	}
	var ar *AuthResults
	for _, h := range l.AuthResults {
		r := ParseAuthResults(h)
		if r.ServID != "" && servMatches(r.ServID, s.AuthservID) {
			ar = &r
			break
		}
	}
	if ar == nil {
		return false, "no Authentication-Results of " + s.AuthservID
	}
	ok := false
	for _, m := range ar.Methods {
		switch {
		case m.Method == "dmarc" && m.Result == "pass":
			from := m.Props["header.from"]
			if from == "" || strings.EqualFold(from, dom) {
				ok = true
			}
		case m.Method == "dkim" && m.Result == "pass":
			d := strings.ToLower(m.Props["header.d"])
			if d == "" {
				d = Domain(strings.TrimPrefix(m.Props["header.i"], "@"))
				if d == "" {
					d = strings.TrimPrefix(strings.ToLower(m.Props["header.i"]), "@")
				}
			}
			if d != "" && (d == dom || strings.HasSuffix(dom, "."+d)) {
				ok = true
			}
		}
	}
	if !ok {
		return false, "neither dmarc=pass nor dkim=pass for " + dom
	}
	if !s.Corporate(dom) {
		return false, "domain " + dom + " is not corporate"
	}
	return true, ""
}

func servMatches(got, want string) bool {
	got, want = strings.ToLower(got), strings.ToLower(want)
	return want != "" && (got == want || strings.HasSuffix(got, "."+want))
}

// Mode is direct when the bot is the only recipient: To holds only the
// mailbox or its aliases and Cc is empty (R8, ML-11, ML-12; tech §13 #2).
func (l *Letter) Mode(s Settings) string {
	if len(l.Cc) > 0 || len(l.To) == 0 {
		return "web_only"
	}
	for _, a := range l.To {
		if !s.Bot(a) {
			return "web_only"
		}
	}
	return "direct"
}

// IDs are the Message-IDs that may link a letter to a thread.
func (l *Letter) IDs() []string {
	seen := map[string]bool{}
	var out []string
	for _, id := range append(append([]string{}, l.InReplyTo...), l.References...) {
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

var subjectPrefix = regexp.MustCompile(`(?i)^\s*((re|fwd?|aw|wg|отв|ответ|пересл)\s*(\[\d+\])?\s*:\s*)+`)

// CleanSubject drops Re:/Fwd: prefixes.
func CleanSubject(s string) string {
	s = strings.TrimSpace(subjectPrefix.ReplaceAllString(s, ""))
	if s == "" {
		return "(no subject)"
	}
	return s
}
