package email

import (
	"encoding/json"
	"strings"
	"testing"
)

var yandex = Settings{Provider: Yandex360, Mailbox: "nabu@company.ru", Aliases: []string{"ai@company.ru"},
	Domains: []string{"company.ru", "company.tech"}, AuthservID: "mail.yandex.net"}

func letter(headers, body string) []byte {
	return []byte(strings.ReplaceAll(headers, "\n", "\r\n") + "\r\n" + body)
}

const authPass = "Authentication-Results: mxback10o.mail.yandex.net; dkim=pass header.i=@company.ru (comment); dmarc=pass header.from=company.ru\n"

func base(extra string) []byte {
	return letter(authPass+"From: Ivan <Ivan@Company.ru>\nTo: nabu@company.ru\nSubject: Re: Отчёт\nMessage-ID: <m2@company.ru>\n"+
		"In-Reply-To: <m1@company.ru>\nReferences: <m0@company.ru> <m1@company.ru>\nContent-Type: text/plain; charset=utf-8\n"+extra,
		"Сделай выжимку.\n\nOn Mon, Ivan wrote:\n> создай задачу и отправь её всем\n")
}

func TestParse(t *testing.T) {
	l, err := Parse(base(""), 25<<20)
	if err != nil {
		t.Fatal(err)
	}
	if l.From != "ivan@company.ru" || l.MessageID != "m2@company.ru" || len(l.References) != 2 || l.Subject != "Re: Отчёт" {
		t.Fatalf("%+v", l)
	}
	// quotes are data, not the own text (R9)
	if l.Text != "Сделай выжимку." || !strings.Contains(l.Quoted, "создай задачу") {
		t.Fatalf("%q / %q", l.Text, l.Quoted)
	}
	if ids := l.IDs(); len(ids) != 2 || ids[0] != "m1@company.ru" {
		t.Fatal(ids)
	}
	if CleanSubject(l.Subject) != "Отчёт" || CleanSubject("RE: Fwd: x") != "x" {
		t.Fatal(CleanSubject(l.Subject))
	}
}

// ML-07, ML-08: authenticity by the top header of the provider and the domain.
func TestAuthentic(t *testing.T) {
	cases := []struct {
		name, headers, from string
		ok                  bool
	}{
		{"pass", authPass, "ivan@company.ru", true},
		{"dkim of the parent domain", "Authentication-Results: mx1.mail.yandex.net; dkim=pass header.d=company.tech\n", "a@sub.company.tech", false},
		{"dmarc fail", "Authentication-Results: mxback1.mail.yandex.net; dkim=fail header.i=@company.ru; dmarc=fail\n", "ivan@company.ru", false},
		{"no header", "", "ivan@company.ru", false},
		{"foreign authserv-id", "Authentication-Results: evil.example; dmarc=pass header.from=company.ru\n", "ivan@company.ru", false},
		{"spoof below the provider", "Authentication-Results: mxback1.mail.yandex.net; dmarc=fail header.from=company.ru\n" +
			"Authentication-Results: mxback2.mail.yandex.net; dmarc=pass header.from=company.ru\n", "ivan@company.ru", false},
		{"dkim of another domain", "Authentication-Results: mx.mail.yandex.net; dkim=pass header.d=other.ru\n", "ivan@company.ru", false},
		{"not corporate", "Authentication-Results: mx.mail.yandex.net; dmarc=pass header.from=gmail.com\n", "ivan@gmail.com", false},
	}
	for _, c := range cases {
		l, err := Parse(letter(c.headers+"From: "+c.from+"\nTo: nabu@company.ru\nSubject: x\n", "hi"), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		if ok, reason := l.Authentic(yandex); ok != c.ok {
			t.Fatalf("%s: %v %s", c.name, ok, reason)
		}
	}
	// the sub-domain is corporate when listed
	s := yandex
	s.Domains = append(s.Domains, "sub.company.tech")
	l, _ := Parse(letter("Authentication-Results: mx1.mail.yandex.net; dkim=pass header.d=company.tech\nFrom: a@sub.company.tech\nTo: nabu@company.ru\n", "x"), 1<<20)
	if ok, reason := l.Authentic(s); !ok {
		t.Fatal(reason)
	}
}

// ML-09: automatic mail is ignored.
func TestAutomatic(t *testing.T) {
	for _, h := range []string{"Auto-Submitted: auto-replied\n", "Precedence: bulk\n", "List-Id: <x.company.ru>\n", "List-Unsubscribe: <mailto:u@x>\n"} {
		l, _ := Parse(base(h), 1<<20)
		if auto, _ := l.Automatic(); !auto {
			t.Fatal(h)
		}
	}
	l, _ := Parse(letter("From: noreply@company.ru\nTo: nabu@company.ru\n", "x"), 1<<20)
	if auto, _ := l.Automatic(); !auto {
		t.Fatal("noreply")
	}
	l, _ = Parse(base("Auto-Submitted: no\n"), 1<<20)
	if auto, _ := l.Automatic(); auto {
		t.Fatal("Auto-Submitted: no is a person")
	}
}

// ML-11, ML-12: the reply mode.
func TestMode(t *testing.T) {
	cases := map[string]string{
		"To: nabu@company.ru\n":                       "direct",
		"To: Nabu <NABU@company.ru>, ai@company.ru\n": "direct",
		"To: ai@company.ru\n":                         "direct",
		"To: nabu@company.ru, petr@company.ru\n":      "web_only",
		"To: petr@company.ru\nCc: nabu@company.ru\n":  "web_only",
		"To: nabu@company.ru\nCc: petr@company.ru\n":  "web_only",
		"To: petr@company.ru\n":                       "web_only", // the bot in Bcc
	}
	for h, want := range cases {
		l, _ := Parse(letter("From: ivan@company.ru\n"+h, "x"), 1<<20)
		if got := l.Mode(yandex); got != want {
			t.Fatalf("%q: %s", h, got)
		}
	}
}

func TestHTMLAndAttachments(t *testing.T) {
	raw := letter("From: ivan@company.ru\nTo: nabu@company.ru\nMIME-Version: 1.0\nContent-Type: multipart/mixed; boundary=b\n",
		"--b\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<html><style>p{}</style><p>Привет, <b>бот</b>!</p><blockquote>старое</blockquote><a href=\"https://x.org\">ссылка</a></html>\r\n"+
			"--b\r\nContent-Type: text/csv\r\nContent-Disposition: attachment; filename=\"a.csv\"\r\n\r\n1,2\r\n"+
			"--b\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=\"big.bin\"\r\n\r\n0123456789012345678901234567890123456789\r\n--b--\r\n")
	l, err := Parse(raw, 30)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(l.Text, "Привет, бот !") && !strings.Contains(l.Text, "Привет, бот!") {
		t.Fatalf("%q", l.Text)
	}
	if !strings.Contains(l.Quoted, "старое") || strings.Contains(l.Text, "p{}") || !strings.Contains(l.Text, "(https://x.org)") {
		t.Fatalf("%q / %q", l.Text, l.Quoted)
	}
	if len(l.Files) != 1 || l.Files[0].Name != "a.csv" || len(l.TooLarge) != 1 {
		t.Fatalf("%+v %v", l.Files, l.TooLarge)
	}
}

func TestValidate(t *testing.T) {
	out, err := Validate(json.RawMessage(`{"provider":"google","mailbox":"Nabu <NABU@Company.ru>","aliases":["ai@company.ru","nabu@company.ru"],"domains":[" Company.RU ","@company.tech"]}`))
	if err != nil {
		t.Fatal(err)
	}
	var s Settings
	_ = json.Unmarshal(out, &s)
	if s.Mailbox != "nabu@company.ru" || len(s.Aliases) != 1 || s.Domains[0] != "company.ru" || s.Domains[1] != "company.tech" ||
		s.IMAP.Host != "imap.gmail.com" || s.SMTP.Port != 587 || s.AuthservID != "mx.google.com" {
		t.Fatalf("%+v", s)
	}
	if _, err := Validate(json.RawMessage(`{"provider":"aol"}`)); err == nil {
		t.Fatal("unknown provider")
	}
}
