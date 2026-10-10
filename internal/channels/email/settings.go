// Package email is the mail channel of Nabu (FTR.NAB.CMN-0002 R6–R11; arch
// §3; tech §5): the mailbox of the bot at Google Workspace, Yandex 360 or VK
// WorkMail read over IMAP with IDLE, letters checked for authenticity by
// the header of the provider, threads turned into topics, answers sent over
// SMTP to the sender only.
package email

import (
	"encoding/json"
	"net/mail"
	"slices"
	"strings"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
)

// Providers.
const (
	Google     = "google"
	Yandex360  = "yandex360"
	VKWorkMail = "vkworkmail"
)

// Endpoint is a host and a port.
type Endpoint struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// Settings are the settings of the mail channel (tech §2.1).
type Settings struct {
	Provider string   `json:"provider"`
	Mailbox  string   `json:"mailbox"`
	Aliases  []string `json:"aliases"`
	Domains  []string `json:"domains"`
	// AuthservID is the authserv-id of the Authentication-Results header
	// added by the provider; a host ending with "."+AuthservID matches too
	// (Yandex adds the name of the receiving server).
	AuthservID string   `json:"authservId"`
	IMAP       Endpoint `json:"imap"`
	SMTP       Endpoint `json:"smtp"`
}

// Secrets of the mail channel.
const (
	SecretAppPassword    = "appPassword"
	SecretServiceAccount = "serviceAccountJson"
)

// Presets fill IMAP, SMTP and the authserv-id by the provider (arch §3.1).
// The authserv-id values were checked against letters of each provider; the
// administrator may change them in the settings.
var Presets = map[string]Settings{
	Google:     {IMAP: Endpoint{"imap.gmail.com", 993}, SMTP: Endpoint{"smtp.gmail.com", 587}, AuthservID: "mx.google.com"},
	Yandex360:  {IMAP: Endpoint{"imap.yandex.ru", 993}, SMTP: Endpoint{"smtp.yandex.ru", 465}, AuthservID: "mail.yandex.net"},
	VKWorkMail: {IMAP: Endpoint{"imap.mail.ru", 993}, SMTP: Endpoint{"smtp.mail.ru", 465}, AuthservID: "mail.ru"},
}

func invalid(field, msg string) error {
	return apperr.Unprocessable("invalid_settings", msg).With("field", field)
}

func lowerAddr(s string) (string, bool) {
	a, err := mail.ParseAddress(strings.TrimSpace(s))
	if err != nil {
		return "", false
	}
	return strings.ToLower(a.Address), true
}

// Validate normalizes the settings and fills the preset of the provider.
func Validate(raw json.RawMessage) (json.RawMessage, error) {
	var s Settings
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, invalid("settings", "the settings are not valid")
	}
	p, ok := Presets[s.Provider]
	if !ok {
		return nil, invalid("provider", "the provider is google, yandex360 or vkworkmail")
	}
	if s.Mailbox != "" {
		m, ok := lowerAddr(s.Mailbox)
		if !ok {
			return nil, invalid("mailbox", "the mailbox is not an email address")
		}
		s.Mailbox = m
	}
	var aliases []string
	for _, a := range s.Aliases {
		m, ok := lowerAddr(a)
		if !ok {
			return nil, invalid("aliases", "an alias is not an email address")
		}
		if m != s.Mailbox && !slices.Contains(aliases, m) {
			aliases = append(aliases, m)
		}
	}
	s.Aliases = aliases
	var domains []string
	for _, d := range s.Domains {
		d = strings.Trim(strings.ToLower(strings.TrimSpace(d)), "@.")
		if d == "" {
			continue
		}
		if strings.ContainsAny(d, " /@") || !strings.Contains(d, ".") {
			return nil, invalid("domains", "a domain is not valid")
		}
		if !slices.Contains(domains, d) {
			domains = append(domains, d)
		}
	}
	s.Domains = domains
	if s.IMAP.Host == "" {
		s.IMAP = p.IMAP
	}
	if s.SMTP.Host == "" {
		s.SMTP = p.SMTP
	}
	if s.AuthservID == "" {
		s.AuthservID = p.AuthservID
	}
	s.AuthservID = strings.ToLower(strings.TrimSpace(s.AuthservID))
	return json.Marshal(s)
}

// Bot reports whether the address is the mailbox of the bot or its alias.
func (s Settings) Bot(addr string) bool {
	addr = strings.ToLower(addr)
	return addr == s.Mailbox || slices.Contains(s.Aliases, addr)
}

// Corporate reports whether the domain is in the list of the channel.
func (s Settings) Corporate(domain string) bool {
	return slices.Contains(s.Domains, strings.ToLower(domain))
}
