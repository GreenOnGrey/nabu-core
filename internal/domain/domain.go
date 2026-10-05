// Package domain holds the vocabulary shared by all slices of Nabu: the
// authenticated principal, channels, tones and languages (FTR.NAB.CMN-0001).
package domain

import (
	"strings"

	"github.com/google/uuid"
)

// Principal is the user a request acts for. Site requests carry the session
// user; client requests with delegation carry the delegated user and the
// client (FTR.NAB.CMN-0001 R1a).
type Principal struct {
	UserID  uuid.UUID
	Email   string
	Name    string
	IsAdmin bool
	Blocked bool
	// ClientID is set when a service client acts on behalf of the user.
	ClientID   *uuid.UUID
	ClientName string
}

// Channel is where a message came from: web, telegram, vkws, client:<id> or task:<id>.
func (p *Principal) Channel() string {
	if p.ClientID != nil {
		return "client:" + p.ClientName
	}
	return ChannelWeb
}

// Channels.
const (
	ChannelWeb      = "web"
	ChannelTelegram = "telegram"
	ChannelVKWS     = "vkws"
)

// Messenger reports whether the channel is a linked messenger.
func Messenger(ch string) bool { return ch == ChannelTelegram || ch == ChannelVKWS }

// Tones of the personal agent (tech spec §3.1).
var Tones = []string{"business", "friendly", "brief", "mentor"}

// ValidTone reports whether t is a known tone.
func ValidTone(t string) bool { return contains(Tones, t) }

// Languages of the interface; zh is shown as ZH (R31).
var Languages = []string{"en", "ru", "de", "es", "zh"}

// ValidLanguage reports whether l is offered.
func ValidLanguage(l string) bool { return contains(Languages, l) }

// Themes of the interface.
var Themes = []string{"light", "dark", "system"}

// ValidTheme reports whether t is a known theme.
func ValidTheme(t string) bool { return contains(Themes, t) }

// NormalizeEmail lower-cases and trims an email (users.email is stored so).
func NormalizeEmail(e string) string { return strings.ToLower(strings.TrimSpace(e)) }

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
