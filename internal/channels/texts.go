package channels

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// texts are the service messages of the bots and letters (the user's
// language when known, otherwise the language of the account).
var texts = map[string]map[string]string{
	"en": {
		"tg.send_key":         "Send me your personal Nabu key to link this Telegram account. Get it in Nabu: %s → Telegram → Get key.",
		"tg.wrong_key":        "The key is not valid. Check it or reissue it in Nabu → Connections → Telegram.",
		"tg.blocked":          "Too many wrong keys. Try again in an hour.",
		"tg.bound":            "Telegram is linked to your Nabu account. Write to your agent here.",
		"tg.unbound":          "This Telegram account was unlinked from Nabu: another account was linked to the same user.",
		"tg.ready":            "You are linked. Write to your agent here.",
		"tg.not_delivered":    "The message could not be delivered to the agent, try again later.",
		"tg.too_large":        "The file is too large for Nabu.",
		"channel.unavailable": "The channel is not available for your account. Contact your administrator.",
		"group.not_linked":    "Only a Nabu user with access to this channel can add the bot. Link your account in a private chat with the bot first. Leaving the group.",
		"group.disabled":      "Group chats are switched off for this channel in Nabu. Leaving the group.",
		"group.unavailable":   "The channel is not available for your Nabu account. Leaving the group.",
		"group.welcome":       "Hello! I am the group agent of this chat. Mention me or reply to my message to ask something. Owner: %s.",
		"group.not_member":    "I answer only participants with a Nabu account and access to this channel.",
		"group.inactive":      "The group agent is switched off.",
		"mail.unavailable":    "The channel is not available for your account. Contact your administrator.",
		"mail.open":           "Open the conversation in Nabu",
		"mail.confirm":        "Confirm in Nabu",
	},
	"ru": {
		"tg.send_key":         "Пришлите личный ключ Nabu, чтобы привязать этот аккаунт Telegram. Ключ выдаётся в Nabu: %s → Telegram → «Получить ключ».",
		"tg.wrong_key":        "Ключ не подходит. Проверьте его или перевыпустите в Nabu → «Подключения» → Telegram.",
		"tg.blocked":          "Слишком много неверных ключей. Попробуйте через час.",
		"tg.bound":            "Telegram привязан к вашей учётной записи Nabu. Пишите агенту здесь.",
		"tg.unbound":          "Этот аккаунт Telegram отвязан от Nabu: к той же учётной записи привязан другой аккаунт.",
		"tg.ready":            "Аккаунт привязан. Пишите агенту здесь.",
		"tg.not_delivered":    "Сообщение не удалось передать агенту, попробуйте позже.",
		"tg.too_large":        "Файл слишком большой для Nabu.",
		"channel.unavailable": "Канал недоступен для вашей учётной записи, обратитесь к администратору.",
		"group.not_linked":    "Добавить бота может только пользователь Nabu с доступом к этому каналу. Сначала привяжите аккаунт в личном чате с ботом. Выхожу из группы.",
		"group.disabled":      "Групповые чаты выключены для этого канала в Nabu. Выхожу из группы.",
		"group.unavailable":   "Канал недоступен для вашей учётной записи Nabu. Выхожу из группы.",
		"group.welcome":       "Здравствуйте! Я групповой агент этого чата. Упомяните меня или ответьте на моё сообщение, чтобы спросить. Владелец: %s.",
		"group.not_member":    "Я отвечаю только участникам с учётной записью Nabu и доступом к этому каналу.",
		"group.inactive":      "Групповой агент отключён.",
		"mail.unavailable":    "Канал недоступен для вашей учётной записи, обратитесь к администратору.",
		"mail.open":           "Открыть разговор в Nabu",
		"mail.confirm":        "Подтвердить в Nabu",
	},
}

// T returns a service text in the language (ru for ru, en otherwise).
func T(lang, key string, args ...any) string {
	m := texts["en"]
	if strings.HasPrefix(strings.ToLower(lang), "ru") {
		m = texts["ru"]
	}
	t, ok := m[key]
	if !ok {
		t = texts["en"][key]
	}
	if len(args) > 0 {
		return fmt.Sprintf(t, args...)
	}
	return t
}

// ─── group agents (R16–R17) ─────────────────────────────────────────

// GroupEvent is the bot added to a group.
type GroupEvent struct {
	Channel, ChatID, Title string
	// AdderID is the Nabu user who added the bot (uuid.Nil — not linked).
	AdderID  uuid.UUID
	Members  int
	Language string
}

// GroupMessage is an address to the bot in a group.
type GroupMessage struct {
	Channel, ChatID, Title string
	// AuthorID is the Nabu user of the author (uuid.Nil — unknown).
	AuthorID   uuid.UUID
	AuthorName string
	Text       string
	// Quote is the quoted message (stored with the address).
	Quote    string
	Language string
}

// Groups is the group agents slice.
type Groups interface {
	// Added creates or reactivates the group agent; false — the bot leaves
	// with the message.
	Added(ctx context.Context, ev GroupEvent) (message string, ok bool)
	Removed(ctx context.Context, channel, chatID string)
	// Message passes an address to the agent; a non-empty reply is a refusal.
	Message(ctx context.Context, m GroupMessage) (reply string)
}
