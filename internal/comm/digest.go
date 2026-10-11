package comm

import (
	"encoding/json"
	"fmt"
	"strings"
)

// DigestCard is the card of a digest message (tech §3.5): the web chat draws
// it with checkboxes; the state of an item is read when the card is shown.
type DigestCard struct {
	Type   string           `json:"type"`   // digest
	Period string           `json:"period"` // day | week
	Items  []DigestCardItem `json:"items"`
}

// DigestCardItem is a commitment of the card.
type DigestCardItem struct {
	CommitmentID string `json:"commitmentId"`
	Text         string `json:"text"`
	Direction    string `json:"direction"`
	Group        string `json:"group"`
	Person       *Tag   `json:"person"`
	Topics       []Tag  `json:"topics"`
	Due          *Date  `json:"due"`
	State        string `json:"state"`
	NoteID       string `json:"noteId"`
	URL          string `json:"url"`
}

var digestTexts = map[string]map[string]string{
	"ru": {"day": "Не забыть сегодня", "week": "Не забыть на этой неделе", GroupOverdue: "Просрочено", GroupDue + "day": "Сегодня", GroupDue + "week": "На этой неделе",
		GroupTheirs: "Мне должны", GroupUndated: "Без срока", "source": "источник", "until": "до", "hint": "Ответьте, что сделано: «1 и 2 сделал».",
		"titleday": "Сводка на день", "titleweek": "Сводка на неделю"},
	"en": {"day": "Do not forget today", "week": "Do not forget this week", GroupOverdue: "Overdue", GroupDue + "day": "Today", GroupDue + "week": "This week",
		GroupTheirs: "Owed to me", GroupUndated: "No due date", "source": "source", "until": "by", "hint": "Reply with what is done: “1 and 2 are done”.",
		"titleday": "Daily digest", "titleweek": "Weekly digest"},
}

func digestLang(lang string) map[string]string {
	if t, ok := digestTexts[lang]; ok {
		return t
	}
	return digestTexts["en"]
}

// DigestTitle is the title of the digest in the language of the user.
func DigestTitle(period, lang string) string { return digestLang(lang)["title"+period] }

// RenderDigest builds the message of a digest (R16): the text — a numbered
// list by groups with links to the sources, for messengers, mail and clients
// that do not draw cards — and the card for the web chat. The numbers of the
// text are what the user answers with («2 сделал»).
func RenderDigest(items []DigestItem, period, lang string) (string, json.RawMessage) {
	t := digestLang(lang)
	var b strings.Builder
	b.WriteString("**" + t[period] + "**\n")
	card := DigestCard{Type: "digest", Period: period, Items: make([]DigestCardItem, 0, len(items))}
	group := ""
	for i, it := range items {
		if it.Group != group {
			group = it.Group
			title := t[group]
			if group == GroupDue {
				title = t[GroupDue+period]
			}
			b.WriteString("\n**" + title + "**\n")
		}
		fmt.Fprintf(&b, "%d. %s", i+1, it.Text)
		var tail []string
		if it.Person != nil && it.Person.Label != "" {
			tail = append(tail, it.Person.Label)
		}
		if len(it.Topics) > 0 { // R16: the person, the topic and the source of every item
			tail = append(tail, it.Topics[0].Label)
		}
		if it.Due != nil {
			tail = append(tail, t["until"]+" "+it.Due.Format("02.01"))
		}
		if len(tail) > 0 {
			b.WriteString(" — " + strings.Join(tail, ", "))
		}
		if it.BasisURL != "" {
			b.WriteString(" · [" + t["source"] + "](" + it.BasisURL + ")")
		}
		b.WriteString("\n")
		card.Items = append(card.Items, DigestCardItem{CommitmentID: it.ID.String(), Text: it.Text, Direction: it.Direction, Group: it.Group,
			Person: it.Person, Topics: it.Topics, Due: it.Due, State: it.State, NoteID: it.NoteID.String(), URL: it.BasisURL})
	}
	b.WriteString("\n_" + t["hint"] + "_")
	raw, _ := json.Marshal(card)
	return b.String(), raw
}
