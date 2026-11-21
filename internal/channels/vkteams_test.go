package channels

import (
	"strings"
	"testing"

	"github.com/GreenOnGrey/nabu-core/internal/render"
)

// VOC-16: a file of VK Teams comes as an attachment, and the link of the file
// that the messenger puts into the text is dropped; a link of the user stays
// (FTR.NAB.CHAT-0001 R21).
func TestStripFileLinks(t *testing.T) {
	const id = "IdjUEXuGdNhLKUfD5rvkE03IOax54cD"
	for _, c := range []struct {
		name, text string
		ids        []string
		want       string
	}{
		{"the link of the file", "https://files.myteam.mail.ru/get/" + id + "?size=10", []string{id}, ""},
		{"the link of the file in a caption", "смотри https://files.myteam.mail.ru/get/" + id + " отчет", []string{id}, "смотри отчет"},
		{"the lines of a caption stay", "первая строка\nвторая https://files.myteam.mail.ru/get/" + id, []string{id}, "первая строка\nвторая"},
		{"the file service of another server", "https://files.corp.example/get/0R2vA8", []string{id}, ""},
		{"no attachments", "https://files.myteam.mail.ru/get/abc", nil, "https://files.myteam.mail.ru/get/abc"},
		{"an ordinary link", "см. https://example.com/doc", []string{id}, "см. https://example.com/doc"},
	} {
		if got := stripFileLinks(c.text, c.ids); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// VOC-16: the recording of a voice part goes to the recognizer by its type.
func TestVoiceFile(t *testing.T) {
	for _, c := range []struct{ name, want, mime string }{
		{"voice.ogg", "voice.ogg", "audio/ogg"},
		{"AUDIO.M4A", "AUDIO.M4A", "audio/mp4"},
		{"recording", "voice.ogg", "audio/ogg"},
		{"", "voice.ogg", "audio/ogg"},
	} {
		name, mime := voiceFile(c.name)
		if name != c.want || mime != c.mime {
			t.Errorf("%q: %q %q, want %q %q", c.name, name, mime, c.want, c.mime)
		}
	}
}

// SKL-12: the commands of the answer to /start of VK Teams are one per line:
// Markdown reads a single newline as a space, and the HTML of VK Teams reads a
// newline of the source as a space too (R6).
func TestHelpMarkdownLines(t *testing.T) {
	html := vkBreaks(strings.Join(render.HTMLParts(render.Parse(HelpMarkdown("ru")), VKTeamsLimit), "\n"))
	if n, want := strings.Count(html, "<br>"), len(commandList("ru")); n != want {
		t.Fatalf("line breaks = %d, want %d: %s", n, want, html)
	}
}
