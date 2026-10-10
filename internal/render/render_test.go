package render

import (
	"encoding/json"
	"strings"
	"testing"
)

const answer = "## Итоги\n\nКоманда закрыла **12** задач, см. [отчёт](https://x.org/r).\n\n" +
	"| Сервис | Ошибки |\n|:--|--:|\n| api | 3 |\n| worker | 10 |\n\n" +
	"- первый\n- второй с `code`\n\n```go\nfmt.Println(1)\n```\n\n> цитата\n"

func TestParse(t *testing.T) {
	bs := Parse(answer)
	kinds := []string{}
	for _, b := range bs {
		kinds = append(kinds, b.Kind)
	}
	if got := strings.Join(kinds, ","); got != "heading,paragraph,table,list,code,quote" {
		t.Fatal(got)
	}
	tb := bs[2]
	if len(tb.Rows) != 3 || !tb.Rows[0][0].Header || tb.Align[1] != "right" || PlainInline(tb.Rows[2][0].Inline) != "worker" {
		t.Fatalf("%+v", tb)
	}
}

// TG-08: a table becomes a native table of a rich message.
func TestTelegramRich(t *testing.T) {
	msgs := TelegramRich(Parse(answer), 8)
	if len(msgs) != 1 {
		t.Fatal(len(msgs))
	}
	b, _ := json.Marshal(msgs[0])
	s := string(b)
	for _, want := range []string{`"type":"heading"`, `"size":2`, `"type":"table"`, `"is_header":true`, `"align":"right"`,
		`"type":"url"`, `"url":"https://x.org/r"`, `"type":"pre"`, `"language":"go"`, `"type":"list"`, `"type":"blockquote"`, `"type":"bold"`} {
		if !strings.Contains(s, want) {
			t.Fatalf("%s not in %s", want, s)
		}
	}
	// a table wider than TG_TABLE_MAX_COLS goes as a code block
	wide := Parse("|a|b|c|\n|-|-|-|\n|1|2|3|\n")
	b, _ = json.Marshal(TelegramRich(wide, 2))
	if !strings.Contains(string(b), `"type":"pre"`) || strings.Contains(string(b), `"type":"table"`) {
		t.Fatal(string(b))
	}
}

func TestTelegramRichSplits(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < 600; i++ {
		sb.WriteString("paragraph\n\n")
	}
	msgs := TelegramRich(Parse(sb.String()), 8)
	if len(msgs) != 2 || len(msgs[0]) != richMaxBlocks {
		t.Fatal(len(msgs), len(msgs[0]))
	}
}

// VK-02: HTML markup, a table as a pre block.
func TestHTML(t *testing.T) {
	h := HTML(Parse(answer))
	for _, want := range []string{"<b>Итоги</b>", "<b>12</b>", `<a href="https://x.org/r">отчёт</a>`, "<pre>Сервис │ Ошибки",
		"• первый", "<code>code</code>", `<pre><code class="language-go">fmt.Println(1)</code></pre>`, "<blockquote>цитата</blockquote>"} {
		if !strings.Contains(h, want) {
			t.Fatalf("%q not in\n%s", want, h)
		}
	}
	if strings.Contains(HTML(Parse("a <script>x</script> & b")), "<script>") {
		t.Fatal("not escaped")
	}
}

func TestHTMLPartsLimit(t *testing.T) {
	code := "```\n" + strings.Repeat("line of code\n", 400) + "```"
	parts := HTMLParts(Parse("intro\n\n"+code), 1000)
	if len(parts) < 5 {
		t.Fatal(len(parts))
	}
	for _, p := range parts {
		if len([]rune(p)) > 1000 {
			t.Fatal(len(p))
		}
		if strings.Count(p, "<pre>") != strings.Count(p, "</pre>") {
			t.Fatal("broken tags", p)
		}
	}
}

// ML-16: a letter with an HTML table and a text version.
func TestEmail(t *testing.T) {
	h, txt := Email(Parse(answer))
	for _, want := range []string{"<table", "<th ", "text-align:right", "<pre style=", "<blockquote"} {
		if !strings.Contains(h, want) {
			t.Fatalf("%q not in %s", want, h)
		}
	}
	for _, want := range []string{"Итоги\n=====", "worker │     10", "• первый", "отчёт (https://x.org/r)", "> цитата"} {
		if !strings.Contains(txt, want) {
			t.Fatalf("%q not in\n%s", want, txt)
		}
	}
}
