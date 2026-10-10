package render

import (
	"fmt"
	"html"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ─── Telegram rich messages (tech §9; Bot API InputRichMessage) ────────

// Rich-message limits of the Bot API with a margin.
const (
	richMaxBlocks = 450
	richMaxChars  = 30000
)

// TelegramRich builds the blocks of rich messages: one message per element,
// split by block boundaries within the Bot API limits. Tables wider than
// maxCols go as a code block (TG_TABLE_MAX_COLS).
func TelegramRich(bs []Block, maxCols int) [][]any {
	var out [][]any
	var cur []any
	n, chars := 0, 0
	for _, b := range bs {
		rb, cnt, size := richBlock(b, maxCols)
		if len(cur) > 0 && (n+cnt > richMaxBlocks || chars+size > richMaxChars) {
			out = append(out, cur)
			cur, n, chars = nil, 0, 0
		}
		cur = append(cur, rb)
		n += cnt
		chars += size
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

func richText(in []Inline) any {
	var parts []any
	for _, s := range in {
		switch s.Kind {
		case "text":
			parts = append(parts, s.Text)
		case "break":
			parts = append(parts, "\n")
		case "code":
			parts = append(parts, map[string]any{"type": "code", "text": s.Text})
		case "bold", "italic":
			parts = append(parts, map[string]any{"type": s.Kind, "text": richText(s.Children)})
		case "strike":
			parts = append(parts, map[string]any{"type": "strikethrough", "text": richText(s.Children)})
		case "link":
			if strings.HasPrefix(s.URL, "http://") || strings.HasPrefix(s.URL, "https://") {
				parts = append(parts, map[string]any{"type": "url", "text": richText(s.Children), "url": s.URL})
			} else {
				parts = append(parts, PlainInline([]Inline{s}))
			}
		}
	}
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	}
	return parts
}

func richBlock(b Block, maxCols int) (any, int, int) {
	switch b.Kind {
	case "heading":
		size := b.Level
		if size > 6 {
			size = 6
		}
		return map[string]any{"type": "heading", "text": richText(b.Inline), "size": size}, 1, len(PlainInline(b.Inline))
	case "code":
		m := map[string]any{"type": "pre", "text": b.Code}
		if b.Lang != "" {
			m["language"] = b.Lang
		}
		return m, 1, len(b.Code)
	case "divider":
		return map[string]any{"type": "divider"}, 1, 0
	case "quote":
		var inner []any
		cnt, size := 1, 0
		for _, c := range b.Children {
			rb, n, s := richBlock(c, maxCols)
			inner = append(inner, rb)
			cnt += n
			size += s
		}
		return map[string]any{"type": "blockquote", "blocks": inner}, cnt, size
	case "list":
		var items []any
		cnt, size := 1, 0
		for i, it := range b.Items {
			var inner []any
			for _, c := range it {
				rb, n, s := richBlock(c, maxCols)
				inner = append(inner, rb)
				cnt += n
				size += s
			}
			item := map[string]any{"blocks": inner}
			if b.Checked[i] != nil {
				item["has_checkbox"] = true
				if *b.Checked[i] {
					item["is_checked"] = true
				}
			}
			if b.Ordered {
				item["type"] = "1"
				item["value"] = b.Start + i
			}
			items = append(items, item)
			cnt++
		}
		return map[string]any{"type": "list", "items": items}, cnt, size
	case "table":
		cols := 0
		for _, r := range b.Rows {
			if len(r) > cols {
				cols = len(r)
			}
		}
		if cols > maxCols {
			t := TableText(b)
			return map[string]any{"type": "pre", "text": t}, 1, len(t)
		}
		var rows []any
		size := 0
		for _, r := range b.Rows {
			var cells []any
			for i, c := range r {
				align := "left"
				if i < len(b.Align) && b.Align[i] != "" {
					align = b.Align[i]
				}
				cell := map[string]any{"text": richText(c.Inline), "align": align, "valign": "top"}
				if c.Header {
					cell["is_header"] = true
				}
				cells = append(cells, cell)
				size += len(PlainInline(c.Inline))
			}
			rows = append(rows, cells)
		}
		return map[string]any{"type": "table", "cells": rows, "is_bordered": true}, 1 + len(b.Rows), size
	}
	return map[string]any{"type": "paragraph", "text": richText(b.Inline)}, 1, len(PlainInline(b.Inline))
}

// ─── the HTML subset of Telegram and VK Teams ───────────────────────

// HTMLParts renders blocks as the HTML of messengers (b, i, s, code, pre,
// a, blockquote; lists with symbols; tables as aligned pre) split into
// messages of at most limit characters by block boundaries; a block longer
// than limit is cut by lines, then words.
func HTMLParts(bs []Block, limit int) []string {
	var parts []string
	var cur strings.Builder
	flush := func() {
		if s := strings.TrimSpace(cur.String()); s != "" {
			parts = append(parts, s)
		}
		cur.Reset()
	}
	for _, b := range bs {
		for _, whole := range htmlBlock(b, limit) {
			pieces := []string{whole}
			if utf8.RuneCountInString(whole) > limit {
				pieces = splitHTML(whole, limit)
			}
			for _, piece := range pieces {
				if cur.Len() > 0 && utf8.RuneCountInString(cur.String())+utf8.RuneCountInString(piece)+2 > limit {
					flush()
				}
				if cur.Len() > 0 {
					cur.WriteString("\n\n")
				}
				cur.WriteString(piece)
			}
		}
	}
	flush()
	return parts
}

// HTML renders all blocks as the messenger HTML in one string.
func HTML(bs []Block) string { return strings.Join(HTMLParts(bs, 1<<30), "\n\n") }

type htmlToken struct {
	s    string
	open string // the name of an opening tag
	end  string // the name of a closing tag
}

// htmlTokens cuts the HTML of htmlBlock into tags, entities and characters.
func htmlTokens(s string) []htmlToken {
	var out []htmlToken
	for len(s) > 0 {
		switch s[0] {
		case '<':
			if i := strings.IndexByte(s, '>'); i > 0 {
				t := htmlToken{s: s[:i+1]}
				if s[1] == '/' {
					t.end = s[2:i]
				} else {
					t.open, _, _ = strings.Cut(s[1:i], " ")
				}
				out, s = append(out, t), s[i+1:]
				continue
			}
		case '&':
			if i := strings.IndexByte(s, ';'); i > 0 && i <= 8 {
				out, s = append(out, htmlToken{s: s[:i+1]}), s[i+1:]
				continue
			}
		}
		_, n := utf8.DecodeRuneInString(s)
		out, s = append(out, htmlToken{s: s[:n]}), s[n:]
	}
	return out
}

// StripHTML is the plain text of a part of HTMLParts.
func StripHTML(s string) string {
	var b strings.Builder
	for _, t := range htmlTokens(s) {
		if t.open == "" && t.end == "" {
			b.WriteString(t.s)
		}
	}
	return html.UnescapeString(b.String())
}

// splitHTML cuts a piece into parts of at most limit characters at a line
// end, else at a space, else anywhere outside tags and entities; the tags
// open at a cut are closed there and opened again in the next part.
func splitHTML(s string, limit int) []string {
	toks := htmlTokens(s)
	var parts []string
	var stack []htmlToken // the tags open at the start of the part
	for len(toks) > 0 {
		var b strings.Builder
		size := 0
		for _, t := range stack {
			b.WriteString(t.s)
			size += utf8.RuneCountInString(t.s)
		}
		head := b.Len()
		open := append([]htmlToken{}, stack...)
		closing := func(open []htmlToken) (string, int) {
			var c strings.Builder
			for i := len(open) - 1; i >= 0; i-- {
				c.WriteString("</" + open[i].open + ">")
			}
			return c.String(), utf8.RuneCountInString(c.String())
		}
		// the last line end and the last space that still fit: the number of
		// tokens taken, the length of the text and the tags open there
		type point struct {
			n, len int
			open   []htmlToken
		}
		var line, space point
		n := 0
		for ; n < len(toks); n++ {
			t := toks[n]
			next := open
			switch {
			case t.open != "":
				next = append(append([]htmlToken{}, open...), t)
			case t.end != "" && len(open) > 0:
				next = open[:len(open)-1]
			}
			_, tail := closing(next)
			if size+utf8.RuneCountInString(t.s)+tail > limit && b.Len() > head {
				break
			}
			b.WriteString(t.s)
			size += utf8.RuneCountInString(t.s)
			open = next
			switch t.s {
			case "\n":
				line = point{n + 1, b.Len(), open}
			case " ":
				space = point{n + 1, b.Len(), open}
			}
		}
		text := b.String()
		if n < len(toks) {
			for _, p := range []point{line, space} {
				if p.n > 0 {
					n, text, open = p.n, text[:p.len], p.open
					break
				}
			}
		}
		tail, _ := closing(open)
		if part := strings.TrimSpace(text[head:]); part != "" {
			parts = append(parts, strings.TrimRight(text, " \n")+tail)
		}
		toks, stack = toks[n:], open
	}
	return parts
}

func esc(s string) string { return html.EscapeString(s) }

func htmlInline(in []Inline) string {
	var b strings.Builder
	for _, s := range in {
		switch s.Kind {
		case "text":
			b.WriteString(esc(s.Text))
		case "break":
			b.WriteByte('\n')
		case "code":
			b.WriteString("<code>" + esc(s.Text) + "</code>")
		case "bold":
			b.WriteString("<b>" + htmlInline(s.Children) + "</b>")
		case "italic":
			b.WriteString("<i>" + htmlInline(s.Children) + "</i>")
		case "strike":
			b.WriteString("<s>" + htmlInline(s.Children) + "</s>")
		case "link":
			b.WriteString(`<a href="` + esc(s.URL) + `">` + htmlInline(s.Children) + "</a>")
		}
	}
	return b.String()
}

// htmlBlock renders a block; a code block longer than limit is cut by lines
// into several pre blocks.
func htmlBlock(b Block, limit int) []string {
	switch b.Kind {
	case "heading":
		return []string{"<b>" + htmlInline(b.Inline) + "</b>"}
	case "code":
		return preChunks(b.Code, b.Lang, limit)
	case "table":
		return preChunks(TableText(b), "", limit)
	case "divider":
		return []string{"——————"}
	case "quote":
		var inner []string
		for _, c := range b.Children {
			inner = append(inner, htmlBlock(c, limit)...)
		}
		return []string{"<blockquote>" + strings.Join(inner, "\n") + "</blockquote>"}
	case "list":
		var lines []string
		for i, it := range b.Items {
			var inner []string
			for _, c := range it {
				inner = append(inner, htmlBlock(c, limit)...)
			}
			lines = append(lines, marker(b, i)+strings.Join(inner, "\n"))
		}
		return []string{strings.Join(lines, "\n")}
	}
	return []string{htmlInline(b.Inline)}
}

func marker(b Block, i int) string {
	if c := b.Checked[i]; c != nil {
		if *c {
			return "☑ "
		}
		return "☐ "
	}
	if b.Ordered {
		return strconv.Itoa(b.Start+i) + ". "
	}
	return "• "
}

func preChunks(code, lang string, limit int) []string {
	open, close := "<pre>", "</pre>"
	if lang != "" {
		open, close = `<pre><code class="language-`+esc(lang)+`">`, "</code></pre>"
	}
	max := limit - len(open) - len(close) - 16
	var out []string
	var cur strings.Builder
	for _, l := range strings.Split(code, "\n") {
		el := esc(l)
		if cur.Len() > 0 && utf8.RuneCountInString(cur.String())+utf8.RuneCountInString(el)+1 > max {
			out = append(out, open+strings.TrimRight(cur.String(), "\n")+close)
			cur.Reset()
		}
		cur.WriteString(el + "\n")
	}
	out = append(out, open+strings.TrimRight(cur.String(), "\n")+close)
	return out
}

// TableText draws a table as aligned text.
func TableText(b Block) string {
	cols := 0
	for _, r := range b.Rows {
		if len(r) > cols {
			cols = len(r)
		}
	}
	width := make([]int, cols)
	cells := make([][]string, len(b.Rows))
	for i, r := range b.Rows {
		cells[i] = make([]string, cols)
		for j, c := range r {
			t := strings.ReplaceAll(PlainInline(c.Inline), "\n", " ")
			cells[i][j] = t
			if w := utf8.RuneCountInString(t); w > width[j] {
				width[j] = w
			}
		}
	}
	var out strings.Builder
	for i, r := range cells {
		for j, t := range r {
			if j > 0 {
				out.WriteString(" │ ")
			}
			pad := width[j] - utf8.RuneCountInString(t)
			if j < len(b.Align) && b.Align[j] == "right" {
				out.WriteString(strings.Repeat(" ", pad) + t)
			} else {
				out.WriteString(t + strings.Repeat(" ", pad))
			}
		}
		out.WriteString("\n")
		if i == 0 && len(b.Rows) > 1 && len(b.Rows[0]) > 0 && b.Rows[0][0].Header {
			for j := range width {
				if j > 0 {
					out.WriteString("─┼─")
				}
				out.WriteString(strings.Repeat("─", width[j]))
			}
			out.WriteString("\n")
		}
	}
	return strings.TrimRight(out.String(), "\n")
}

// ─── letters (tech §9: HTML with inline styles and text/plain) ─────────

// Email renders a letter: the HTML body with inline styles and its text version.
func Email(bs []Block) (htmlBody, textBody string) {
	var h strings.Builder
	for _, b := range bs {
		h.WriteString(emailBlock(b))
	}
	return h.String(), Text(bs)
}

const (
	stCode  = `font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:13px;`
	stPre   = `background:#f4f4f7;border-radius:6px;padding:10px 12px;overflow-x:auto;white-space:pre;` + stCode
	stTable = `border-collapse:collapse;margin:8px 0;`
	stCell  = `border:1px solid #d9d9e3;padding:6px 10px;text-align:left;vertical-align:top;`
	stQuote = `border-left:3px solid #c9c4f2;margin:8px 0;padding:2px 12px;color:#55556a;`
)

func emailInline(in []Inline) string {
	var b strings.Builder
	for _, s := range in {
		switch s.Kind {
		case "text":
			b.WriteString(esc(s.Text))
		case "break":
			b.WriteString("<br>")
		case "code":
			b.WriteString(`<code style="background:#f4f4f7;border-radius:4px;padding:1px 4px;` + stCode + `">` + esc(s.Text) + "</code>")
		case "bold":
			b.WriteString("<strong>" + emailInline(s.Children) + "</strong>")
		case "italic":
			b.WriteString("<em>" + emailInline(s.Children) + "</em>")
		case "strike":
			b.WriteString("<s>" + emailInline(s.Children) + "</s>")
		case "link":
			b.WriteString(`<a href="` + esc(s.URL) + `" style="color:#6d5bd0;">` + emailInline(s.Children) + "</a>")
		}
	}
	return b.String()
}

func emailBlock(b Block) string {
	switch b.Kind {
	case "heading":
		size := map[int]int{1: 22, 2: 19, 3: 17}[b.Level]
		if size == 0 {
			size = 15
		}
		return fmt.Sprintf(`<p style="font-size:%dpx;font-weight:600;margin:16px 0 8px;">%s</p>`, size, emailInline(b.Inline))
	case "code":
		return `<pre style="` + stPre + `">` + esc(b.Code) + "</pre>"
	case "divider":
		return `<hr style="border:none;border-top:1px solid #e3e3ea;margin:16px 0;">`
	case "quote":
		var inner strings.Builder
		for _, c := range b.Children {
			inner.WriteString(emailBlock(c))
		}
		return `<blockquote style="` + stQuote + `">` + inner.String() + "</blockquote>"
	case "list":
		tag := "ul"
		if b.Ordered {
			tag = "ol"
		}
		var out strings.Builder
		out.WriteString("<" + tag + ` style="margin:8px 0;padding-left:24px;">`)
		for i, it := range b.Items {
			var inner strings.Builder
			for _, c := range it {
				if c.Kind == "paragraph" {
					inner.WriteString(emailInline(c.Inline))
				} else {
					inner.WriteString(emailBlock(c))
				}
			}
			prefix := ""
			if c := b.Checked[i]; c != nil {
				prefix = "☐ "
				if *c {
					prefix = "☑ "
				}
			}
			out.WriteString(`<li style="margin:2px 0;">` + prefix + inner.String() + "</li>")
		}
		out.WriteString("</" + tag + ">")
		return out.String()
	case "table":
		var out strings.Builder
		out.WriteString(`<table style="` + stTable + `">`)
		for _, r := range b.Rows {
			out.WriteString("<tr>")
			for j, c := range r {
				tag, st := "td", stCell
				if c.Header {
					tag, st = "th", stCell+"background:#f4f4f7;font-weight:600;"
				}
				if j < len(b.Align) && b.Align[j] != "" {
					st = strings.Replace(st, "text-align:left;", "text-align:"+b.Align[j]+";", 1)
				}
				out.WriteString("<" + tag + ` style="` + st + `">` + emailInline(c.Inline) + "</" + tag + ">")
			}
			out.WriteString("</tr>")
		}
		out.WriteString("</table>")
		return out.String()
	}
	return `<p style="margin:8px 0;">` + emailInline(b.Inline) + "</p>"
}

// Text renders blocks as plain text (the text/plain part of letters).
func Text(bs []Block) string {
	var parts []string
	for _, b := range bs {
		parts = append(parts, textBlock(b, ""))
	}
	return strings.Join(parts, "\n\n")
}

func textBlock(b Block, indent string) string {
	switch b.Kind {
	case "heading":
		t := PlainInline(b.Inline)
		return t + "\n" + strings.Repeat("=", utf8.RuneCountInString(t))
	case "code":
		return indentLines(b.Code, indent+"    ")
	case "table":
		return indentLines(TableText(b), indent)
	case "divider":
		return indent + "----"
	case "quote":
		var inner []string
		for _, c := range b.Children {
			inner = append(inner, textBlock(c, ""))
		}
		return indentLines(strings.Join(inner, "\n\n"), indent+"> ")
	case "list":
		var lines []string
		for i, it := range b.Items {
			var inner []string
			for _, c := range it {
				inner = append(inner, strings.TrimLeft(textBlock(c, indent+"  "), " "))
			}
			lines = append(lines, indent+marker(b, i)+strings.Join(inner, "\n"))
		}
		return strings.Join(lines, "\n")
	}
	return indentLines(PlainInline(b.Inline), indent)
}

func indentLines(s, prefix string) string {
	if prefix == "" {
		return s
	}
	ls := strings.Split(s, "\n")
	for i := range ls {
		ls[i] = prefix + ls[i]
	}
	return strings.Join(ls, "\n")
}
