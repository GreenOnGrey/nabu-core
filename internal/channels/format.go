package channels

import (
	"strings"
	"unicode/utf8"
)

// TelegramLimit is the length limit of a Telegram message.
const TelegramLimit = 4096

// mdv2Special are the characters MarkdownV2 requires to escape outside code.
const mdv2Special = "_*[]()~`>#+-=|{}.!\\"

func escapeText(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune(mdv2Special, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func escapeCode(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	return strings.ReplaceAll(s, "`", "\\`")
}

// ToMarkdownV2 renders the agent's Markdown for Telegram: code blocks and
// inline code are kept, **bold** becomes bold, everything else is escaped
// (CH-02).
func ToMarkdownV2(md string) string {
	var b strings.Builder
	lines := strings.Split(md, "\n")
	inCode := false
	for i, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			if !inCode {
				lang := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "```"))
				b.WriteString("```" + escapeText(lang))
			} else {
				b.WriteString("```")
			}
			inCode = !inCode
		} else if inCode {
			b.WriteString(escapeCode(line))
		} else {
			b.WriteString(inline(line))
		}
		if i < len(lines)-1 {
			b.WriteByte('\n')
		}
	}
	if inCode {
		b.WriteString("\n```")
	}
	return b.String()
}

// inline renders one line: `code`, **bold**, the rest escaped.
func inline(line string) string {
	var b strings.Builder
	for line != "" {
		switch {
		case strings.HasPrefix(line, "`"):
			if end := strings.Index(line[1:], "`"); end >= 0 {
				b.WriteString("`" + escapeCode(line[1:1+end]) + "`")
				line = line[end+2:]
				continue
			}
		case strings.HasPrefix(line, "**"):
			if end := strings.Index(line[2:], "**"); end > 0 {
				b.WriteString("*" + escapeText(line[2:2+end]) + "*")
				line = line[end+4:]
				continue
			}
		}
		r, size := utf8.DecodeRuneInString(line)
		b.WriteString(escapeText(string(r)))
		line = line[size:]
	}
	return b.String()
}

// Split splits the agent's Markdown into messages whose rendered form fits
// the limit, on paragraph boundaries; a code block is never cut without
// closing and reopening its fence (CH-01).
func Split(md string, limit int) []string {
	render := func(s string) int { return utf8.RuneCountInString(ToMarkdownV2(s)) }
	if render(md) <= limit {
		return []string{md}
	}
	// Blocks: code blocks whole, other text by paragraphs.
	var blocks []string
	var cur []string
	inCode := false
	flush := func() {
		if len(cur) > 0 {
			blocks = append(blocks, strings.Join(cur, "\n"))
			cur = nil
		}
	}
	for _, line := range strings.Split(md, "\n") {
		fence := strings.HasPrefix(strings.TrimSpace(line), "```")
		switch {
		case fence && !inCode:
			flush()
			inCode = true
			cur = append(cur, line)
		case fence && inCode:
			cur = append(cur, line)
			inCode = false
			flush()
		case inCode:
			cur = append(cur, line)
		case strings.TrimSpace(line) == "":
			flush()
		default:
			cur = append(cur, line)
		}
	}
	flush()
	var out []string
	var msg string
	push := func() {
		if strings.TrimSpace(msg) != "" {
			out = append(out, msg)
		}
		msg = ""
	}
	for _, blk := range blocks {
		for _, piece := range splitBlock(blk, limit, render) {
			cand := piece
			if msg != "" {
				cand = msg + "\n\n" + piece
			}
			if render(cand) <= limit {
				msg = cand
				continue
			}
			push()
			msg = piece
		}
	}
	push()
	return out
}

// splitBlock cuts a block that alone exceeds the limit: by lines, and a code
// block keeps its fences in every part.
func splitBlock(blk string, limit int, render func(string) int) []string {
	if render(blk) <= limit {
		return []string{blk}
	}
	lines := strings.Split(blk, "\n")
	open, close := "", ""
	if strings.HasPrefix(strings.TrimSpace(lines[0]), "```") {
		open, close = lines[0], "```"
		lines = lines[1:]
		if len(lines) > 0 && strings.HasPrefix(strings.TrimSpace(lines[len(lines)-1]), "```") {
			lines = lines[:len(lines)-1]
		}
	}
	wrap := func(ls []string) string {
		if open == "" {
			return strings.Join(ls, "\n")
		}
		return open + "\n" + strings.Join(ls, "\n") + "\n" + close
	}
	var out []string
	var cur []string
	for _, l := range lines {
		for render(wrap([]string{l})) > limit { // a single very long line
			r := []rune(l)
			cut := len(r) / 2
			if cut == 0 {
				break
			}
			out = append(out, splitBlock(string(r[:cut]), limit, render)...)
			l = string(r[cut:])
		}
		if len(cur) > 0 && render(wrap(append(cur, l))) > limit {
			out = append(out, wrap(cur))
			cur = nil
		}
		cur = append(cur, l)
	}
	if len(cur) > 0 {
		out = append(out, wrap(cur))
	}
	return out
}
