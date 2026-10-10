// Package render turns the agent's Markdown (GFM) into the formats of the
// channels (FTR.NAB.CMN-0002 arch §2.3, tech §9): Telegram rich messages,
// the HTML subset of Telegram and VK Teams, and letters (HTML and text).
// The Markdown is parsed once into blocks; every channel gets the best it can show.
package render

import (
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	east "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/text"
)

// Block is a block of the answer.
type Block struct {
	Kind     string    // heading | paragraph | code | list | quote | table | divider
	Level    int       // heading 1–6
	Inline   []Inline  // heading, paragraph
	Lang     string    // code
	Code     string    // code
	Ordered  bool      // list
	Start    int       // list: the first number
	Items    [][]Block // list items
	Children []Block   // quote
	Rows     [][]Cell  // table; the first row is the header
	Align    []string  // table: left | center | right | ""
	Checked  []*bool   // list: task items
}

// Cell is a table cell.
type Cell struct {
	Inline []Inline
	Header bool
}

// Inline is a span of text.
type Inline struct {
	Kind     string // text | bold | italic | strike | code | link | break
	Text     string // text, code
	URL      string // link
	Children []Inline
}

var md = goldmark.New(goldmark.WithExtensions(extension.GFM))

// Parse reads Markdown into blocks.
func Parse(src string) []Block {
	source := []byte(src)
	doc := md.Parser().Parse(text.NewReader(source))
	return blocks(doc, source)
}

func blocks(parent ast.Node, src []byte) []Block {
	var out []Block
	for n := parent.FirstChild(); n != nil; n = n.NextSibling() {
		if b, ok := block(n, src); ok {
			out = append(out, b)
		}
	}
	return out
}

func block(n ast.Node, src []byte) (Block, bool) {
	switch v := n.(type) {
	case *ast.Heading:
		return Block{Kind: "heading", Level: v.Level, Inline: inlines(n, src)}, true
	case *ast.Paragraph, *ast.TextBlock:
		in := inlines(n, src)
		if len(in) == 0 {
			return Block{}, false
		}
		return Block{Kind: "paragraph", Inline: in}, true
	case *ast.FencedCodeBlock:
		return Block{Kind: "code", Lang: string(v.Language(src)), Code: lines(n, src)}, true
	case *ast.CodeBlock:
		return Block{Kind: "code", Code: lines(n, src)}, true
	case *ast.HTMLBlock:
		return Block{Kind: "paragraph", Inline: []Inline{{Kind: "text", Text: strings.TrimSpace(lines(n, src))}}}, true
	case *ast.ThematicBreak:
		return Block{Kind: "divider"}, true
	case *ast.Blockquote:
		return Block{Kind: "quote", Children: blocks(n, src)}, true
	case *ast.List:
		b := Block{Kind: "list", Ordered: v.IsOrdered(), Start: v.Start}
		for it := n.FirstChild(); it != nil; it = it.NextSibling() {
			var checked *bool
			if p := it.FirstChild(); p != nil {
				if cb, ok := p.FirstChild().(*east.TaskCheckBox); ok {
					c := cb.IsChecked
					checked = &c
				}
			}
			b.Items = append(b.Items, blocks(it, src))
			b.Checked = append(b.Checked, checked)
		}
		return b, true
	case *east.Table:
		b := Block{Kind: "table"}
		for _, a := range v.Alignments {
			switch a {
			case east.AlignLeft:
				b.Align = append(b.Align, "left")
			case east.AlignCenter:
				b.Align = append(b.Align, "center")
			case east.AlignRight:
				b.Align = append(b.Align, "right")
			default:
				b.Align = append(b.Align, "")
			}
		}
		for row := n.FirstChild(); row != nil; row = row.NextSibling() {
			_, header := row.(*east.TableHeader)
			var cells []Cell
			for c := row.FirstChild(); c != nil; c = c.NextSibling() {
				cells = append(cells, Cell{Inline: inlines(c, src), Header: header})
			}
			b.Rows = append(b.Rows, cells)
		}
		return b, true
	}
	if n.HasChildren() {
		return Block{Kind: "paragraph", Inline: inlines(n, src)}, true
	}
	return Block{}, false
}

func lines(n ast.Node, src []byte) string {
	var b strings.Builder
	l := n.Lines()
	for i := 0; i < l.Len(); i++ {
		s := l.At(i)
		b.Write(s.Value(src))
	}
	return strings.TrimRight(b.String(), "\n")
}

func inlines(parent ast.Node, src []byte) []Inline {
	var out []Inline
	for n := parent.FirstChild(); n != nil; n = n.NextSibling() {
		switch v := n.(type) {
		case *ast.Text:
			out = appendText(out, string(v.Segment.Value(src)))
			if v.HardLineBreak() {
				out = append(out, Inline{Kind: "break"})
			} else if v.SoftLineBreak() {
				out = appendText(out, " ")
			}
		case *ast.String:
			out = appendText(out, string(v.Value))
		case *ast.CodeSpan:
			var b strings.Builder
			for c := n.FirstChild(); c != nil; c = c.NextSibling() {
				if t, ok := c.(*ast.Text); ok {
					b.Write(t.Segment.Value(src))
				}
			}
			out = append(out, Inline{Kind: "code", Text: b.String()})
		case *ast.Emphasis:
			kind := "italic"
			if v.Level >= 2 {
				kind = "bold"
			}
			out = append(out, Inline{Kind: kind, Children: inlines(n, src)})
		case *east.Strikethrough:
			out = append(out, Inline{Kind: "strike", Children: inlines(n, src)})
		case *ast.Link:
			out = append(out, Inline{Kind: "link", URL: string(v.Destination), Children: inlines(n, src)})
		case *ast.AutoLink:
			u := string(v.URL(src))
			label := string(v.Label(src))
			if v.AutoLinkType == ast.AutoLinkEmail && !strings.HasPrefix(u, "mailto:") {
				u = "mailto:" + u
			}
			out = append(out, Inline{Kind: "link", URL: u, Children: []Inline{{Kind: "text", Text: label}}})
		case *ast.Image:
			out = append(out, Inline{Kind: "link", URL: string(v.Destination), Children: inlines(n, src)})
		case *ast.RawHTML:
			var b strings.Builder
			for i := 0; i < v.Segments.Len(); i++ {
				s := v.Segments.At(i)
				b.Write(s.Value(src))
			}
			if t := b.String(); t == "<br>" || t == "<br/>" || t == "<br />" {
				out = append(out, Inline{Kind: "break"})
			}
		case *east.TaskCheckBox:
			// shown by the list
		default:
			if n.HasChildren() {
				out = append(out, inlines(n, src)...)
			}
		}
	}
	return out
}

func appendText(out []Inline, s string) []Inline {
	if s == "" {
		return out
	}
	if k := len(out) - 1; k >= 0 && out[k].Kind == "text" {
		out[k].Text += s
		return out
	}
	return append(out, Inline{Kind: "text", Text: s})
}

// PlainInline is the text of spans without formatting.
func PlainInline(in []Inline) string {
	var b strings.Builder
	for _, s := range in {
		switch s.Kind {
		case "text", "code":
			b.WriteString(s.Text)
		case "break":
			b.WriteByte('\n')
		case "link":
			t := PlainInline(s.Children)
			b.WriteString(t)
			if u := strings.TrimPrefix(s.URL, "mailto:"); u != "" && u != t {
				b.WriteString(" (" + s.URL + ")")
			}
		default:
			b.WriteString(PlainInline(s.Children))
		}
	}
	return b.String()
}
