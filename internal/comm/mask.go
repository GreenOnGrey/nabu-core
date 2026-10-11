package comm

import (
	"regexp"
	"strings"
)

// Masked replaces a secret in the texts the collector saves (R7, tech §6.5).
const Masked = "[скрыто]"

var (
	rePEM      = regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]+-----.*?(-----END [A-Z0-9 ]+-----|$)`)
	reJWT      = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}`)
	rePrefixed = regexp.MustCompile(`\b(AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9_-]{20,}|xox[abpr]-[A-Za-z0-9-]{10,}|AIza[0-9A-Za-z_-]{30,})`)
	// the value after a word that names a secret and a separator
	reNamed = regexp.MustCompile(`(?i)((?:password|passwd|pwd|пароль|secret|секрет|token|токен|api[ _-]?key|ключ)(?:[^:=\n]{0,30}[:=]|\s+is|\s+[—-])\s*)("[^"]+"|'[^']+'|\S+)`)
	reLong  = regexp.MustCompile(`[A-Za-z0-9+/=_-]{32,}`)
	reCard  = regexp.MustCompile(`\b(?:\d[ -]?){12,18}\d\b`)
)

// Mask hides passwords, keys, tokens and card numbers and reports how many
// places it changed. It runs in the handlers that write to the store, so the
// requirement does not depend on the model following its instructions.
func Mask(s string) (string, int) {
	n := 0
	repl := func(re *regexp.Regexp, fn func(string) string) {
		s = re.ReplaceAllStringFunc(s, func(m string) string {
			out := fn(m)
			if out != m {
				n++
			}
			return out
		})
	}
	all := func(string) string { return Masked }
	repl(rePEM, all)
	repl(reJWT, all)
	repl(rePrefixed, all)
	repl(reNamed, func(m string) string {
		g := reNamed.FindStringSubmatch(m)
		if strings.Contains(g[2], Masked) {
			return m
		}
		return g[1] + Masked
	})
	repl(reLong, func(m string) string {
		if !mixed(m) {
			return m // a long word or a number, not a key
		}
		return Masked
	})
	repl(reCard, func(m string) string {
		if !luhn(m) {
			return m
		}
		return Masked
	})
	return s, n
}

// mixed reports letters together with digits: what keys of 32+ characters
// look like, unlike long words and identifiers made of digits only.
func mixed(s string) bool {
	var letters, digits bool
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			digits = true
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
			letters = true
		}
	}
	return letters && digits
}

func luhn(s string) bool {
	sum, alt, n := 0, false, 0
	for i := len(s) - 1; i >= 0; i-- {
		c := s[i]
		if c < '0' || c > '9' {
			continue
		}
		d := int(c - '0')
		if alt {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		alt = !alt
		n++
	}
	return n >= 13 && n <= 19 && sum%10 == 0
}
