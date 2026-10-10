package email

import "strings"

// AuthResults is a parsed Authentication-Results header (RFC 8601).
type AuthResults struct {
	ServID  string
	Methods []AuthMethod
}

// AuthMethod is one result: dkim=pass header.d=company.ru.
type AuthMethod struct {
	Method, Result string
	Props          map[string]string
}

// stripComments removes (comments) outside quoted strings.
func stripComments(s string) string {
	var b strings.Builder
	depth := 0
	quoted := false
	for _, r := range s {
		switch {
		case r == '"' && depth == 0:
			quoted = !quoted
			b.WriteRune(r)
		case quoted:
			b.WriteRune(r)
		case r == '(':
			depth++
		case r == ')' && depth > 0:
			depth--
		case depth == 0:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// ParseAuthResults reads "authserv-id [version]; method=result prop=value; …".
func ParseAuthResults(h string) AuthResults {
	h = stripComments(strings.ReplaceAll(strings.ReplaceAll(h, "\r", " "), "\n", " "))
	parts := strings.Split(h, ";")
	var ar AuthResults
	if len(parts) == 0 {
		return ar
	}
	if f := strings.Fields(parts[0]); len(f) > 0 {
		ar.ServID = strings.ToLower(f[0])
	}
	for _, p := range parts[1:] {
		f := strings.Fields(p)
		if len(f) == 0 {
			continue
		}
		mr := strings.SplitN(f[0], "=", 2)
		if len(mr) != 2 {
			continue
		}
		m := AuthMethod{Method: strings.ToLower(mr[0]), Result: strings.ToLower(strings.Trim(mr[1], `"`)), Props: map[string]string{}}
		if i := strings.IndexByte(m.Method, '/'); i > 0 { // method/version
			m.Method = m.Method[:i]
		}
		for _, kv := range f[1:] {
			if k, v, ok := strings.Cut(kv, "="); ok {
				m.Props[strings.ToLower(k)] = strings.Trim(v, `"`)
			}
		}
		ar.Methods = append(ar.Methods, m)
	}
	return ar
}
