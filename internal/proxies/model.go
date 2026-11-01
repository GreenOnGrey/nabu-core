// Package proxies holds the proxies of sandboxes (FTR.HMR.CMN-0007 R16–R24,
// tech §3): named lists of addresses a sandbox may reach through the egress
// gateway, with the variables the sandbox gets. A kind only shapes the form:
// "packages" builds the addresses and variables from a template, "hosts"
// takes them as they are; the gateway and the assignment ignore the kind.
package proxies

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
)

// Kinds of proxies.
const (
	KindPackages = "packages"
	KindHosts    = "hosts"
)

// Ecosystems of the "packages" template.
const (
	EcoGo   = "go"
	EcoNPM  = "npm"
	EcoPyPI = "pypi"
)

// Limits of a proxy (tech §3.1, too_many).
const (
	maxHosts = 50
	maxEnv   = 30
)

// Host is an address a proxy opens: a DNS name, an IP or a mask "*.domain".
type Host struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Internal bool   `json:"internal"`
}

// String is "host:port".
func (h Host) String() string { return net.JoinHostPort(h.Host, strconv.Itoa(h.Port)) }

// Env is a variable the sandbox gets. A secret value is stored encrypted and
// never leaves the api: Value is empty and Set tells whether it is there.
type Env struct {
	Name     string `json:"name"`
	Value    string `json:"value,omitempty"`
	ValueEnc []byte `json:"valueEnc,omitempty"`
	Secret   bool   `json:"secret"`
	Set      bool   `json:"set,omitempty"`
}

// Registry is a private registry of the "packages" kind.
type Registry struct {
	Ecosystem string `json:"ecosystem"`
	URL       string `json:"url"`
	Internal  bool   `json:"internal"`
	Token     string `json:"token,omitempty"`
	TokenEnc  []byte `json:"tokenEnc,omitempty"`
	TokenSet  bool   `json:"tokenSet,omitempty"`
}

// Spec is the form of the "packages" kind (proxies.spec).
type Spec struct {
	Ecosystems []string   `json:"ecosystems"`
	Registries []Registry `json:"registries"`
}

var (
	nameRe = regexp.MustCompile(`^[a-z][a-z0-9-]{1,39}$`)
	envRe  = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
	// labelRe is a label of a DNS name (RFC 1123).
	labelRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
)

// reservedEnv are the variables of Nabu a proxy may not override (R22).
var reservedEnv = map[string]bool{"HTTP_PROXY": true, "HTTPS_PROXY": true, "NO_PROXY": true, "ALL_PROXY": true,
	"HOME": true, "PATH": true, "USER": true, "SHELL": true}

// privateSuffixes are names that point inside the network of a company.
var privateSuffixes = []string{".local", ".internal", ".lan", ".corp", ".localhost"}

// ValidName checks the name of a proxy.
func ValidName(name string) error {
	if !nameRe.MatchString(name) {
		return apperr.Unprocessable("invalid_name", "the name is lowercase latin letters, digits and dashes, 2–40 characters").With("field", "name")
	}
	return nil
}

// NormalizeHost lower-cases the host and sets the default port 443.
func NormalizeHost(h Host) Host {
	h.Host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(h.Host)), ".")
	if h.Port == 0 {
		h.Port = 443
	}
	return h
}

// ValidHost checks one address (R22, tech §3.1); field names it in errors.
func ValidHost(h Host, field string) error {
	if h.Port < 1 || h.Port > 65535 {
		return apperr.Unprocessable("invalid_port", "the port is 1–65535").With("field", field+".port")
	}
	if ip, err := netip.ParseAddr(h.Host); err == nil {
		if PrivateIP(ip) && !h.Internal {
			return apperr.Unprocessable("private_address", "an address inside the company network needs «internal»").With("field", field+".host")
		}
		return nil
	}
	name := h.Host
	if strings.Contains(name, "*") {
		if !strings.HasPrefix(name, "*.") || strings.Count(name, "*") > 1 || strings.Count(name[2:], ".") < 1 {
			return apperr.Unprocessable("wide_mask", "a mask is *. and a domain of the second level or deeper").With("field", field+".host")
		}
		name = name[2:]
	}
	labels := strings.Split(name, ".")
	if len(name) > 253 || len(labels) < 2 && !h.Internal {
		return apperr.Unprocessable("invalid_host", "the host is not a DNS name").With("field", field+".host")
	}
	for _, l := range labels {
		if !labelRe.MatchString(l) {
			return apperr.Unprocessable("invalid_host", "the host is not a DNS name").With("field", field+".host")
		}
	}
	if PrivateName(name) && !h.Internal {
		return apperr.Unprocessable("private_address", "an address inside the company network needs «internal»").With("field", field+".host")
	}
	return nil
}

// PrivateIP reports an address inside a network: RFC 1918, RFC 4193,
// loopback, link-local, CGNAT and unspecified.
func PrivateIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || netip.MustParsePrefix("100.64.0.0/10").Contains(ip)
}

// PrivateName reports a name of the company network by its suffix.
func PrivateName(name string) bool {
	name = "." + strings.ToLower(name)
	for _, s := range privateSuffixes {
		if strings.HasSuffix(name, s) {
			return true
		}
	}
	return false
}

// ValidEnv checks the variables; reserved names belong to Nabu (R22).
func ValidEnv(env []Env) error {
	seen := map[string]bool{}
	for i, e := range env {
		field := fmt.Sprintf("env[%d].name", i)
		if !envRe.MatchString(e.Name) {
			return apperr.Unprocessable("invalid_env", "the name of a variable is A–Z, digits and _").With("field", field)
		}
		if Reserved(e.Name) {
			return apperr.Unprocessable("reserved_env", "Nabu uses this variable").With("field", field)
		}
		if seen[e.Name] {
			return apperr.Unprocessable("invalid_env", "the variable is given twice").With("field", field)
		}
		seen[e.Name] = true
	}
	return nil
}

// Reserved reports a variable of Nabu, in any case.
func Reserved(name string) bool {
	u := strings.ToUpper(name)
	return reservedEnv[u] || strings.HasPrefix(u, "NABU_") || strings.HasPrefix(u, "LD_")
}

// Match finds the address of allow that permits host:port: an exact name or
// a mask "*.d" — any subdomain of d, not d itself (tech §3.5).
func Match(allow []Host, host string, port int) (Host, bool) {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	for _, h := range allow {
		if h.Port != port {
			continue
		}
		if h.Host == host {
			return h, true
		}
		if strings.HasPrefix(h.Host, "*.") && strings.HasSuffix(host, h.Host[1:]) && len(host) > len(h.Host)-1 {
			return h, true
		}
	}
	return Host{}, false
}

// ecosystem is the template of one ecosystem (tech §3.2).
type ecosystem struct {
	allow []Host
	env   []Env
}

var template = map[string]ecosystem{
	EcoGo: {allow: []Host{{Host: "proxy.golang.org", Port: 443}, {Host: "sum.golang.org", Port: 443}},
		env: []Env{{Name: "GOPROXY", Value: "https://proxy.golang.org"}, {Name: "GOFLAGS", Value: "-mod=mod"}}},
	EcoNPM: {allow: []Host{{Host: "registry.npmjs.org", Port: 443}}},
	EcoPyPI: {allow: []Host{{Host: "pypi.org", Port: 443}, {Host: "files.pythonhosted.org", Port: 443}},
		env: []Env{{Name: "PIP_INDEX_URL", Value: "https://pypi.org/simple"}}},
}

// Ecosystems lists the ecosystems of the template in a stable order.
func Ecosystems() []string { return []string{EcoGo, EcoNPM, EcoPyPI} }

// TemplateView is the template for the form (GET …/proxies/templates).
func TemplateView() map[string]any {
	out := map[string]any{}
	for _, e := range Ecosystems() {
		t := template[e]
		hosts := make([]string, 0, len(t.allow))
		for _, h := range t.allow {
			hosts = append(hosts, h.Host)
		}
		env := make([]string, 0, len(t.env))
		for _, v := range t.env {
			env = append(env, v.Name+"="+v.Value)
		}
		out[e] = map[string]any{"hosts": hosts, "env": env}
	}
	return map[string]any{KindPackages: out}
}

// Build makes the addresses and variables of the "packages" kind. Tokens of
// registries become secret variables; their sealed values are passed as is.
func Build(spec Spec) ([]Host, []Env, error) {
	var allow []Host
	env := map[string]Env{}
	order := []string{}
	put := func(e Env) {
		if _, ok := env[e.Name]; !ok {
			order = append(order, e.Name)
		}
		env[e.Name] = e
	}
	for _, e := range spec.Ecosystems {
		t, ok := template[e]
		if !ok {
			return nil, nil, apperr.Unprocessable("invalid_ecosystem", "the ecosystem is go, npm or pypi").With("field", "ecosystems")
		}
		allow = append(allow, t.allow...)
		for _, v := range t.env {
			put(v)
		}
	}
	for i, r := range spec.Registries {
		field := fmt.Sprintf("registries[%d]", i)
		u, err := url.Parse(r.URL)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil {
			return nil, nil, apperr.Unprocessable("invalid_host", "the registry is an http(s) URL without credentials").With("field", field+".url")
		}
		port := 443
		if u.Scheme == "http" {
			port = 80
		}
		if p := u.Port(); p != "" {
			port, _ = strconv.Atoi(p)
		}
		h := NormalizeHost(Host{Host: u.Hostname(), Port: port, Internal: r.Internal})
		if err := ValidHost(h, field); err != nil {
			return nil, nil, err
		}
		allow = append(allow, h)
		hasToken := r.Token != "" || len(r.TokenEnc) > 0
		base := strings.TrimSuffix(r.URL, "/")
		switch r.Ecosystem {
		case EcoGo:
			if hasToken {
				return nil, nil, apperr.Unprocessable("token_unsupported", "a private Go proxy with a token is not supported").With("field", field+".token")
			}
			put(Env{Name: "GOPROXY", Value: base + ",https://proxy.golang.org"})
			put(Env{Name: "GONOSUMDB", Value: u.Hostname()})
		case EcoNPM:
			put(Env{Name: "NPM_CONFIG_REGISTRY", Value: base + "/"})
			if hasToken {
				// Kubernetes takes no "/" or ":" in names, so the scoped
				// _authToken of npm cannot be a variable: the .npmrc of the
				// project reads it, //host/:_authToken=${NPM_TOKEN}.
				put(Env{Name: "NPM_TOKEN", Value: r.Token, ValueEnc: r.TokenEnc, Secret: true})
			}
		case EcoPyPI:
			if hasToken {
				// The token ("user:password") goes into the URL: pip has no
				// separate setting. A kept token is the sealed whole URL.
				v := ""
				if r.Token != "" {
					c := *u
					user, pass, _ := strings.Cut(r.Token, ":")
					c.User = url.UserPassword(user, pass)
					v = strings.TrimSuffix(c.String(), "/")
				}
				put(Env{Name: "PIP_EXTRA_INDEX_URL", Value: v, ValueEnc: r.TokenEnc, Secret: true})
			} else {
				put(Env{Name: "PIP_EXTRA_INDEX_URL", Value: base})
			}
		default:
			return nil, nil, apperr.Unprocessable("invalid_ecosystem", "the ecosystem is go, npm or pypi").With("field", field+".ecosystem")
		}
	}
	out := make([]Env, 0, len(order))
	for _, n := range order {
		out = append(out, env[n])
	}
	return dedupHosts(allow), out, nil
}

func dedupHosts(hs []Host) []Host {
	seen := map[string]bool{}
	out := make([]Host, 0, len(hs))
	for _, h := range hs {
		k := h.String()
		if !seen[k] {
			seen[k] = true
			out = append(out, h)
		}
	}
	return out
}

// Merge joins the variables of proxies sorted by name; on a clash the proxy
// earlier by name wins and the clash is returned (tech §3.4).
func Merge(ps []Resolved) (map[string]string, []string) {
	sort.Slice(ps, func(i, j int) bool { return ps[i].Name < ps[j].Name })
	out := map[string]string{}
	from := map[string]string{}
	var clashes []string
	for _, p := range ps {
		for k, v := range p.Env {
			if prev, ok := from[k]; ok {
				clashes = append(clashes, fmt.Sprintf("%s: %s over %s", k, prev, p.Name))
				continue
			}
			out[k], from[k] = v, p.Name
		}
	}
	return out, clashes
}
