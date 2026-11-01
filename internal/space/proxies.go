package space

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"strings"

	"github.com/GreenOnGrey/nabu-core/internal/egress"
	"github.com/GreenOnGrey/nabu-core/internal/proxies"
)

// Proxies of sandboxes (FTR.HMR.CMN-0007 R17, R24, tech §3.4): a sandbox with
// proxies gets HTTPS_PROXY to the egress gateway with its token, the
// variables of the proxies, the label the network policy lets out to the
// gateway and an annotation with the hash of what it got.

// LabelEgress lets a sandbox reach the egress gateway (NetworkPolicy).
const LabelEgress = "nabu.io/egress"

// AnnotationProxies is the hash of the proxies a sandbox started with.
const AnnotationProxies = "nabu.io/proxies"

// proxySetup is what a sandbox gets for its proxies.
type proxySetup struct {
	env      []map[string]any
	hash     string
	resolved []proxies.Resolved
}

// proxyHash names a set of proxies and their versions; "" — none.
func proxyHash(rs []proxies.Resolved) string {
	if len(rs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		parts = append(parts, r.Name+"@"+r.UpdatedAt.UTC().Format("20060102T150405.000000"))
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, ",")))
	return hex.EncodeToString(sum[:8])
}

// resolveProxies loads the proxies by name; none without the gateway.
func (m *Sandboxes) resolveProxies(ctx context.Context, names []string) ([]proxies.Resolved, error) {
	if len(names) == 0 || m.Proxies == nil {
		return nil, nil
	}
	if m.Cfg.EgressURL == "" {
		slog.WarnContext(ctx, "sandbox proxies are given but the egress gateway is off", "proxies", names)
		return nil, nil
	}
	return m.Proxies.Resolve(ctx, names)
}

// setup builds the environment for resolved proxies with the token.
func (m *Sandboxes) setup(rs []proxies.Resolved, token func(names []string) string) proxySetup {
	if len(rs) == 0 {
		return proxySetup{}
	}
	names := make([]string, 0, len(rs))
	for _, r := range rs {
		names = append(names, r.Name)
	}
	vars, clashes := proxies.Merge(rs)
	for _, c := range clashes {
		slog.Warn("sandbox proxies set the same variable", "clash", c)
	}
	proxyURL := egress.ProxyURL(m.Cfg.EgressURL, token(names))
	noProxy := []string{"localhost", "127.0.0.1"}
	for _, u := range []string{m.Cfg.RelayURL, m.Cfg.APIURL} {
		if p, err := url.Parse(u); err == nil && p.Hostname() != "" {
			noProxy = append(noProxy, p.Hostname())
		}
	}
	for k, v := range map[string]string{"HTTPS_PROXY": proxyURL, "HTTP_PROXY": proxyURL, "https_proxy": proxyURL, "http_proxy": proxyURL,
		"NO_PROXY": strings.Join(noProxy, ","), "no_proxy": strings.Join(noProxy, ",")} {
		vars[k] = v
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	env := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		env = append(env, map[string]any{"name": k, "value": vars[k]})
	}
	return proxySetup{env: env, hash: proxyHash(rs), resolved: rs}
}

// apply puts the proxies into a pod manifest.
func (ps proxySetup) apply(man map[string]any) {
	if len(ps.env) == 0 {
		return
	}
	meta := man["metadata"].(map[string]any)
	labels := meta["labels"].(map[string]string)
	labels[LabelEgress] = "true"
	ann, _ := meta["annotations"].(map[string]string)
	if ann == nil {
		ann = map[string]string{}
	}
	ann[AnnotationProxies] = ps.hash
	meta["annotations"] = ann
	c := man["spec"].(map[string]any)["containers"].([]map[string]any)[0]
	c["env"] = append(c["env"].([]map[string]any), ps.env...)
}

// ProxyNote tells the agent what its proxies open (R20).
func ProxyNote(rs []proxies.Resolved) string {
	if len(rs) == 0 {
		return "The sandbox has no access to the internet."
	}
	var b strings.Builder
	b.WriteString("The sandbox reaches the internet only through these proxies (HTTPS_PROXY is set):")
	for _, r := range rs {
		fmt.Fprintf(&b, "\n- %s (%s): %s", r.Title, strings.Join(r.Hosts(), ", "), r.Description)
	}
	return b.String()
}
