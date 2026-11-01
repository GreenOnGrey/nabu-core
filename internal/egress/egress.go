// Package egress is the gateway sandboxes go outside through (FTR.HMR.CMN-0007
// R17, R18, R22, tech §3.5): an HTTP proxy with CONNECT that lets a sandbox
// reach only the addresses of the proxies in its token. The gateway resolves
// DNS itself and connects to the address it checked, so a permitted name
// cannot lead inside the network without «internal».
package egress

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
	"github.com/GreenOnGrey/nabu-core/internal/proxies"
)

// Limits of the gateway (tech §3.5).
const (
	DialTimeout = 10 * time.Second
	IdleTimeout = 5 * time.Minute
	MaxTunnels  = 64
	cacheTTL    = 30 * time.Second
	// User is the user of Proxy-Authorization; the password is the token.
	User = "nabu"
)

// RunToken is the egress token of a run's sandbox: valid for the run.
func RunToken(s *jwt.Signer, runID string, names []string, ttl time.Duration) string {
	return s.Issue(jwt.Claims{Audience: jwt.AudEgress, Subject: "run", Run: runID, Proxies: names}, ttl)
}

// WorkspaceToken is the egress token of a personal or group sandbox.
func WorkspaceToken(s *jwt.Signer, workspaceID string, names []string, ttl time.Duration) string {
	return s.Issue(jwt.Claims{Audience: jwt.AudEgress, Subject: "workspace", Workspace: workspaceID, Proxies: names}, ttl)
}

// ProxyURL is HTTPS_PROXY for a sandbox with the token.
func ProxyURL(gateway, token string) string {
	u, err := url.Parse(gateway)
	if err != nil {
		return ""
	}
	u.User = url.UserPassword(User, token)
	return u.String()
}

// Gateway is the egress gateway.
type Gateway struct {
	Signer *jwt.Signer
	// Allow loads the addresses of all proxies by name; cached for 30 s, so
	// a changed address reaches running sandboxes (R21).
	Allow func(ctx context.Context) (map[string][]proxies.Host, error)
	// Resolve and Dial are replaced in tests.
	Resolve func(ctx context.Context, host string) ([]netip.Addr, error)
	Dial    func(ctx context.Context, network, addr string) (net.Conn, error)
	// Recheck is how often an open tunnel is checked against the addresses
	// of its proxies (default 30 s): a removed address closes it (R21).
	Recheck time.Duration

	mu      sync.Mutex
	cache   map[string][]proxies.Host
	cacheAt time.Time
	tunnels map[string]int
}

func (g *Gateway) resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if g.Resolve != nil {
		return g.Resolve(ctx, host)
	}
	return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
}

func (g *Gateway) dial(ctx context.Context, addr string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, DialTimeout)
	defer cancel()
	if g.Dial != nil {
		return g.Dial(ctx, "tcp", addr)
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

// allowOf returns the addresses of the proxies in a token.
func (g *Gateway) allowOf(ctx context.Context, names []string) (map[string][]proxies.Host, error) {
	g.mu.Lock()
	if g.cache == nil || time.Since(g.cacheAt) > cacheTTL {
		g.mu.Unlock()
		all, err := g.Allow(ctx)
		if err != nil {
			return nil, err
		}
		g.mu.Lock()
		g.cache, g.cacheAt = all, time.Now()
	}
	out := map[string][]proxies.Host{}
	for _, n := range names {
		if hs, ok := g.cache[n]; ok {
			out[n] = hs
		}
	}
	g.mu.Unlock()
	return out, nil
}

// Flush drops the cache of addresses (tests).
func (g *Gateway) Flush() {
	g.mu.Lock()
	g.cache = nil
	g.mu.Unlock()
}

// claims reads Proxy-Authorization.
func (g *Gateway) claims(r *http.Request) (jwt.Claims, bool) {
	h := r.Header.Get("Proxy-Authorization")
	enc, ok := strings.CutPrefix(h, "Basic ")
	if !ok {
		return jwt.Claims{}, false
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(enc))
	if err != nil {
		return jwt.Claims{}, false
	}
	user, pass, ok := strings.Cut(string(raw), ":")
	if !ok || user != User {
		return jwt.Claims{}, false
	}
	c, err := g.Signer.Verify(pass, jwt.AudEgress)
	return c, err == nil
}

// owner names the sandbox of a token in the journal and the tunnel limit.
func owner(c jwt.Claims) (string, string) {
	if c.Run != "" {
		return "run", c.Run
	}
	if c.Workspace != "" {
		return "workspace", c.Workspace
	}
	return "check", c.Subject
}

// decision is the check of a target.
type decision struct {
	proxy  string
	addr   netip.Addr
	reason string
}

// decide checks host:port against the proxies of the token and resolves it.
func (g *Gateway) decide(ctx context.Context, c jwt.Claims, host string, port int) decision {
	match, proxy, err := g.match(ctx, c.Proxies, host, port)
	if err != nil {
		return decision{proxy: "-", reason: "the proxies are not available: " + err.Error()}
	}
	if proxy == "" {
		return decision{proxy: "-", reason: fmt.Sprintf("%s is not in the proxies of this sandbox (%s)", net.JoinHostPort(host, strconv.Itoa(port)), strings.Join(c.Proxies, ", "))}
	}
	var addrs []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		addrs = []netip.Addr{ip}
	} else if addrs, err = g.resolve(ctx, host); err != nil || len(addrs) == 0 {
		return decision{proxy: proxy, reason: fmt.Sprintf("cannot resolve %s", host)}
	}
	for _, a := range addrs {
		if proxies.PrivateIP(a) && !match.Internal {
			return decision{proxy: proxy, reason: fmt.Sprintf("%s resolves to an address inside the network (%s)", host, a)}
		}
	}
	return decision{proxy: proxy, addr: addrs[0]}
}

// match finds the first proxy of names (by name) with host:port; "" — none.
func (g *Gateway) match(ctx context.Context, names []string, host string, port int) (proxies.Host, string, error) {
	allow, err := g.allowOf(ctx, names)
	if err != nil {
		return proxies.Host{}, "", err
	}
	sorted := make([]string, 0, len(allow))
	for n := range allow {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)
	for _, n := range sorted {
		if h, ok := proxies.Match(allow[n], host, port); ok {
			return h, n, nil
		}
	}
	return proxies.Host{}, "", nil
}

// watch closes upstream when host:port leaves the proxies of the token or
// loses «internal» for a private address (R21). Proxies that cannot be
// loaded keep the tunnel: the next check decides. stop ends the watch.
func (g *Gateway) watch(c jwt.Claims, host string, port int, addr netip.Addr, upstream net.Conn, log *slog.Logger) (stop func()) {
	every := g.Recheck
	if every <= 0 {
		every = cacheTTL
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
			}
			h, proxy, err := g.match(context.Background(), c.Proxies, host, port)
			if err != nil {
				continue
			}
			if proxy == "" || (proxies.PrivateIP(addr) && !h.Internal) {
				log.Info("egress tunnel", "result", "revoked")
				_ = upstream.Close()
				return
			}
		}
	}()
	return func() { close(done) }
}

// ServeHTTP serves CONNECT and plain HTTP with an absolute URL.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	c, ok := g.claims(r)
	if !ok {
		metrics.EgressConnections.WithLabelValues("-", "auth").Inc()
		w.Header().Set("Proxy-Authenticate", `Basic realm="nabu"`)
		http.Error(w, "nabu egress: proxy authentication required", http.StatusProxyAuthRequired)
		return
	}
	var target string
	switch {
	case r.Method == http.MethodConnect:
		target = r.Host
	case r.URL.IsAbs() && r.URL.Scheme == "http":
		target = r.URL.Host
		if r.URL.Port() == "" {
			target = net.JoinHostPort(r.URL.Hostname(), "80")
		}
	default:
		http.Error(w, "nabu egress: CONNECT or an absolute http URL", http.StatusMethodNotAllowed)
		return
	}
	host, portS, err := net.SplitHostPort(target)
	port, perr := strconv.Atoi(portS)
	if err != nil || perr != nil {
		http.Error(w, "nabu egress: bad target", http.StatusBadRequest)
		return
	}
	kind, id := owner(c)
	log := slog.With(kind, id, "host", host, "port", port)
	key := kind + ":" + id
	if !g.acquire(key) {
		metrics.EgressConnections.WithLabelValues("-", "limit").Inc()
		log.Info("egress tunnel", "result", "limit")
		http.Error(w, "nabu egress: too many tunnels of this sandbox", http.StatusTooManyRequests)
		return
	}
	defer g.release(key)
	d := g.decide(r.Context(), c, host, port)
	if d.reason != "" {
		metrics.EgressConnections.WithLabelValues(d.proxy, "denied").Inc()
		log.Info("egress tunnel", "proxy", d.proxy, "result", "denied", "reason", d.reason)
		http.Error(w, "nabu egress: "+d.reason, http.StatusForbidden)
		return
	}
	upstream, err := g.dial(r.Context(), net.JoinHostPort(d.addr.String(), strconv.Itoa(port)))
	if err != nil {
		metrics.EgressConnections.WithLabelValues(d.proxy, "dial_error").Inc()
		log.Info("egress tunnel", "proxy", d.proxy, "ip", d.addr.String(), "result", "dial_error", "err", err)
		http.Error(w, "nabu egress: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer upstream.Close()
	defer g.watch(c, host, port, d.addr, upstream, log)()
	metrics.EgressActive.WithLabelValues(d.proxy).Inc()
	defer metrics.EgressActive.WithLabelValues(d.proxy).Dec()
	var up, down int64
	if r.Method == http.MethodConnect {
		up, down, err = tunnel(w, upstream)
	} else {
		up, down, err = forward(w, r, upstream)
	}
	result := "ok"
	if err != nil {
		result = "dial_error"
	}
	metrics.EgressConnections.WithLabelValues(d.proxy, result).Inc()
	metrics.EgressBytes.WithLabelValues(d.proxy, "up").Add(float64(up))
	metrics.EgressBytes.WithLabelValues(d.proxy, "down").Add(float64(down))
	log.Info("egress tunnel", "proxy", d.proxy, "ip", d.addr.String(), "bytes_up", up, "bytes_down", down, "result", result,
		"duration_ms", time.Since(start).Milliseconds())
}

func (g *Gateway) acquire(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tunnels == nil {
		g.tunnels = map[string]int{}
	}
	if g.tunnels[key] >= MaxTunnels {
		return false
	}
	g.tunnels[key]++
	return true
}

func (g *Gateway) release(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.tunnels[key]--; g.tunnels[key] <= 0 {
		delete(g.tunnels, key)
	}
}

// idleConn resets the deadline on every read and write: a tunnel closes
// after IdleTimeout without traffic, however long it lives.
type idleConn struct {
	net.Conn
	n *atomic.Int64
}

func (c idleConn) Read(b []byte) (int, error) {
	_ = c.SetDeadline(time.Now().Add(IdleTimeout))
	n, err := c.Conn.Read(b)
	c.n.Add(int64(n))
	return n, err
}

func (c idleConn) Write(b []byte) (int, error) {
	_ = c.SetDeadline(time.Now().Add(IdleTimeout))
	return c.Conn.Write(b)
}

func tunnel(w http.ResponseWriter, upstream net.Conn) (int64, int64, error) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "nabu egress: no hijack", http.StatusInternalServerError)
		return 0, 0, errors.New("no hijack")
	}
	client, buf, err := hj.Hijack()
	if err != nil {
		return 0, 0, err
	}
	defer client.Close()
	if _, err := client.Write([]byte("HTTP/1.1 200 Connection established\r\n\r\n")); err != nil {
		return 0, 0, err
	}
	var up, down atomic.Int64
	cu := idleConn{Conn: client, n: &up}
	uu := idleConn{Conn: upstream, n: &down}
	done := make(chan struct{}, 2)
	go func() {
		// Bytes the client sent along with CONNECT are already buffered.
		if n := buf.Reader.Buffered(); n > 0 {
			b, _ := buf.Peek(n)
			up.Add(int64(n))
			_, _ = uu.Write(b)
		}
		_, _ = io.Copy(uu, cu)
		closeWrite(upstream)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(cu, uu)
		closeWrite(client)
		done <- struct{}{}
	}()
	<-done
	<-done
	return up.Load(), down.Load(), nil
}

func closeWrite(c net.Conn) {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}

// forward proxies one plain HTTP request over the checked connection.
func forward(w http.ResponseWriter, r *http.Request, upstream net.Conn) (int64, int64, error) {
	out := r.Clone(r.Context())
	out.RequestURI = ""
	out.Header.Del("Proxy-Authorization")
	out.Header.Del("Proxy-Connection")
	_ = upstream.SetDeadline(time.Now().Add(IdleTimeout))
	if err := out.Write(upstream); err != nil {
		http.Error(w, "nabu egress: "+err.Error(), http.StatusBadGateway)
		return 0, 0, err
	}
	resp, err := http.ReadResponse(bufio.NewReader(upstream), out)
	if err != nil {
		http.Error(w, "nabu egress: "+err.Error(), http.StatusBadGateway)
		return 0, 0, err
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	n, err := io.Copy(w, resp.Body)
	return r.ContentLength, n, err
}

// Check opens CONNECT to h through the gateway with a check token of the
// proxy (R16): the state of a proxy is whether the gateway reaches it.
func Check(gateway string, s *jwt.Signer) proxies.Checker {
	return func(ctx context.Context, proxy string, h proxies.Host) error {
		u, err := url.Parse(gateway)
		if err != nil {
			return err
		}
		var d net.Dialer
		conn, err := d.DialContext(ctx, "tcp", u.Host)
		if err != nil {
			return fmt.Errorf("the gateway: %w", err)
		}
		defer conn.Close()
		if dl, ok := ctx.Deadline(); ok {
			_ = conn.SetDeadline(dl)
		}
		tok := s.Issue(jwt.Claims{Audience: jwt.AudEgress, Subject: "check", Proxies: []string{proxy}}, time.Minute)
		auth := base64.StdEncoding.EncodeToString([]byte(User + ":" + tok))
		if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: Basic %s\r\n\r\n", h, h, auth); err != nil {
			return err
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
			return errors.New(strings.TrimSpace(strings.TrimPrefix(string(b), "nabu egress: ")))
		}
		return nil
	}
}
