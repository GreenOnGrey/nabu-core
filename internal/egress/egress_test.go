package egress

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/proxies"
)

// setup starts the gateway; every public name resolves to 93.184.216.34 and
// every dial goes to a local echo server, "inner.example.org" resolves inside.
func setup(t *testing.T, allow map[string][]proxies.Host) (*Gateway, *jwt.Signer, string) {
	t.Helper()
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = echo.Close() })
	go func() {
		for {
			c, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = io.Copy(c, c) }()
		}
	}()
	s := jwt.NewSigner("nabu", []byte("0123456789abcdef0123456789abcdef"))
	g := &Gateway{Signer: s,
		Allow: func(context.Context) (map[string][]proxies.Host, error) { return allow, nil },
		Resolve: func(_ context.Context, host string) ([]netip.Addr, error) {
			if host == "inner.example.org" {
				return []netip.Addr{netip.MustParseAddr("10.0.0.1")}, nil
			}
			return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
		},
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, echo.Addr().String())
		}}
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	return g, s, strings.TrimPrefix(srv.URL, "http://")
}

// connect sends CONNECT and, on 200, echoes "ping" through the tunnel.
func connect(t *testing.T, gw, target, token string) (int, string) {
	t.Helper()
	c, err := net.Dial("tcp", gw)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	auth := ""
	if token != "" {
		auth = "Proxy-Authorization: Basic " + base64.StdEncoding.EncodeToString([]byte(User+":"+token)) + "\r\n"
	}
	fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n%s\r\n", target, target, auth)
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	fmt.Fprint(c, "ping")
	b := make([]byte, 4)
	if _, err := io.ReadFull(br, b); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

var allow = map[string][]proxies.Host{
	"packages": {{Host: "registry.npmjs.org", Port: 443}, {Host: "*.example.com", Port: 443}},
	"docs":     {{Host: "inner.example.org", Port: 443, Internal: true}},
	"open":     {{Host: "inner.example.org", Port: 8443}},
}

// FTR.HMR.CMN-0007 EG-01: no, foreign or expired tokens get 407.
func TestEgressAuth(t *testing.T) {
	_, s, gw := setup(t, allow)
	other := jwt.NewSigner("nabu", []byte("ffffffffffffffffffffffffffffffff"))
	for name, tok := range map[string]string{
		"none":     "",
		"foreign":  RunToken(other, "r1", []string{"packages"}, time.Minute),
		"expired":  RunToken(s, "r1", []string{"packages"}, -time.Minute),
		"audience": s.Issue(jwt.Claims{Audience: jwt.AudSandbox, Run: "r1"}, time.Minute),
	} {
		if code, _ := connect(t, gw, "registry.npmjs.org:443", tok); code != http.StatusProxyAuthRequired {
			t.Errorf("%s: %d", name, code)
		}
	}
}

// EG-02, EG-03, PX-14: the addresses of the given proxies only.
func TestEgressAllow(t *testing.T) {
	_, s, gw := setup(t, allow)
	tok := RunToken(s, "r1", []string{"packages"}, time.Minute)
	if code, body := connect(t, gw, "registry.npmjs.org:443", tok); code != 200 || body != "ping" {
		t.Fatalf("allowed: %d %q", code, body)
	}
	if code, _ := connect(t, gw, "a.example.com:443", tok); code != 200 {
		t.Fatalf("mask: %d", code)
	}
	code, body := connect(t, gw, "example.com:443", tok)
	if code != http.StatusForbidden || !strings.Contains(body, "not in the proxies of this sandbox (packages)") {
		t.Fatalf("denied: %d %q", code, body)
	}
	// EG-05: another port of an allowed name
	if code, _ := connect(t, gw, "registry.npmjs.org:80", tok); code != http.StatusForbidden {
		t.Fatalf("port: %d", code)
	}
}

// EG-04: a name that resolves inside the network needs «internal».
func TestEgressInternal(t *testing.T) {
	_, s, gw := setup(t, allow)
	if code, _ := connect(t, gw, "inner.example.org:443", RunToken(s, "r1", []string{"docs"}, time.Minute)); code != 200 {
		t.Fatalf("internal: %d", code)
	}
	code, body := connect(t, gw, "inner.example.org:8443", RunToken(s, "r1", []string{"open"}, time.Minute))
	if code != http.StatusForbidden || !strings.Contains(body, "inside the network") {
		t.Fatalf("rebinding: %d %q", code, body)
	}
}

// EG-06: at most MaxTunnels tunnels of one sandbox.
func TestEgressTunnelLimit(t *testing.T) {
	g := &Gateway{}
	for i := 0; i < MaxTunnels; i++ {
		if !g.acquire("run:r1") {
			t.Fatalf("tunnel %d refused", i)
		}
	}
	if g.acquire("run:r1") {
		t.Fatal("the limit is not applied")
	}
	if !g.acquire("run:r2") {
		t.Fatal("another sandbox is limited")
	}
	g.release("run:r1")
	if !g.acquire("run:r1") {
		t.Fatal("a released tunnel is not counted back")
	}
}

// EG-08, PX-18: a removed address is refused once the cache expires.
func TestEgressReload(t *testing.T) {
	cur := map[string][]proxies.Host{"packages": {{Host: "pypi.org", Port: 443}}}
	g, s, gw := setup(t, nil)
	g.Allow = func(context.Context) (map[string][]proxies.Host, error) { return cur, nil }
	tok := RunToken(s, "r1", []string{"packages"}, time.Minute)
	if code, _ := connect(t, gw, "pypi.org:443", tok); code != 200 {
		t.Fatalf("before: %d", code)
	}
	cur = map[string][]proxies.Host{"packages": {{Host: "registry.npmjs.org", Port: 443}}}
	g.Flush() // the 30 s of the cache
	if code, _ := connect(t, gw, "pypi.org:443", tok); code != http.StatusForbidden {
		t.Fatalf("removed: %d", code)
	}
	if code, _ := connect(t, gw, "registry.npmjs.org:443", tok); code != 200 {
		t.Fatalf("added: %d", code)
	}
}

// R21: a removed address closes the open tunnels to it.
func TestEgressRevoke(t *testing.T) {
	var cur atomic.Pointer[map[string][]proxies.Host]
	cur.Store(&map[string][]proxies.Host{"packages": {{Host: "pypi.org", Port: 443}}})
	g, s, gw := setup(t, nil)
	g.Allow = func(context.Context) (map[string][]proxies.Host, error) { return *cur.Load(), nil }
	g.Recheck = 20 * time.Millisecond
	c, err := net.Dial("tcp", gw)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	auth := base64.StdEncoding.EncodeToString([]byte(User + ":" + RunToken(s, "r1", []string{"packages"}, time.Minute)))
	fmt.Fprintf(c, "CONNECT pypi.org:443 HTTP/1.1\r\nHost: pypi.org:443\r\nProxy-Authorization: Basic %s\r\n\r\n", auth)
	br := bufio.NewReader(c)
	if resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect}); err != nil || resp.StatusCode != 200 {
		t.Fatalf("open: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	fmt.Fprint(c, "ping")
	b := make([]byte, 4)
	if _, err := io.ReadFull(br, b); err != nil {
		t.Fatalf("a kept address keeps the tunnel: %v", err)
	}
	cur.Store(&map[string][]proxies.Host{"packages": {{Host: "registry.npmjs.org", Port: 443}}})
	g.Flush()
	if _, err := io.ReadAll(br); err != nil {
		t.Fatalf("the tunnel is closed, not timed out: %v", err)
	}
}

// EG-07: the gateway is not a web server.
func TestEgressMethods(t *testing.T) {
	_, s, gw := setup(t, allow)
	req, _ := http.NewRequest(http.MethodPost, "http://"+gw+"/", nil)
	req.Header.Set("Proxy-Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(User+":"+RunToken(s, "r1", nil, time.Minute))))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("%d", resp.StatusCode)
	}
}

// R16: the check of a proxy is a CONNECT through the gateway.
func TestCheck(t *testing.T) {
	_, s, gw := setup(t, allow)
	check := Check("http://"+gw, s)
	if err := check(context.Background(), "packages", proxies.Host{Host: "registry.npmjs.org", Port: 443}); err != nil {
		t.Fatal(err)
	}
	if err := check(context.Background(), "packages", proxies.Host{Host: "evil.org", Port: 443}); err == nil || !strings.Contains(err.Error(), "not in the proxies") {
		t.Fatalf("%v", err)
	}
}
