package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/go-chi/chi/v5"

	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// Store records which relay pod holds which workspace (workspace_connections).
type Store interface {
	Register(ctx context.Context, workspaceID, kind, pod string) error
	Unregister(ctx context.Context, workspaceID, pod string) error
	Lookup(ctx context.Context, workspaceID string) (pod string, ok bool, err error)
	Ping(ctx context.Context, workspaceID, pod string) error
	// Touch marks activity of the workspace (sandboxes sleep after idle, R7).
	Touch(ctx context.Context, workspaceID string)
}

// Config configures the relay.
type Config struct {
	// Pod is the address other relay pods use to reach this one (pod IP:port).
	Pod string
	// WaitConnect is how long a call waits for its workspace to connect.
	WaitConnect time.Duration
	// Reconnect is how long a call interrupted by a disconnect waits for the
	// workspace to come back before it fails (tech §6: 60 seconds).
	Reconnect time.Duration
	// PingInterval and missed pings before the channel is dropped (20 s, 3).
	PingInterval time.Duration
	MaxMissed    int
}

// Server is the relay: it accepts workspace channels and serves the calls of
// the agent's nabu-workspace extension.
type Server struct {
	cfg    Config
	signer *jwt.Signer
	store  Store

	mu    sync.Mutex
	conns map[string]*conn
	seq   atomic.Int64
	http  *http.Client
}

// NewServer creates a relay.
func NewServer(cfg Config, signer *jwt.Signer, store Store) *Server {
	if cfg.WaitConnect <= 0 {
		cfg.WaitConnect = 90 * time.Second
	}
	if cfg.Reconnect <= 0 {
		cfg.Reconnect = 60 * time.Second
	}
	if cfg.PingInterval <= 0 {
		cfg.PingInterval = 20 * time.Second
	}
	if cfg.MaxMissed <= 0 {
		cfg.MaxMissed = 3
	}
	return &Server{cfg: cfg, signer: signer, store: store, conns: map[string]*conn{}, http: &http.Client{}}
}

// Routes mounts GET /v1/workspaces/connect (public, through the ingress of
// nabu-api) and the internal call endpoint.
func (s *Server) Routes(r chi.Router) {
	r.Get("/v1/workspaces/connect", s.connect)
	r.Post("/internal/v1/workspaces/{id}/v1/*", s.call)
}

// ─── workspace channels ─────────────────────────────────────────────

type conn struct {
	id, kind string
	ws       *websocket.Conn
	out      chan Frame
	done     chan struct{}
	once     sync.Once
	missed   atomic.Int32

	mu      sync.Mutex
	pending map[string]chan Frame
}

func (c *conn) close() {
	c.once.Do(func() {
		close(c.done)
		c.mu.Lock()
		for id, ch := range c.pending {
			close(ch)
			delete(c.pending, id)
		}
		c.mu.Unlock()
	})
}

func (c *conn) send(ctx context.Context, f Frame) error {
	select {
	case c.out <- f:
		return nil
	case <-c.done:
		return errDisconnected
	case <-ctx.Done():
		return ctx.Err()
	}
}

var errDisconnected = errors.New("workspace_disconnected")

func (s *Server) connect(w http.ResponseWriter, r *http.Request) {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	claims, err := s.signer.Verify(tok, jwt.AudWorkspace)
	if err != nil || claims.Workspace == "" {
		http.Error(w, `{"error":"unauthorized","message":"invalid workspace token"}`, http.StatusUnauthorized) // RLY-02
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{})
	if err != nil {
		return
	}
	ws.SetReadLimit(MaxFrame * 2)
	ctx := r.Context()
	hctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	var hello Frame
	err = readFrame(hctx, ws, &hello)
	cancel()
	if err != nil || hello.Type != FrameHello || hello.WorkspaceID != claims.Workspace || hello.Protocol != Protocol {
		_ = ws.Close(websocket.StatusPolicyViolation, "hello does not match the token")
		return
	}
	kind := claims.Kind
	if kind == "" {
		kind = hello.Kind
	}
	c := &conn{id: claims.Workspace, kind: kind, ws: ws, out: make(chan Frame, 64), done: make(chan struct{}), pending: map[string]chan Frame{}}
	s.mu.Lock()
	prev := s.conns[c.id]
	s.conns[c.id] = c
	s.mu.Unlock()
	if prev != nil {
		prev.close()
		_ = prev.ws.Close(websocket.StatusGoingAway, "replaced by a new connection")
	}
	if err := s.store.Register(ctx, c.id, kind, s.cfg.Pod); err != nil {
		slog.Error("relay: register workspace", "workspace", c.id, "err", err)
	}
	metrics.WorkspaceConnections.WithLabelValues(kind).Inc()
	slog.Info("workspace connected", "workspace", c.id, "kind", kind)
	s.serveConn(ctx, c)
	metrics.WorkspaceConnections.WithLabelValues(kind).Dec()
	s.mu.Lock()
	if s.conns[c.id] == c {
		delete(s.conns, c.id)
		_ = s.store.Unregister(context.WithoutCancel(ctx), c.id, s.cfg.Pod)
	}
	s.mu.Unlock()
	slog.Info("workspace disconnected", "workspace", c.id)
}

func (s *Server) serveConn(ctx context.Context, c *conn) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer c.close()
	go func() { // writer
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.done:
				return
			case f := <-c.out:
				if err := writeFrame(ctx, c.ws, f); err != nil {
					c.close()
					return
				}
			}
		}
	}()
	go func() { // pings: 3 missed pongs drop the channel
		t := time.NewTicker(s.cfg.PingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.done:
				return
			case <-t.C:
				if int(c.missed.Add(1)) > s.cfg.MaxMissed {
					slog.Warn("workspace missed pings", "workspace", c.id)
					c.close()
					_ = c.ws.Close(websocket.StatusGoingAway, "missed pings")
					return
				}
				_ = c.send(ctx, Frame{Type: FramePing})
				_ = s.store.Ping(ctx, c.id, s.cfg.Pod)
			}
		}
	}()
	for {
		var f Frame
		if err := readFrame(ctx, c.ws, &f); err != nil {
			return
		}
		switch f.Type {
		case FramePong:
			c.missed.Store(0)
		case FramePing:
			c.missed.Store(0)
			_ = c.send(ctx, Frame{Type: FramePong})
		case FrameResult:
			c.mu.Lock()
			ch := c.pending[f.ID]
			if f.Final && ch != nil {
				delete(c.pending, f.ID)
			}
			c.mu.Unlock()
			if ch != nil {
				ch <- f
				if f.Final {
					close(ch)
				}
			}
		}
	}
}

func (s *Server) local(id string) *conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns[id]
}

// Connected reports whether the workspace is connected to any relay pod.
func (s *Server) Connected(ctx context.Context, id string) bool {
	if s.local(id) != nil {
		return true
	}
	_, ok, _ := s.store.Lookup(ctx, id)
	return ok
}

// ─── calls ──────────────────────────────────────────────────────────

const forwardedHeader = "X-Nabu-Relay-Forwarded"

func callErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "message": msg})
}

func (s *Server) call(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	op := chi.URLParam(r, "*")
	claims, err := s.signer.Verify(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), jwt.AudCall)
	if err != nil || claims.Workspace != id {
		callErr(w, http.StatusUnauthorized, "unauthorized", "invalid call token")
		return
	}
	if !Ops[op] {
		callErr(w, http.StatusNotFound, "not_found", "unknown operation "+op)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 80<<20))
	if err != nil {
		callErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	s.store.Touch(r.Context(), id)
	deadline := time.Now().Add(s.cfg.WaitConnect)
	for {
		c := s.local(id)
		if c == nil && r.Header.Get(forwardedHeader) == "" {
			pod, ok, _ := s.store.Lookup(r.Context(), id)
			if ok && pod != s.cfg.Pod {
				s.forward(w, r, pod, body)
				return
			}
		}
		if c == nil {
			if time.Now().After(deadline) {
				metrics.WorkspaceCalls.WithLabelValues(op, "not_connected").Inc()
				callErr(w, http.StatusConflict, "workspace_not_connected", "the workspace is not connected")
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		started, err := s.do(w, r, c, op, body)
		if err == nil {
			metrics.WorkspaceCalls.WithLabelValues(op, "ok").Inc()
			return
		}
		if !errors.Is(err, errDisconnected) || r.Context().Err() != nil {
			return
		}
		// RLY-05/06: an interrupted call is repeated after the workspace
		// reconnects, except exec whose effects cannot be repeated.
		if Streaming(op) || started {
			metrics.WorkspaceCalls.WithLabelValues(op, "disconnected").Inc()
			if !started {
				callErr(w, http.StatusBadGateway, "workspace_disconnected", "the workspace disconnected during the call")
			} else if Streaming(op) {
				line, _ := json.Marshal(map[string]any{"exitCode": nil, "disconnected": true})
				_, _ = w.Write(append(line, '\n'))
			}
			return
		}
		deadline = time.Now().Add(s.cfg.Reconnect)
		for s.local(id) == c {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// do sends one call and streams its result frames as the HTTP response.
// started reports whether the response has begun.
func (s *Server) do(w http.ResponseWriter, r *http.Request, c *conn, op string, body []byte) (started bool, err error) {
	id := strconv.FormatInt(s.seq.Add(1), 36)
	ch := make(chan Frame, 16)
	c.mu.Lock()
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()
	if !json.Valid(body) {
		body = []byte("{}")
	}
	// Params travel in chunks like results (RLY-04): fs/write of a large file.
	for seq, off := 1, 0; ; seq++ {
		end := min(off+chunkSize, len(body))
		f := Frame{Type: FrameCall, ID: id, Seq: seq, Chunk: body[off:end], Final: end == len(body)}
		if seq == 1 {
			f.Tool = op
		}
		if err := c.send(r.Context(), f); err != nil {
			return false, err
		}
		if f.Final {
			break
		}
		off = end
	}
	fl, _ := w.(http.Flusher)
	for {
		select {
		case <-r.Context().Done():
			_ = c.send(context.Background(), Frame{Type: FrameAbort, ID: id})
			return started, r.Context().Err()
		case f, ok := <-ch:
			if !ok {
				return started, errDisconnected
			}
			if !started {
				status := f.Status
				if status == 0 {
					status = http.StatusOK
				}
				if f.Error != "" && f.Status == 0 {
					status = http.StatusBadGateway
				}
				ct := "application/json"
				if op == "exec" && status == http.StatusOK {
					ct = "application/x-ndjson"
				}
				w.Header().Set("Content-Type", ct)
				w.WriteHeader(status)
				started = true
			}
			if len(f.Chunk) > 0 {
				if _, err := w.Write(f.Chunk); err != nil {
					return started, err
				}
				if fl != nil {
					fl.Flush()
				}
			}
			if f.Final {
				return started, nil
			}
		}
	}
}

// forward passes a call to the relay pod that holds the workspace.
func (s *Server) forward(w http.ResponseWriter, r *http.Request, pod string, body []byte) {
	target, err := url.Parse("http://" + pod)
	if err != nil {
		callErr(w, http.StatusBadGateway, "workspace_not_connected", err.Error())
		return
	}
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.FlushInterval = -1
	r2 := r.Clone(r.Context())
	r2.Body = io.NopCloser(strings.NewReader(string(body)))
	r2.ContentLength = int64(len(body))
	r2.Header.Set(forwardedHeader, "1")
	rp.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, err error) {
		callErr(w, http.StatusBadGateway, "workspace_not_connected", fmt.Sprintf("relay pod %s: %v", pod, err))
	}
	rp.ServeHTTP(w, r2)
}

func readFrame(ctx context.Context, ws *websocket.Conn, f *Frame) error {
	_, data, err := ws.Read(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, f)
}

func writeFrame(ctx context.Context, ws *websocket.Conn, f Frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return ws.Write(wctx, websocket.MessageText, b)
}
