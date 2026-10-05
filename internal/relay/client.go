package relay

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Client is the workspace side of the channel: it connects to the relay with
// a workspace token, sends hello and serves call frames with Handler (the
// workspace server). It reconnects with exponential backoff up to 10 seconds
// between attempts (FTR.HMR.CMN-0006 tech §3.5).
type Client struct {
	URL         string // wss://nabu-api.<domain>/v1/workspaces/connect
	Token       string
	WorkspaceID string
	Kind        string // sandbox | external
	// Handler serves the operations; it is called with the bearer token of the
	// workspace server set to HandlerToken.
	Handler      http.Handler
	HandlerToken string
	// GiveUp stops reconnecting after this long without a connection (0 — never).
	GiveUp time.Duration
	// OnConnected is called after each successful hello.
	OnConnected func()
}

// Run keeps the channel open until ctx ends.
func (c *Client) Run(ctx context.Context) error {
	backoff := 500 * time.Millisecond
	lastUp := time.Now()
	for ctx.Err() == nil {
		up, err := c.session(ctx)
		if up {
			backoff = 500 * time.Millisecond
			lastUp = time.Now()
		}
		if ctx.Err() != nil {
			return nil
		}
		if c.GiveUp > 0 && time.Since(lastUp) > c.GiveUp {
			return errors.New("relay: could not reconnect: " + errString(err))
		}
		slog.Warn("relay channel closed, reconnecting", "workspace", c.WorkspaceID, "err", err, "in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 10*time.Second {
			backoff = 10 * time.Second
		}
	}
	return nil
}

func errString(err error) string {
	if err == nil {
		return "closed"
	}
	return err.Error()
}

func (c *Client) session(ctx context.Context) (bool, error) {
	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	ws, _, err := websocket.Dial(dctx, c.URL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": {"Bearer " + c.Token}},
	})
	cancel()
	if err != nil {
		return false, err
	}
	ws.SetReadLimit(MaxFrame * 2)
	defer func() { _ = ws.CloseNow() }()
	if err := writeFrame(ctx, ws, Frame{Type: FrameHello, WorkspaceID: c.WorkspaceID, Kind: c.Kind, Protocol: Protocol}); err != nil {
		return false, err
	}
	if c.OnConnected != nil {
		c.OnConnected()
	}
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	out := make(chan Frame, 64)
	var wmu sync.Mutex
	send := func(f Frame) {
		select {
		case out <- f:
		case <-sctx.Done():
		}
	}
	go func() {
		for {
			select {
			case <-sctx.Done():
				return
			case f := <-out:
				wmu.Lock()
				err := writeFrame(sctx, ws, f)
				wmu.Unlock()
				if err != nil {
					stop()
					return
				}
			}
		}
	}()
	var cmu sync.Mutex
	calls := map[string]context.CancelFunc{}
	partial := map[string]*Frame{} // calls whose params are still arriving
	for {
		var f Frame
		if err := readFrame(sctx, ws, &f); err != nil {
			cmu.Lock()
			for _, cancel := range calls {
				cancel()
			}
			cmu.Unlock()
			return true, err
		}
		switch f.Type {
		case FramePing:
			send(Frame{Type: FramePong})
		case FrameAbort:
			cmu.Lock()
			if cancel := calls[f.ID]; cancel != nil {
				cancel()
			}
			cmu.Unlock()
		case FrameCall:
			if p := partial[f.ID]; p != nil {
				p.Chunk = append(p.Chunk, f.Chunk...)
				p.Final = f.Final
				f = *p
			}
			if !f.Final {
				if partial[f.ID] == nil {
					cp := f
					partial[f.ID] = &cp
				}
				continue
			}
			delete(partial, f.ID)
			f.Params = f.Chunk
			cctx, cancel := context.WithCancel(sctx)
			cmu.Lock()
			calls[f.ID] = cancel
			cmu.Unlock()
			go func(f Frame) {
				defer func() {
					cmu.Lock()
					delete(calls, f.ID)
					cmu.Unlock()
					cancel()
				}()
				c.serveCall(cctx, f, send)
			}(f)
		}
	}
}

// serveCall runs one operation through the workspace handler and streams the
// response back in chunks.
func (c *Client) serveCall(ctx context.Context, f Frame, send func(Frame)) {
	if !Ops[f.Tool] {
		send(Frame{Type: FrameResult, ID: f.ID, Status: http.StatusNotFound, Final: true,
			Chunk: []byte(`{"error":"not_found","message":"unknown operation"}`)})
		return
	}
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/"+f.Tool, bytes.NewReader(f.Params))
	req.Header.Set("Authorization", "Bearer "+c.HandlerToken)
	req.Header.Set("Content-Type", "application/json")
	fw := &frameWriter{id: f.ID, send: send, header: http.Header{}}
	c.Handler.ServeHTTP(fw, req)
	fw.finish()
}

// frameWriter is an http.ResponseWriter that emits result frames; each Write
// (and every Flush of exec output) becomes chunks of at most chunkSize.
type frameWriter struct {
	id     string
	send   func(Frame)
	header http.Header
	status int
	seq    int
	buf    bytes.Buffer
}

func (w *frameWriter) Header() http.Header { return w.header }

func (w *frameWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}

func (w *frameWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.buf.Write(b)
	if w.buf.Len() >= chunkSize {
		w.emit(false)
	}
	return len(b), nil
}

// Flush sends the buffered output: exec streams line by line.
func (w *frameWriter) Flush() { w.emit(false) }

func (w *frameWriter) emit(final bool) {
	for w.buf.Len() > 0 || final {
		n := w.buf.Len()
		if n > chunkSize {
			n = chunkSize
		}
		chunk := append([]byte(nil), w.buf.Next(n)...)
		w.seq++
		last := final && w.buf.Len() == 0
		f := Frame{Type: FrameResult, ID: w.id, Seq: w.seq, Chunk: chunk, Final: last}
		if w.seq == 1 {
			f.Status = w.status
		}
		w.send(f)
		if last || (!final && w.buf.Len() == 0) {
			return
		}
	}
}

func (w *frameWriter) finish() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.emit(true)
}
