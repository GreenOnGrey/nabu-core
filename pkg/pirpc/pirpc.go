// Package pirpc is a client of the Pi coding agent's RPC mode (`pi --mode rpc`).
//
// It starts the Pi process, writes commands as strict JSONL to its stdin,
// correlates responses by id and streams session events. It knows nothing about
// the application that uses it: the caller decides the binary, arguments,
// environment and working directory.
//
// Protocol notes (Pi 1.x):
//   - records are split only on LF, never on Unicode line separators, and a
//     record may be larger than any fixed buffer;
//   - stdout is read continuously: a slow event consumer never stalls Pi or
//     the responses to other commands;
//   - a successful prompt response only means the prompt was accepted; the run
//     ends with agent_settled, not agent_end (retries and compaction can follow);
//   - stderr carries diagnostics only and is never parsed.
package pirpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Options configure the Pi process.
type Options struct {
	// Binary is the Pi executable (for example "pi" or "/usr/local/bin/pi").
	Binary string
	// Args are the process arguments, for example "--mode", "rpc", "--no-approve".
	Args []string
	// Env is the complete process environment; nothing is inherited.
	Env []string
	// Dir is the working directory.
	Dir string
	// Stderr receives Pi's diagnostics in addition to the internal tail buffer.
	Stderr io.Writer
	// StderrTail bounds the stderr tail kept for exit errors (default 8 KiB).
	StderrTail int
}

// ErrClosed is returned for commands sent after the process has exited.
var ErrClosed = errors.New("pirpc: the pi process has exited")

// ExitError reports an unexpected exit of the Pi process.
type ExitError struct {
	Code       int
	StderrTail string
}

func (e *ExitError) Error() string {
	if e.StderrTail == "" {
		return fmt.Sprintf("pi exited with code %d", e.Code)
	}
	return fmt.Sprintf("pi exited with code %d: %s", e.Code, e.StderrTail)
}

// CommandError is a failed command response (success: false).
type CommandError struct {
	Command string
	Message string
}

func (e *CommandError) Error() string { return fmt.Sprintf("pi %s: %s", e.Command, e.Message) }

// Response is the response record of one command.
type Response struct {
	ID      string          `json:"id,omitempty"`
	Command string          `json:"command"`
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data,omitempty"`
	Error   string          `json:"error,omitempty"`
}

// Event is one session event. Type is always set; Raw holds the whole record,
// so event types this package does not know pass through unchanged.
type Event struct {
	Type string
	Raw  json.RawMessage
}

// Decode unmarshals the event record into v.
func (e Event) Decode(v any) error { return json.Unmarshal(e.Raw, v) }

// Client is a running Pi process in RPC mode.
type Client struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	wmu   sync.Mutex
	seq   atomic.Int64

	mu      sync.Mutex
	pending map[string]chan Response
	subs    map[*subscription]struct{}
	exited  bool

	events *subscription
	tail   *tailBuffer
	done   chan struct{}
	code   int
	err    error
}

// Start launches `Binary Args...` and begins reading its output. The process
// is killed when ctx is cancelled.
func Start(ctx context.Context, o Options) (*Client, error) {
	if o.Binary == "" {
		return nil, errors.New("pirpc: Options.Binary is required")
	}
	cmd := exec.CommandContext(ctx, o.Binary, o.Args...)
	cmd.Env = o.Env
	if cmd.Env == nil {
		cmd.Env = []string{} // nothing inherited
	}
	cmd.Dir = o.Dir
	cmd.WaitDelay = 5 * time.Second
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	n := o.StderrTail
	if n <= 0 {
		n = 8 << 10
	}
	tail := &tailBuffer{max: n}
	if o.Stderr != nil {
		cmd.Stderr = io.MultiWriter(tail, o.Stderr)
	} else {
		cmd.Stderr = tail
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("pirpc: start %s: %w", o.Binary, err)
	}
	c := &Client{cmd: cmd, stdin: stdin, pending: map[string]chan Response{}, subs: map[*subscription]struct{}{},
		tail: tail, done: make(chan struct{})}
	c.events = c.subscribe()
	go c.read(stdout)
	return c, nil
}

// read consumes stdout until EOF, dispatching responses and events.
func (c *Client) read(stdout io.Reader) {
	r := bufio.NewReaderSize(stdout, 64<<10)
	for {
		line, err := readRecord(r)
		if len(line) > 0 {
			c.dispatch(line)
		}
		if err != nil {
			break
		}
	}
	werr := c.cmd.Wait()
	c.mu.Lock()
	c.exited = true
	c.code = c.cmd.ProcessState.ExitCode()
	if werr != nil {
		var ee *exec.ExitError
		if !errors.As(werr, &ee) {
			c.err = werr
		}
	}
	pending := c.pending
	c.pending = map[string]chan Response{}
	subs := c.subs
	c.subs = map[*subscription]struct{}{}
	c.mu.Unlock()
	for _, ch := range pending {
		close(ch)
	}
	for s := range subs {
		s.close()
	}
	close(c.done)
}

// readRecord returns the next LF-terminated record without the LF (and a
// preceding CR). It never splits on Unicode separators and has no size limit.
func readRecord(r *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		chunk, err := r.ReadSlice('\n')
		buf = append(buf, chunk...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		buf = bytes.TrimSuffix(buf, []byte("\n"))
		buf = bytes.TrimSuffix(buf, []byte("\r"))
		return buf, err
	}
}

func (c *Client) dispatch(line []byte) {
	if len(bytes.TrimSpace(line)) == 0 {
		return
	}
	var head struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}
	if err := json.Unmarshal(line, &head); err != nil || head.Type == "" {
		return // not a protocol record; stdout is reserved for JSONL, so this is noise
	}
	if head.Type == "response" {
		var resp Response
		if json.Unmarshal(line, &resp) == nil {
			c.mu.Lock()
			ch, ok := c.pending[resp.ID]
			delete(c.pending, resp.ID)
			c.mu.Unlock()
			if ok {
				ch <- resp
				close(ch)
			}
		}
		return
	}
	ev := Event{Type: head.Type, Raw: append(json.RawMessage(nil), line...)}
	c.mu.Lock()
	subs := make([]*subscription, 0, len(c.subs))
	for s := range c.subs {
		subs = append(subs, s)
	}
	c.mu.Unlock()
	for _, s := range subs {
		s.push(ev)
	}
}

// Events is the stream of all session events of the process. It is closed
// when the process exits. Read it continuously or not at all: it buffers
// without bound and never blocks the reader.
func (c *Client) Events() <-chan Event { return c.events.out }

// Subscribe returns an additional event stream starting now; cancel stops it.
func (c *Client) Subscribe() (<-chan Event, func()) {
	s := c.subscribe()
	return s.out, func() { c.unsubscribe(s) }
}

func (c *Client) subscribe() *subscription {
	s := newSubscription()
	c.mu.Lock()
	if c.exited {
		c.mu.Unlock()
		s.close()
		return s
	}
	c.subs[s] = struct{}{}
	c.mu.Unlock()
	return s
}

func (c *Client) unsubscribe(s *subscription) {
	c.mu.Lock()
	_, ok := c.subs[s]
	delete(c.subs, s)
	c.mu.Unlock()
	if ok {
		s.close()
	}
}

// Command sends one command and waits for its response. fields are merged
// into the record next to "type" and "id".
func (c *Client) Command(ctx context.Context, typ string, fields map[string]any) (json.RawMessage, error) {
	id := "c" + strconv.FormatInt(c.seq.Add(1), 10)
	rec := map[string]any{}
	for k, v := range fields {
		rec[k] = v
	}
	rec["type"], rec["id"] = typ, id
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	ch := make(chan Response, 1)
	c.mu.Lock()
	if c.exited {
		c.mu.Unlock()
		return nil, c.exitError()
	}
	c.pending[id] = ch
	c.mu.Unlock()
	if err := c.write(append(b, '\n')); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}
	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, c.exitError()
		}
		if !resp.Success {
			return nil, &CommandError{Command: typ, Message: resp.Error}
		}
		return resp.Data, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (c *Client) write(b []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if _, err := c.stdin.Write(b); err != nil {
		select {
		case <-c.done:
			return c.exitError()
		default:
			return fmt.Errorf("pirpc: write: %w", err)
		}
	}
	return nil
}

// exitError describes why the process is gone.
func (c *Client) exitError() error {
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		return ErrClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return fmt.Errorf("%w: %v", ErrClosed, c.err)
	}
	return &ExitError{Code: c.code, StderrTail: c.tail.String()}
}

// Done is closed when the process has exited.
func (c *Client) Done() <-chan struct{} { return c.done }

// Close closes stdin (Pi's orderly shutdown) and waits for the process; after
// 10 seconds it kills it.
func (c *Client) Close() error {
	c.wmu.Lock()
	_ = c.stdin.Close()
	c.wmu.Unlock()
	select {
	case <-c.done:
	case <-time.After(10 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.done
	}
	return nil
}

// Wait blocks until the process exits and returns its exit code and the tail
// of its stderr.
func (c *Client) Wait() (int, []byte, error) {
	<-c.done
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.code, []byte(c.tail.String()), c.err
}

// StderrTail returns the last bytes Pi wrote to stderr.
func (c *Client) StderrTail() string { return c.tail.String() }

// ─── subscriptions ──────────────────────────────────────────────────

// subscription buffers events without bound, so the stdout reader never waits
// for a consumer (a blocked reader would also hold back command responses).
type subscription struct {
	mu     sync.Mutex
	queue  []Event
	closed bool
	wake   chan struct{}
	out    chan Event
	stop   chan struct{}
}

func newSubscription() *subscription {
	s := &subscription{wake: make(chan struct{}, 1), out: make(chan Event), stop: make(chan struct{})}
	go s.pump()
	return s
}

func (s *subscription) push(e Event) {
	s.mu.Lock()
	if !s.closed {
		s.queue = append(s.queue, e)
	}
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *subscription) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// pump delivers queued events in order; after close it drains the queue and
// closes out.
func (s *subscription) pump() {
	defer close(s.out)
	for {
		s.mu.Lock()
		if len(s.queue) == 0 {
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return
			}
			<-s.wake
			continue
		}
		e := s.queue[0]
		s.queue = s.queue[1:]
		s.mu.Unlock()
		select {
		case s.out <- e:
		case <-s.stop:
			return
		}
	}
}

// ─── stderr tail ────────────────────────────────────────────────────

type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > t.max {
		t.buf = append([]byte(nil), t.buf[len(t.buf)-t.max:]...)
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(bytes.TrimSpace(t.buf))
}
