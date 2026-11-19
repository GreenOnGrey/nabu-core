package catalog

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
)

// The waits of a run of the collector over the rate limit of its source (FTR.NAB.CMN-0003 decision 54). One wait is
// at most five minutes; the whole call, the answers of the source with the waits, stays within six, below the time
// Pi waits for the answer of the source (engine.collectorSourceTimeout, 7 minutes); and the run keeps a minute for
// its own work after a wait.
const (
	runRateLimitWaitMax = 5 * time.Minute
	runCallBudget       = 6 * time.Minute
	runRateLimitMargin  = time.Minute
)

// rateLimitStop is what a run of the collector reads when the source keeps its rate limit after the wait: a model
// that got only the text of the server called the tool again every two seconds until the run ran out of time.
const rateLimitStop = "The source limits the rate of requests, and the wait for it is over. Do not call the source again " +
	"in this run and do not retry: save notes only for the threads you have already read, then end the run. " +
	"The other threads of the batch are read at the next collection."

// memoryRunCall forwards a tools/call of a run of the collector of communications and holds it over the rate limit
// of the source (FTR.NAB.CMN-0003 decision 54). The collection of the next tick and the run read the source with the
// same access of the user, so they share its limit; a server of a company system answers it in the result of the
// tool and names the wait. The proxy waits as the server says while the run has time, and calls again. A limit that
// stays marks the batch: it fails without remembering its unread messages, and the next tick repeats it.
func (s *Service) memoryRunCall(w http.ResponseWriter, r *http.Request, it *Item, headers map[string]string, body []byte,
	id json.RawMessage, tool string, uid, run uuid.UUID, deadline time.Time) {
	ctx := r.Context()
	start := time.Now()
	for {
		resp, err := s.forwardCall(ctx, r, it, headers, body)
		if err != nil {
			slog.WarnContext(ctx, "mcp proxy", "item", it.Name, "err", err)
			observe(it, tool, "transport_error", time.Since(start))
			rpcError(w, http.StatusBadGateway, "the MCP server is unreachable")
			return
		}
		text, isErr := callText(resp.raw)
		after, limited := time.Duration(0), false
		switch {
		case resp.status == http.StatusTooManyRequests:
			limited = true
			if after = retryAfterHeader(resp.header.Get("Retry-After")); after <= 0 {
				after, _ = rateLimitAnswer(text + " " + string(resp.raw))
			}
		case isErr:
			after, limited = rateLimitAnswer(text)
		}
		if !limited {
			copyAnswerHeaders(w, resp.header)
			observe(it, tool, callResult(resp.status, isErr, nil), time.Since(start))
			w.WriteHeader(resp.status)
			_, _ = w.Write(resp.raw)
			return
		}
		if after > 0 && after <= runRateLimitWaitMax && time.Since(start)+after <= runCallBudget && time.Until(deadline) >= after+runRateLimitMargin {
			select {
			case <-ctx.Done():
				return
			case <-time.After(after):
			}
			continue
		}
		observe(it, tool, "rate_limited", time.Since(start))
		if s.MemoryRateLimited != nil {
			if err := s.MemoryRateLimited(context.WithoutCancel(ctx), uid, run); err != nil {
				slog.ErrorContext(ctx, "mcp proxy: mark the batch", "run", run, "err", err)
			}
		}
		toolError(w, id, rateLimitStop)
		return
	}
}

type forwarded struct {
	status int
	header http.Header
	raw    []byte
}

// forwardCall sends the call to the server of the item and reads the answer whole.
func (s *Service) forwardCall(ctx context.Context, r *http.Request, it *Item, headers map[string]string, body []byte) (*forwarded, error) {
	up, err := http.NewRequestWithContext(ctx, r.Method, it.Source.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	setUpstreamHeaders(up, r, headers)
	resp, err := http.DefaultClient.Do(up)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	return &forwarded{status: resp.StatusCode, header: resp.Header, raw: raw}, nil
}

// retryAfterHeader reads Retry-After: seconds or an HTTP date; 0 — none.
func retryAfterHeader(v string) time.Duration {
	v = strings.TrimSpace(v)
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d.Round(time.Second)
		}
	}
	return 0
}

// callText reads the text of the answer to a tools/call, a JSON message or a stream of events, and whether it is
// an error: the last message with a result or an error is the answer.
func callText(raw []byte) (string, bool) {
	var msg struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	parse := func(b []byte) bool {
		msg.Result, msg.Error = nil, nil
		return json.Unmarshal(bytes.TrimSpace(b), &msg) == nil && (msg.Result != nil || msg.Error != nil)
	}
	found := parse(raw)
	if !found {
		sc := bufio.NewScanner(bytes.NewReader(raw))
		sc.Buffer(nil, 16<<20)
		var last []byte
		for sc.Scan() {
			if d, ok := bytes.CutPrefix(sc.Bytes(), []byte("data:")); ok {
				if parse(d) {
					last = append(last[:0], d...)
				}
			}
		}
		found = last != nil && parse(last)
	}
	if !found {
		return "", false
	}
	if msg.Error != nil {
		return msg.Error.Message, true
	}
	return mcp.ResultText(msg.Result)
}
