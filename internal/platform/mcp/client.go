package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// CallTool calls one tool of a Streamable HTTP MCP server: initialize,
// notifications/initialized, tools/call (FTR.NAB.CMN-0002 tech §6: a
// confirmed call is executed by api). It returns the text content of the
// result and whether the tool reported an error.
func CallTool(ctx context.Context, url string, headers map[string]string, tool string, args json.RawMessage) (string, bool, error) {
	c := &http.Client{Timeout: 2 * time.Minute}
	session := ""
	send := func(id int, method string, params any) (json.RawMessage, error) {
		body := map[string]any{"jsonrpc": "2.0", "method": method}
		if id > 0 {
			body["id"] = id
		}
		if params != nil {
			body["params"] = params
		}
		b, _ := json.Marshal(body)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("Mcp-Protocol-Version", ProtocolVersion)
		if session != "" {
			req.Header.Set("Mcp-Session-Id", session)
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := c.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if s := resp.Header.Get("Mcp-Session-Id"); s != "" {
			session = s
		}
		if resp.StatusCode == http.StatusAccepted || id == 0 {
			return nil, nil
		}
		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("mcp %s: HTTP %d", method, resp.StatusCode)
		}
		var raw []byte
		if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
			sc := bufio.NewScanner(resp.Body)
			sc.Buffer(nil, 8<<20)
			for sc.Scan() {
				if d, ok := strings.CutPrefix(sc.Text(), "data:"); ok {
					d = strings.TrimSpace(d)
					var probe struct {
						ID json.RawMessage `json:"id"`
					}
					if json.Unmarshal([]byte(d), &probe) == nil && string(probe.ID) == fmt.Sprint(id) {
						raw = []byte(d)
						break
					}
				}
			}
		} else {
			raw, _ = io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		}
		var msg struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &msg); err != nil {
			return nil, fmt.Errorf("mcp %s: unreadable answer", method)
		}
		if msg.Error != nil {
			return nil, errors.New("mcp " + method + ": " + msg.Error.Message)
		}
		return msg.Result, nil
	}
	if _, err := send(1, "initialize", map[string]any{"protocolVersion": ProtocolVersion, "capabilities": map[string]any{},
		"clientInfo": map[string]string{"name": "nabu", "version": "1.0.0"}}); err != nil {
		return "", false, err
	}
	_, _ = send(0, "notifications/initialized", nil)
	if len(args) == 0 {
		args = json.RawMessage("{}")
	}
	res, err := send(2, "tools/call", map[string]any{"name": tool, "arguments": args})
	if err != nil {
		return "", false, err
	}
	text, isErr := ResultText(res)
	return text, isErr, nil
}

// ResultText joins the text content of a tools/call result.
func ResultText(res json.RawMessage) (string, bool) {
	var r struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		IsError bool `json:"isError"`
	}
	_ = json.Unmarshal(res, &r)
	var parts []string
	for _, c := range r.Content {
		if c.Type == "text" {
			parts = append(parts, c.Text)
		}
	}
	return strings.Join(parts, "\n"), r.IsError
}
