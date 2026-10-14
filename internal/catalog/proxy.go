package catalog

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/platform/httpx"
	"github.com/GreenOnGrey/nabu-core/internal/platform/jwt"
	"github.com/GreenOnGrey/nabu-core/internal/platform/mcp"
	"github.com/GreenOnGrey/nabu-core/internal/platform/metrics"
)

// DelegationHeader is the header with the email Nabu acts for.
const DelegationHeader = "Nabu-On-Behalf-Of"

// UserEmail loads the email of a user (delegation to products); false — the
// user may not use the agent (blocked, archived).
type UserEmail func(ctx context.Context, uid uuid.UUID) (email string, allowed bool)

// Hold keeps a call of a tool that changes data in a conversation whose
// writes require confirmation (FTR.NAB.CMN-0002 R9, tech §6); held — the
// answer to the agent.
type Hold func(ctx context.Context, conv, uid uuid.UUID, item uuid.UUID, server, tool string, args json.RawMessage) (answer string, held bool)

// ProxyRoutes mounts /internal/v1/mcp-proxy/{item}: the MCP servers of the
// catalog as sessions see them. The proxy checks the session token, adds the
// user's or the platform credentials and filters tools of read-only items.
func (s *Service) ProxyRoutes(r chi.Router, userEmail UserEmail, hold Hold) {
	s.SetUserEmail(userEmail)
	r.HandleFunc("/internal/v1/mcp-proxy/{item}", func(w http.ResponseWriter, r *http.Request) {
		s.proxy(w, r, hold)
	})
}

// SetUserEmail tells the catalog how to find the email of a user and whether
// the user may use the agent: the worker calls sources without the proxy
// routes (FTR.NAB.CMN-0003 arch §3.1).
func (s *Service) SetUserEmail(fn UserEmail) { s.userEmail = fn }

// delegationHeaders sign a Nabu JWT for the product of a delegation item and
// name the user (R23: the product checks the signature by Nabu's JWKS).
func (s *Service) delegationHeaders(it *Item, email string) map[string]string {
	aud := it.PersonalAuth.Audience
	if aud == "" {
		aud = it.Name
	}
	return map[string]string{
		"Authorization":  "Bearer " + s.Signer.Issue(jwt.Claims{Audience: aud, Subject: "nabu", Email: email}, 10*time.Minute),
		DelegationHeader: email,
	}
}

func rpcError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": nil, "error": map[string]any{"code": -32001, "message": msg}})
}

type proxyErr struct {
	status int
	msg    string
}

// itemHeaders are the credentials of a call: platform ones, or the personal
// access of the user (delegation, OAuth or a token).
func (s *Service) itemHeaders(ctx context.Context, it *Item, uid uuid.UUID) (map[string]string, *proxyErr) {
	var platEnc []byte
	_ = s.Pool.QueryRow(ctx, `SELECT platform_auth_enc FROM catalog_items WHERE id = $1`, it.ID).Scan(&platEnc)
	email := func() (string, bool) {
		if s.userEmail == nil {
			return "", false
		}
		return s.userEmail(ctx, uid)
	}
	switch {
	case it.Mode != nil && *it.Mode == "platform":
		h, err := s.platformHeaders(platEnc)
		if err != nil {
			return nil, &proxyErr{http.StatusBadGateway, "platform credentials unreadable"}
		}
		if uid != uuid.Nil {
			if _, ok := email(); !ok {
				return nil, &proxyErr{http.StatusForbidden, "user_blocked"}
			}
			if !s.connected(ctx, uid, it.ID) {
				return nil, &proxyErr{http.StatusForbidden, "the item is not connected"}
			}
		}
		return h, nil
	case uid == uuid.Nil:
		return nil, &proxyErr{http.StatusForbidden, "personal servers need a user"}
	}
	mail, ok := email()
	if !ok {
		return nil, &proxyErr{http.StatusForbidden, "user_blocked"}
	}
	if !s.connected(ctx, uid, it.ID) {
		return nil, &proxyErr{http.StatusForbidden, "the access was revoked"} // CAT-03
	}
	pa := it.PersonalAuth
	if pa.Kind == "delegation" {
		return s.delegationHeaders(it, mail), nil
	}
	tok, err := s.userToken(ctx, uid, it)
	if err != nil {
		return nil, &proxyErr{http.StatusUnauthorized, "the personal access is missing or expired; connect the item again in Connections"}
	}
	return pa.tokenHeaders(tok), nil
}

// tokenHeaders place a personal token: Authorization: Bearer <token> by default.
func (pa *PersonalAuth) tokenHeaders(tok string) map[string]string {
	h, prefix := pa.Header, pa.Prefix
	if h == "" {
		h, prefix = "Authorization", "Bearer "
	}
	return map[string]string{h: prefix + tok}
}

// CallTool executes a confirmed call with the current credentials of the
// user (tech §6: :approve). File arguments are replaced at the moment of the
// call, not when the call waits for the confirmation (FIL-10).
func (s *Service) CallTool(ctx context.Context, uid, itemID uuid.UUID, tool string, args json.RawMessage) (string, bool, error) {
	it, err := s.Get(ctx, itemID)
	if err != nil {
		return "", true, err
	}
	if !it.Published || it.InDevelopment {
		return "", true, ErrUnavailable()
	}
	if !it.allowed(tool) {
		observe(it, tool, "denied", 0)
		return refusal(it, tool), true, nil
	}
	headers, perr := s.itemHeaders(ctx, it, uid)
	if perr != nil {
		observe(it, tool, "auth", 0)
		return perr.msg, true, nil
	}
	args, fr := s.rewriteFileArgs(ctx, it, uid, tool, args)
	if fr != nil {
		observe(it, tool, fr.result, 0)
		return fr.text, true, nil
	}
	start := time.Now()
	text, isErr, err := mcp.CallTool(ctx, it.Source.URL, headers, tool, args)
	observe(it, tool, callResult(statusOf(err), isErr, err), time.Since(start))
	return text, isErr, err
}

// refusal is the answer to a call of a tool the agent may not use.
func refusal(it *Item, tool string) string {
	if it.AllowedTools == nil && it.ReadOnly {
		return readOnlyRefusal(tool)
	}
	return deniedRefusal(tool)
}

// callResult is the result label of a call (arch §4.3): a server that refused
// the credentials (401, 403) is "auth", other failures to reach it —
// "transport_error".
func callResult(status int, isErr bool, err error) string {
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "auth"
	case err != nil || status >= 300:
		return "transport_error"
	case isErr:
		return "tool_error"
	}
	return "ok"
}

// statusOf finds the HTTP status in an error of mcp.CallTool ("…: HTTP 401").
func statusOf(err error) int {
	if err == nil {
		return 0
	}
	if i := strings.LastIndex(err.Error(), "HTTP "); i >= 0 {
		var n int
		if _, e := fmt.Sscanf(err.Error()[i:], "HTTP %d", &n); e == nil {
			return n
		}
	}
	return 0
}

// observe counts a call of a tool (FTR.NAB.CMN-0006 R15, OBS-01).
func observe(it *Item, tool, result string, d time.Duration) {
	name := it.metricTool(tool)
	metrics.MCPProxyCalls.WithLabelValues(it.Name, name, result).Inc()
	if d > 0 {
		metrics.MCPProxyCallSeconds.WithLabelValues(it.Name, name).Observe(d.Seconds())
	}
}

// toolError answers a tools/call with a result the agent reads as an error
// of the tool.
func toolError(w http.ResponseWriter, id json.RawMessage, text string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{
		"isError": true, "content": []map[string]string{{"type": "text", "text": text}}}})
}

func (s *Service) proxy(w http.ResponseWriter, r *http.Request, hold Hold) {
	itemID, err := uuid.Parse(chi.URLParam(r, "item"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	c, err := s.Signer.Verify(httpx.Bearer(r), jwt.AudMCP)
	if err != nil || c.Subject != "proxy:"+itemID.String() {
		rpcError(w, http.StatusUnauthorized, "invalid token")
		return
	}
	it, err := s.Get(r.Context(), itemID)
	if err != nil || !it.Published || it.InDevelopment {
		rpcError(w, http.StatusConflict, "catalog_item_unavailable: the item was unpublished") // CAT-08
		return
	}
	uid, _ := uuid.Parse(c.User)
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		rpcError(w, http.StatusBadRequest, "read error")
		return
	}
	var call struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
		Params struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		} `json:"params"`
	}
	_ = json.Unmarshal(body, &call)
	isCall := call.Method == "tools/call"
	headers, perr := s.itemHeaders(r.Context(), it, uid)
	if perr != nil {
		if isCall {
			observe(it, call.Params.Name, "auth", 0)
		}
		rpcError(w, perr.status, perr.msg)
		return
	}
	// the tools the administrator allowed come first: a refused call is never
	// held for a confirmation (CAT-02)
	if isCall && !it.allowed(call.Params.Name) {
		observe(it, call.Params.Name, "denied", 0)
		toolError(w, call.ID, refusal(it, call.Params.Name))
		return
	}
	// FTR.NAB.CMN-0003 tech §6.4: a run of the collector of communications
	// reaches only the item of its batch, only the read tools of the adapter
	// and only the threads of the batch.
	var only []string
	if c.HasScope(mcp.ScopeMemoryRun) {
		run, _ := uuid.Parse(c.Run)
		if s.MemoryRun == nil {
			rpcError(w, http.StatusForbidden, "collector runs are not enabled")
			return
		}
		tools, reason := s.MemoryRun(r.Context(), uid, run, it.ID, call.Method, call.Params.Name, call.Params.Arguments)
		if reason != "" {
			if !isCall {
				rpcError(w, http.StatusForbidden, reason)
				return
			}
			toolError(w, call.ID, reason)
			return
		}
		only = tools
	}
	conv, _ := uuid.Parse(c.Conversation)
	if hold != nil && conv != uuid.Nil && uid != uuid.Nil && isCall && !s.readOnlyTool(it, call.Params.Name) {
		// R9: in a mail topic a call that changes data waits for the user (ML-17, ML-20)
		if answer, held := hold(r.Context(), conv, uid, it.ID, it.Name, call.Params.Name, call.Params.Arguments); held {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": map[string]any{
				"isError": false, "content": []map[string]string{{"type": "text", "text": answer}}}})
			return
		}
	}
	if isCall && len(it.fileArgsOf(call.Params.Name)) > 0 {
		args, fr := s.rewriteFileArgs(r.Context(), it, uid, call.Params.Name, call.Params.Arguments)
		if fr != nil {
			observe(it, call.Params.Name, fr.result, 0)
			toolError(w, call.ID, fr.text)
			return
		}
		body = withArguments(body, args)
	}
	up, err := http.NewRequestWithContext(r.Context(), r.Method, it.Source.URL, bytes.NewReader(body))
	if err != nil {
		rpcError(w, http.StatusBadGateway, err.Error())
		return
	}
	for _, h := range []string{"Content-Type", "Accept", "Mcp-Session-Id", "Mcp-Protocol-Version", "Last-Event-Id"} {
		if v := r.Header.Get(h); v != "" {
			up.Header.Set(h, v)
		}
	}
	for k, v := range headers {
		up.Header.Set(k, v)
	}
	start := time.Now()
	resp, err := http.DefaultClient.Do(up)
	if err != nil {
		slog.WarnContext(r.Context(), "mcp proxy", "item", it.Name, "err", err)
		if isCall {
			observe(it, call.Params.Name, "transport_error", time.Since(start))
		}
		rpcError(w, http.StatusBadGateway, "the MCP server is unreachable")
		return
	}
	defer resp.Body.Close()
	for _, h := range []string{"Content-Type", "Mcp-Session-Id", "Cache-Control"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	filter := call.Method == "tools/list" && (it.AllowedTools != nil || it.ReadOnly || only != nil || len(it.FileArgs) > 0)
	filterTools := func(raw []byte) []byte { return filterToolsList(raw, it, only) }
	w.WriteHeader(resp.StatusCode)
	if !filter {
		// a call is streamed through; its last message tells the result (OBS-01)
		var seen resultSniffer
		fl, _ := w.(http.Flusher)
		buf := make([]byte, 32<<10)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				_, _ = w.Write(buf[:n])
				if fl != nil {
					fl.Flush()
				}
				if isCall {
					seen.write(buf[:n])
				}
			}
			if err != nil {
				break
			}
		}
		if isCall {
			observe(it, call.Params.Name, callResult(resp.StatusCode, seen.isError(), nil), time.Since(start))
		}
		return
	}
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(nil, 8<<20)
		for sc.Scan() {
			line := sc.Text()
			if data, ok := strings.CutPrefix(line, "data:"); ok {
				line = "data: " + string(filterTools([]byte(strings.TrimSpace(data))))
			}
			_, _ = io.WriteString(w, line+"\n")
		}
		return
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	_, _ = w.Write(filterTools(raw))
}

// withArguments replaces params.arguments of a JSON-RPC message.
func withArguments(body []byte, args json.RawMessage) []byte {
	var msg map[string]json.RawMessage
	if json.Unmarshal(body, &msg) != nil {
		return body
	}
	var params map[string]json.RawMessage
	if json.Unmarshal(msg["params"], &params) != nil {
		return body
	}
	params["arguments"] = args
	msg["params"], _ = json.Marshal(params)
	out, _ := json.Marshal(msg)
	return out
}

// resultSniffer keeps the tail of an answer to find "isError": true in the
// result of a call, plain JSON or the last event of a stream.
type resultSniffer struct{ tail []byte }

func (r *resultSniffer) write(p []byte) {
	r.tail = append(r.tail, p...)
	if len(r.tail) > 64<<10 {
		r.tail = r.tail[len(r.tail)-64<<10:]
	}
}

func (r *resultSniffer) isError() bool {
	t := string(r.tail)
	if i := strings.LastIndex(t, "data:"); i >= 0 {
		t = t[i+len("data:"):]
	}
	var msg struct {
		Error  json.RawMessage `json:"error"`
		Result struct {
			IsError bool `json:"isError"`
		} `json:"result"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(t)), &msg) != nil {
		return strings.Contains(t, `"isError":true`) || strings.Contains(t, `"isError": true`)
	}
	return msg.Result.IsError || len(msg.Error) > 0
}

func (s *Service) connected(ctx context.Context, uid, item uuid.UUID) bool {
	var n int
	_ = s.Pool.QueryRow(ctx, `SELECT count(*) FROM user_connections WHERE user_id = $1 AND item_id = $2`, uid, item).Scan(&n)
	return n > 0
}

func readOnlyRefusal(tool string) string {
	return "The tool " + tool + " changes data; this server is read-only in Nabu."
}

// readOnlyTool reports whether the tool is marked read-only in the stored
// list of the item's tools (the administrator's check).
func (s *Service) readOnlyTool(it *Item, name string) bool {
	t, ok := it.tool(name)
	return ok && t.ReadOnly
}

// ErrSourceAccess reports that the personal access of the user to the server
// was revoked, expired or refused (FTR.NAB.CMN-0003 R17).
var ErrSourceAccess = errors.New("no access to the source")

// CallSource calls a tool of a personal MCP server with the access of the
// user, outside a session of an agent: the adapters of the sources of
// communications learn what changed with it (FTR.NAB.CMN-0003 arch §3.1).
// It returns the text of the result.
func (s *Service) CallSource(ctx context.Context, uid, itemID uuid.UUID, tool string, args json.RawMessage) (string, error) {
	it, err := s.Get(ctx, itemID)
	if err != nil {
		return "", err
	}
	if !it.Published || it.InDevelopment {
		return "", ErrUnavailable()
	}
	if !it.allowed(tool) {
		// the adapter calls only the tools the administrator allowed (GML-09)
		return "", fmt.Errorf("the tool %s is not allowed in the item %s", tool, it.Name)
	}
	headers, perr := s.itemHeaders(ctx, it, uid)
	if perr != nil {
		if perr.status == http.StatusUnauthorized || perr.status == http.StatusForbidden {
			return "", ErrSourceAccess
		}
		return "", errors.New(perr.msg)
	}
	text, isErr, err := mcp.CallTool(ctx, it.Source.URL, headers, tool, args)
	if err != nil {
		if strings.Contains(err.Error(), "HTTP 401") || strings.Contains(err.Error(), "HTTP 403") {
			return "", ErrSourceAccess
		}
		return "", err
	}
	if isErr {
		return "", fmt.Errorf("the tool %s failed: %s", tool, text)
	}
	return text, nil
}
