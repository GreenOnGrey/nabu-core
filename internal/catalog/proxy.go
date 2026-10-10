package catalog

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
	s.userEmail = userEmail
	r.HandleFunc("/internal/v1/mcp-proxy/{item}", func(w http.ResponseWriter, r *http.Request) {
		s.proxy(w, r, hold)
	})
}

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
	h, prefix := pa.Header, pa.Prefix
	if h == "" {
		h, prefix = "Authorization", "Bearer "
	}
	return map[string]string{h: prefix + tok}, nil
}

// CallTool executes a confirmed call with the current credentials of the
// user (tech §6: :approve).
func (s *Service) CallTool(ctx context.Context, uid, itemID uuid.UUID, tool string, args json.RawMessage) (string, bool, error) {
	it, err := s.Get(ctx, itemID)
	if err != nil {
		return "", true, err
	}
	if !it.Published || it.InDevelopment {
		return "", true, ErrUnavailable()
	}
	if it.ReadOnly && !s.readOnlyTool(it, tool) {
		return readOnlyRefusal(tool), true, nil
	}
	headers, perr := s.itemHeaders(ctx, it, uid)
	if perr != nil {
		return perr.msg, true, nil
	}
	return mcp.CallTool(ctx, it.Source.URL, headers, tool, args)
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
	headers, perr := s.itemHeaders(r.Context(), it, uid)
	if perr != nil {
		rpcError(w, perr.status, perr.msg)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		rpcError(w, http.StatusBadRequest, "read error")
		return
	}
	var call struct {
		Method string `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	_ = json.Unmarshal(body, &call)
	// the read-only restriction of the item comes first: a refused call is never held for a confirmation
	if it.ReadOnly && call.Method == "tools/call" && !s.readOnlyTool(it, call.Params.Name) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{
			"isError": true, "content": []map[string]string{{"type": "text", "text": readOnlyRefusal(call.Params.Name)}}}})
		return
	}
	conv, _ := uuid.Parse(c.Conversation)
	if hold != nil && conv != uuid.Nil && uid != uuid.Nil && call.Method == "tools/call" && !s.readOnlyTool(it, call.Params.Name) {
		// R9: in a mail topic a call that changes data waits for the user (ML-17, ML-20)
		var p struct {
			ID     json.RawMessage `json:"id"`
			Params struct {
				Arguments json.RawMessage `json:"arguments"`
			} `json:"params"`
		}
		_ = json.Unmarshal(body, &p)
		if answer, held := hold(r.Context(), conv, uid, it.ID, it.Name, call.Params.Name, p.Params.Arguments); held {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": p.ID, "result": map[string]any{
				"isError": false, "content": []map[string]string{{"type": "text", "text": answer}}}})
			return
		}
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
	resp, err := http.DefaultClient.Do(up)
	if err != nil {
		slog.WarnContext(r.Context(), "mcp proxy", "item", it.Name, "err", err)
		rpcError(w, http.StatusBadGateway, "the MCP server is unreachable")
		return
	}
	defer resp.Body.Close()
	for _, h := range []string{"Content-Type", "Mcp-Session-Id", "Cache-Control"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	filter := it.ReadOnly && call.Method == "tools/list"
	w.WriteHeader(resp.StatusCode)
	if !filter {
		fl, _ := w.(http.Flusher)
		buf := make([]byte, 32<<10)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				_, _ = w.Write(buf[:n])
				if fl != nil {
					fl.Flush()
				}
			}
			if err != nil {
				return
			}
		}
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
	for _, t := range it.Tools {
		if t.Name == name {
			return t.ReadOnly
		}
	}
	return false
}

// filterTools keeps only tools with readOnlyHint in a tools/list result (CAT-04).
func filterTools(raw []byte) []byte {
	var msg map[string]json.RawMessage
	if json.Unmarshal(raw, &msg) != nil {
		return raw
	}
	res, ok := msg["result"]
	if !ok {
		return raw
	}
	var r map[string]json.RawMessage
	if json.Unmarshal(res, &r) != nil {
		return raw
	}
	var tools []map[string]json.RawMessage
	if json.Unmarshal(r["tools"], &tools) != nil {
		return raw
	}
	kept := []map[string]json.RawMessage{}
	for _, t := range tools {
		var ann struct {
			ReadOnlyHint *bool `json:"readOnlyHint"`
		}
		_ = json.Unmarshal(t["annotations"], &ann)
		if ann.ReadOnlyHint != nil && *ann.ReadOnlyHint {
			kept = append(kept, t)
		}
	}
	r["tools"], _ = json.Marshal(kept)
	msg["result"], _ = json.Marshal(r)
	out, _ := json.Marshal(msg)
	return out
}
