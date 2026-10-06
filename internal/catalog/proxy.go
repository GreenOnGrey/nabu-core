package catalog

import (
	"bufio"
	"bytes"
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
)

// DelegationHeader is the header with the email Nabu acts for.
const DelegationHeader = "Nabu-On-Behalf-Of"

// UserEmail loads the email of a user (delegation to products).
type UserEmail func(r *http.Request, uid uuid.UUID) (email string, allowed bool)

// ProxyRoutes mounts /internal/v1/mcp-proxy/{item}: the MCP servers of the
// catalog as sessions see them. The proxy checks the session token, adds the
// user's or the platform credentials and filters tools of read-only items.
func (s *Service) ProxyRoutes(r chi.Router, userEmail UserEmail) {
	r.HandleFunc("/internal/v1/mcp-proxy/{item}", func(w http.ResponseWriter, r *http.Request) {
		s.proxy(w, r, userEmail)
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

func (s *Service) proxy(w http.ResponseWriter, r *http.Request, userEmail UserEmail) {
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
	headers := map[string]string{}
	var platEnc []byte
	_ = s.Pool.QueryRow(r.Context(), `SELECT platform_auth_enc FROM catalog_items WHERE id = $1`, itemID).Scan(&platEnc)
	uid, _ := uuid.Parse(c.User)
	switch {
	case it.Mode != nil && *it.Mode == "platform":
		h, err := s.platformHeaders(platEnc)
		if err != nil {
			rpcError(w, http.StatusBadGateway, "platform credentials unreadable")
			return
		}
		headers = h
		if uid != uuid.Nil {
			if _, ok := userEmail(r, uid); !ok {
				rpcError(w, http.StatusForbidden, "user_blocked")
				return
			}
			if !s.connected(r, uid, itemID) {
				rpcError(w, http.StatusForbidden, "the item is not connected")
				return
			}
		}
	case uid == uuid.Nil:
		rpcError(w, http.StatusForbidden, "personal servers need a user")
		return
	default:
		email, ok := userEmail(r, uid)
		if !ok {
			rpcError(w, http.StatusForbidden, "user_blocked")
			return
		}
		if !s.connected(r, uid, itemID) {
			rpcError(w, http.StatusForbidden, "the access was revoked") // CAT-03
			return
		}
		pa := it.PersonalAuth
		switch pa.Kind {
		case "delegation":
			headers = s.delegationHeaders(it, email)
		default:
			tok, err := s.userToken(r.Context(), uid, it)
			if err != nil {
				rpcError(w, http.StatusUnauthorized, "the personal access is missing or expired; connect the item again in Connections")
				return
			}
			h, prefix := pa.Header, pa.Prefix
			if h == "" {
				h, prefix = "Authorization", "Bearer "
			}
			headers[h] = prefix + tok
		}
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
	if it.ReadOnly && call.Method == "tools/call" && !s.readOnlyTool(it, call.Params.Name) {
		var req struct {
			ID json.RawMessage `json:"id"`
		}
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": map[string]any{
			"isError": true, "content": []map[string]string{{"type": "text", "text": "The tool " + call.Params.Name + " changes data; this server is read-only in Nabu."}}}})
		return
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

func (s *Service) connected(r *http.Request, uid, item uuid.UUID) bool {
	var n int
	_ = s.Pool.QueryRow(r.Context(), `SELECT count(*) FROM user_connections WHERE user_id = $1 AND item_id = $2`, uid, item).Scan(&n)
	return n > 0
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
