package comm

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/GreenOnGrey/nabu-core/internal/apperr"
	"github.com/GreenOnGrey/nabu-core/internal/collect"
)

// Source is a source of communications an adapter gives (arch §3.2).
type Source = collect.Source

// adapterSources names the sources of an adapter; a catalog item can be
// marked as a source of communications only when its adapter exists (R1).
func adapterSources(kind string) ([]Source, bool) {
	a, ok := collect.Get(kind)
	if !ok {
		return nil, false
	}
	return a.Sources(), true
}

// Adapters lists the adapters, for the administration.
func Adapters() []string { return collect.Kinds() }

// SetSource marks a catalog item as a source of communications (R1, R29):
// only a personal MCP server whose adapter exists.
func (s *Service) SetSource(ctx context.Context, item uuid.UUID, adapter *string, on *bool) error {
	var typ string
	var mode, cur *string
	var marked bool
	err := s.Pool.QueryRow(ctx, `SELECT type, mode, adapter, comm_source FROM catalog_items WHERE id = $1`, item).Scan(&typ, &mode, &cur, &marked)
	if err != nil {
		return apperr.NotFound("not_found", "catalog item not found")
	}
	if adapter != nil {
		cur = adapter
		if *adapter == "" {
			cur = nil
		}
	}
	if on != nil {
		marked = *on
	}
	if cur != nil {
		if _, ok := adapterSources(*cur); !ok {
			return apperr.Conflict("comm_adapter_missing", "there is no adapter "+*cur+" for sources of communications")
		}
	}
	if marked && (cur == nil || typ != "mcp" || mode == nil || *mode != "personal") {
		return apperr.Conflict("comm_adapter_missing", "only a personal MCP server with an adapter can be a source of communications")
	}
	if marked {
		// SRC-03: the server must have the tools the adapter calls — by the list
		// its last check stored; a server that was never checked passes. With a
		// list of allowed tools only those count (FTR.NAB.CMN-0006 GML-09).
		var raw, allowedRaw []byte
		_ = s.Pool.QueryRow(ctx, `SELECT tools, allowed_tools FROM catalog_items WHERE id = $1`, item).Scan(&raw, &allowedRaw)
		var tools []struct {
			Name string `json:"name"`
		}
		var allowed []string
		hasList := len(allowedRaw) > 0 && json.Unmarshal(allowedRaw, &allowed) == nil
		if json.Unmarshal(raw, &tools) == nil && len(tools) > 0 {
			names := make([]string, 0, len(tools))
			for _, t := range tools {
				if !hasList || slices.Contains(allowed, t.Name) {
					names = append(names, t.Name)
				}
			}
			ad, _ := collect.Get(*cur)
			if err := ad.CheckTools(names); err != nil {
				return apperr.Conflict("comm_source_tools_missing", "the server cannot be a source of communications: "+err.Error())
			}
		}
	}
	_, err = s.Pool.Exec(ctx, `UPDATE catalog_items SET adapter = $2, comm_source = $3, updated_at = now() WHERE id = $1`, item, cur, marked)
	return err
}

// itemTools are the tools of an item its last check stored, only the
// allowed ones when there is a list; checked — there was a check.
func itemTools(raw, allowedRaw []byte) ([]string, bool) {
	var tools []struct {
		Name string `json:"name"`
	}
	var allowed []string
	hasList := len(allowedRaw) > 0 && json.Unmarshal(allowedRaw, &allowed) == nil
	if json.Unmarshal(raw, &tools) != nil || len(tools) == 0 {
		return nil, false
	}
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		if !hasList || slices.Contains(allowed, t.Name) {
			names = append(names, t.Name)
		}
	}
	return names, true
}

// MutedGroup is a group of VK Teams with the notifications off.
type MutedGroup struct {
	ChatSN string `json:"chatSn"`
	Name   string `json:"name"`
}

// mutedGroups lists the muted groups of the user whose name has q
// (FTR.NAB.ADM-0001 tech §3.2), up to 50.
func (a *API) mutedGroups(ctx context.Context, uid, item uuid.UUID, q string) ([]MutedGroup, error) {
	srcs, err := a.Store.Sources(ctx, uid)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(srcs, func(x SourceState) bool { return x.ItemID == item && x.Source == "vkteams" })
	if i < 0 {
		return nil, apperr.NotFound("not_found", "the item gives no source VK Teams")
	}
	if !srcs[i].Connected {
		return nil, apperr.Conflict("comm_source_not_connected", "the source is not connected: connect it in Connections")
	}
	if a.CallSource == nil {
		return nil, apperr.Unavailable("source_unavailable", "the source is not reachable")
	}
	out := []MutedGroup{}
	cursor := ""
	for page := 0; page < 10 && len(out) < 50; page++ {
		args := map[string]any{"include_types": []string{"group"}, "limit": 100}
		if q = strings.TrimSpace(q); q != "" {
			args["name_query"] = q
		}
		if cursor != "" {
			args["cursor"] = cursor
		}
		raw, _ := json.Marshal(args)
		text, err := a.CallSource(ctx, uid, item, "messenger-list-chats", raw)
		if err != nil {
			if errors.Is(err, collect.ErrAccess) {
				return nil, apperr.Conflict("source_access", "no access to the source: connect it again in Connections")
			}
			return nil, apperr.Unavailable("source_unavailable", "the source did not answer")
		}
		var res struct {
			Chats []struct {
				ChatSN        string `json:"chat_sn"`
				Name          string `json:"name"`
				Notifications *bool  `json:"notifications_enabled"`
			} `json:"chats"`
			NextCursor *string `json:"next_cursor"`
		}
		if json.Unmarshal([]byte(text), &res) != nil {
			return nil, apperr.Unavailable("source_unavailable", "the source gave an unexpected answer")
		}
		for _, c := range res.Chats {
			if c.Notifications != nil && !*c.Notifications && len(out) < 50 {
				out = append(out, MutedGroup{ChatSN: c.ChatSN, Name: c.Name})
			}
		}
		if res.NextCursor == nil || *res.NextCursor == "" {
			break
		}
		cursor = *res.NextCursor
	}
	return out, nil
}

// Sources lists the sources the user can collect from: the sources of the
// catalog items marked by the administrator, with whether the user connected
// the item.
func (s *Service) Sources(ctx context.Context, uid uuid.UUID) ([]SourceState, error) {
	rows, err := s.Pool.Query(ctx, `SELECT c.id, COALESCE(c.adapter,''), COALESCE(NULLIF(c.title,''), c.name),
		EXISTS (SELECT 1 FROM user_connections uc WHERE uc.item_id = c.id AND uc.user_id = $1), c.tools, c.allowed_tools
		FROM catalog_items c WHERE c.comm_source AND c.published AND c.type = 'mcp' ORDER BY c.name`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SourceState{}
	for rows.Next() {
		var id uuid.UUID
		var adapter, itemTitle string
		var connected bool
		var toolsRaw, allowedRaw []byte
		if err := rows.Scan(&id, &adapter, &itemTitle, &connected, &toolsRaw, &allowedRaw); err != nil {
			return nil, err
		}
		srcs, _ := adapterSources(adapter)
		tools, checked := itemTools(toolsRaw, allowedRaw)
		ad, _ := collect.Get(adapter)
		for _, src := range srcs {
			if sc, ok := ad.(collect.SourceChecker); ok && checked && sc.CheckSourceTools(src.Key, tools) != nil {
				continue // FTR.NAB.ADM-0001 R4: the server lacks a tool of this source
			}
			// an adapter of any provider (mail) has no title of its own: the item names the mailbox
			title := src.Title
			if title == "" {
				title = itemTitle
			}
			out = append(out, SourceState{ItemID: id, Source: src.Key, Title: title, Connected: connected})
		}
	}
	return out, rows.Err()
}

// CheckCollect is the gate of "Collecting communications" (R13, R21): the user
// consented and every chosen source is a connected source of the catalog.
// settings are the stored settings of the task: { sources: [{ itemId, source }] }.
func (s *Service) CheckCollect(ctx context.Context, uid uuid.UUID, settings []byte) error {
	if ok, err := s.Consented(ctx, uid); err != nil {
		return err
	} else if !ok {
		return apperr.Conflict("comm_consent_required", "accept the consent before switching the collection on")
	}
	var set struct {
		Sources []struct {
			ItemID uuid.UUID `json:"itemId"`
			Source string    `json:"source"`
		} `json:"sources"`
	}
	_ = json.Unmarshal(settings, &set)
	if len(set.Sources) == 0 {
		return apperr.Conflict("comm_source_not_connected", "choose at least one connected source")
	}
	avail, err := s.Sources(ctx, uid)
	if err != nil {
		return err
	}
	offered := 0
	for _, want := range set.Sources {
		i := slices.IndexFunc(avail, func(a SourceState) bool { return a.ItemID == want.ItemID && a.Source == want.Source })
		if i < 0 {
			// the administrator unmarked the source: the collection skips it, and the
			// user no longer sees it to switch it off
			continue
		}
		if !avail[i].Connected {
			return apperr.Conflict("comm_source_not_connected", "the source "+want.Source+" is not connected: connect it in Connections")
		}
		offered++
	}
	if offered == 0 {
		return apperr.Conflict("comm_source_not_connected", "choose at least one connected source")
	}
	return nil
}
