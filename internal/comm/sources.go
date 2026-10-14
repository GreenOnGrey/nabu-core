package comm

import (
	"context"
	"encoding/json"
	"slices"

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

// Sources lists the sources the user can collect from: the sources of the
// catalog items marked by the administrator, with whether the user connected
// the item.
func (s *Service) Sources(ctx context.Context, uid uuid.UUID) ([]SourceState, error) {
	rows, err := s.Pool.Query(ctx, `SELECT c.id, COALESCE(c.adapter,''), COALESCE(NULLIF(c.title,''), c.name),
		EXISTS (SELECT 1 FROM user_connections uc WHERE uc.item_id = c.id AND uc.user_id = $1)
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
		if err := rows.Scan(&id, &adapter, &itemTitle, &connected); err != nil {
			return nil, err
		}
		srcs, _ := adapterSources(adapter)
		for _, src := range srcs {
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

// CheckCollect is the gate of «Сбор коммуникаций» (R13, R21): the user
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
	for _, want := range set.Sources {
		ok := false
		for _, a := range avail {
			if a.ItemID == want.ItemID && a.Source == want.Source && a.Connected {
				ok = true
			}
		}
		if !ok {
			return apperr.Conflict("comm_source_not_connected", "the source "+want.Source+" is not connected: connect it in Connections")
		}
	}
	return nil
}
