-- FTR.HMR.CMN-0007 tech §3.2, decision 28: proxy.golang.org sends the archives of large modules to
-- storage.googleapis.com. Proxies made from the template before it allow that address too.

-- +goose Up
UPDATE proxies SET allow = allow || '[{"host": "storage.googleapis.com", "port": 443, "internal": false}]'::jsonb,
    updated_at = now(), updated_by = 'system'
WHERE allow @> '[{"host": "proxy.golang.org", "port": 443}]'::jsonb
  AND NOT allow @> '[{"host": "storage.googleapis.com", "port": 443}]'::jsonb;

-- +goose Down
UPDATE proxies SET allow = (SELECT coalesce(jsonb_agg(h), '[]'::jsonb) FROM jsonb_array_elements(allow) h
                            WHERE NOT h @> '{"host": "storage.googleapis.com", "port": 443}'::jsonb)
WHERE allow @> '[{"host": "storage.googleapis.com", "port": 443}]'::jsonb;
