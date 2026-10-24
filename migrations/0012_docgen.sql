-- FTR.NAB.CHAT-0003: result arguments of catalog MCP tools (a tool writes a
-- file of the space), tools that write only to the space (no confirmation in
-- mail topics unless they overwrite), the content hash of attachments and the
-- cache of previews of office files (tech §4.1).

-- +goose Up
ALTER TABLE catalog_items
    ADD COLUMN result_args      JSONB  NOT NULL DEFAULT '[]',
    ADD COLUMN result_max_bytes BIGINT NOT NULL DEFAULT 52428800,
    ADD COLUMN space_only_tools JSONB  NOT NULL DEFAULT '[]';
ALTER TABLE attachments ADD COLUMN sha256 TEXT;

CREATE TABLE previews (
    sha256      TEXT PRIMARY KEY,
    kind        TEXT NOT NULL CHECK (kind IN ('doc', 'sheet')),
    state       TEXT NOT NULL CHECK (state IN ('pending', 'ready', 'error')),
    reason      TEXT,
    pages_shown INT,
    pages_total INT,
    bytes       BIGINT,
    started_at  TIMESTAMPTZ,
    ready_at    TIMESTAMPTZ,
    accessed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ
);
CREATE INDEX previews_accessed ON previews (accessed_at);

-- +goose Down
DROP TABLE previews;
ALTER TABLE attachments DROP COLUMN sha256;
ALTER TABLE catalog_items DROP COLUMN result_args, DROP COLUMN result_max_bytes, DROP COLUMN space_only_tools;
