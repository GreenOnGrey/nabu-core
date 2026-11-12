-- FTR.NAB.CHAT-0005 tech §«Схема БД»: the attribution of an audit record
-- (item, session, duration, result code), the publications of an item, its
-- pins and tombstone, the daily and monthly aggregates of usage, the
-- watermark and the goal metric. Expand only: every new column is nullable
-- and the old code works with it (arch A24).

-- +goose Up
ALTER TABLE audit
  ADD COLUMN item_id     UUID,
  ADD COLUMN session_id  TEXT,
  ADD COLUMN duration_ms INTEGER,
  ADD COLUMN result_code TEXT;

ALTER TABLE audit ADD CONSTRAINT audit_result_code_chk
  CHECK (result_code IS NULL OR result_code IN
    ('ok','tool_error','transport_error','denied','file_rejected','file_too_large','auth'));

-- on the partitioned table the index is created on the parent and inherited by the partitions
CREATE INDEX audit_item_at ON audit (item_id, at);

-- the current state of publication and the time of the last check (R13, R15);
-- checked_at (the last SUCCESSFUL check) exists since 0005 and is not duplicated
ALTER TABLE catalog_items
  ADD COLUMN published_at   TIMESTAMPTZ,
  ADD COLUMN unpublished_at TIMESTAMPTZ,
  ADD COLUMN last_check_at  TIMESTAMPTZ;

UPDATE catalog_items SET published_at = created_at WHERE published;

-- publication intervals: append-only, the source of truth about the effective window (R13)
CREATE TABLE catalog_item_publications (
  id             BIGSERIAL PRIMARY KEY,
  item_id        UUID NOT NULL,               -- no FK: intervals outlive the deletion of an item
  published_at   TIMESTAMPTZ NOT NULL,
  unpublished_at TIMESTAMPTZ,
  estimated      BOOLEAN NOT NULL DEFAULT false,
  actor_id       UUID REFERENCES users(id) ON DELETE SET NULL,
  CHECK (unpublished_at IS NULL OR unpublished_at > published_at)
);
CREATE UNIQUE INDEX catalog_item_publications_open
  ON catalog_item_publications (item_id) WHERE unpublished_at IS NULL;
CREATE INDEX catalog_item_publications_item
  ON catalog_item_publications (item_id, published_at DESC);

INSERT INTO catalog_item_publications (item_id, published_at, estimated)
SELECT id, created_at, true FROM catalog_items WHERE published AND NOT in_development;

-- a pin of an item (R12)
CREATE TABLE catalog_item_pins (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  item_id     UUID NOT NULL REFERENCES catalog_items(id) ON DELETE CASCADE,
  reason      TEXT NOT NULL CHECK (length(btrim(reason)) > 0),
  pinned_by   UUID REFERENCES users(id) ON DELETE SET NULL,
  pinned_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  review_at   TIMESTAMPTZ NOT NULL,
  released_at TIMESTAMPTZ
);
CREATE INDEX catalog_item_pins_active  ON catalog_item_pins (item_id)   WHERE released_at IS NULL;
CREATE INDEX catalog_item_pins_overdue ON catalog_item_pins (review_at) WHERE released_at IS NULL;

-- the tombstone of a deleted item (R23): admin_log lives a year, aggregates 400 days
CREATE TABLE catalog_item_tombstones (
  item_id    UUID PRIMARY KEY,
  name       TEXT NOT NULL,
  title      TEXT NOT NULL DEFAULT '',
  type       TEXT NOT NULL,
  deleted_at TIMESTAMPTZ NOT NULL,
  deleted_by UUID REFERENCES users(id) ON DELETE SET NULL
);

-- the daily aggregate (R4): no money and no tokens
CREATE TABLE catalog_usage_daily (
  day             DATE        NOT NULL,
  item_id         UUID        NOT NULL,            -- the zero UUID — "outside the catalog"
  item_name       TEXT        NOT NULL,            -- a snapshot of catalog_items.name
  item_title      TEXT        NOT NULL DEFAULT '', -- a snapshot of the display title
  item_type       TEXT        NOT NULL,            -- mcp | skill | unknown
  group_id        UUID        NOT NULL,            -- the zero UUID — the row of the item level
  group_name      TEXT        NOT NULL DEFAULT '',
  group_builtin   TEXT,                            -- all | admins | NULL
  caller_kind     TEXT        NOT NULL CHECK (caller_kind IN ('personal','service','client')),
  caller          TEXT        NOT NULL DEFAULT '',  -- the service agent name or client_id; '' for people
  channel         TEXT        NOT NULL DEFAULT '',
  mode            TEXT,                            -- a snapshot of the mode of the day
  available       BOOLEAN     NOT NULL DEFAULT true,
  calls           BIGINT      NOT NULL DEFAULT 0,
  ok              BIGINT      NOT NULL DEFAULT 0,
  errors_by_code  JSONB       NOT NULL DEFAULT '{}'::jsonb,  -- every non-ok code, auth and denied included
  users           BIGINT      NOT NULL DEFAULT 0,             -- distinct user_id of the day
  sessions        BIGINT      NOT NULL DEFAULT 0,             -- distinct session_id of the day
  admin_calls     BIGINT      NOT NULL DEFAULT 0,             -- a subset of calls
  service_calls   BIGINT      NOT NULL DEFAULT 0,             -- a subset of calls (service + client)
  latency_buckets JSONB       NOT NULL DEFAULT '{}'::jsonb,   -- {"<upper bound, ms>": count}
  computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (day, item_id, group_id, caller_kind, caller, channel)
);
CREATE INDEX catalog_usage_daily_item ON catalog_usage_daily (item_id, day);
CREATE INDEX catalog_usage_daily_day  ON catalog_usage_daily (day);

-- the monthly slice (R4)
CREATE TABLE catalog_usage_monthly (
  month           DATE        NOT NULL,
  item_id         UUID        NOT NULL,
  item_name       TEXT        NOT NULL,
  item_title      TEXT        NOT NULL DEFAULT '',
  item_type       TEXT        NOT NULL,
  group_id        UUID        NOT NULL,
  group_name      TEXT        NOT NULL DEFAULT '',
  group_builtin   TEXT,
  caller_kind     TEXT        NOT NULL,
  caller          TEXT        NOT NULL DEFAULT '',
  channel         TEXT        NOT NULL DEFAULT '',
  mode            TEXT,
  available_days  INTEGER     NOT NULL DEFAULT 0,
  calls           BIGINT      NOT NULL DEFAULT 0,
  ok              BIGINT      NOT NULL DEFAULT 0,
  errors_by_code  JSONB       NOT NULL DEFAULT '{}'::jsonb,
  users           BIGINT      NOT NULL DEFAULT 0,   -- the sum of daily distinct values: an estimate beyond 400 days
  sessions        BIGINT      NOT NULL DEFAULT 0,
  admin_calls     BIGINT      NOT NULL DEFAULT 0,
  service_calls   BIGINT      NOT NULL DEFAULT 0,
  latency_buckets JSONB       NOT NULL DEFAULT '{}'::jsonb,
  computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (month, item_id, group_id, caller_kind, caller, channel)
);
CREATE INDEX catalog_usage_monthly_item ON catalog_usage_monthly (item_id, month);

-- the watermark and the closing of a day (R5)
CREATE TABLE catalog_usage_state (
  day         DATE PRIMARY KEY,
  computed_at TIMESTAMPTZ NOT NULL,
  closed_at   TIMESTAMPTZ,
  rows        BIGINT NOT NULL DEFAULT 0
);

-- the goal metric: one row per end of the window, written by worker after closing a day (R14)
CREATE TABLE catalog_usage_goal_daily (
  day                DATE PRIMARY KEY,          -- the last day of the window
  window_from        DATE NOT NULL,
  window_to          DATE NOT NULL,
  dead               INTEGER NOT NULL,
  total              INTEGER NOT NULL,
  share              NUMERIC(5,1),
  baseline           NUMERIC(5,1),
  baseline_day       DATE,
  delta              NUMERIC(5,1),
  dropped_total      INTEGER NOT NULL DEFAULT 0,
  dropped_with_calls INTEGER NOT NULL DEFAULT 0,
  computed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (window_to >= window_from)
);

-- the contract of the shared layer with ISS.NAB-0010: only additive item-level rows (arch A5)
CREATE VIEW catalog_usage_item_daily AS
  SELECT * FROM catalog_usage_daily
   WHERE group_id = '00000000-0000-0000-0000-000000000000';

CREATE VIEW catalog_usage_item_monthly AS
  SELECT * FROM catalog_usage_monthly
   WHERE group_id = '00000000-0000-0000-0000-000000000000';

-- the external read-only source (arch A17, A28): SELECT only on the precomputed row.
-- The role nabu_metrics_ro is created by the infrastructure; the migration grants only rights,
-- and does nothing while the role is absent (a local stand or a test).
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nabu_metrics_ro') THEN
    GRANT USAGE ON SCHEMA public TO nabu_metrics_ro;
    GRANT SELECT ON catalog_usage_goal_daily TO nabu_metrics_ro;
  END IF;
END $$;
-- +goose StatementEnd

-- the telemetry of skills (R24): without it "sessions the skill was collected into" is not built
CREATE TABLE skill_usage_events (
  id         BIGSERIAL PRIMARY KEY,
  at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  item_id    UUID NOT NULL,
  item_name  TEXT NOT NULL,
  session_id TEXT,
  user_id    UUID,
  channel    TEXT NOT NULL,
  kind       TEXT NOT NULL CHECK (kind IN ('activated','collected'))
);
CREATE INDEX skill_usage_events_at   ON skill_usage_events (at);
CREATE INDEX skill_usage_events_item ON skill_usage_events (item_id, at);

-- +goose Down
DROP TABLE skill_usage_events;
-- +goose StatementBegin
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'nabu_metrics_ro') THEN
    REVOKE SELECT ON catalog_usage_goal_daily FROM nabu_metrics_ro;
  END IF;
END $$;
-- +goose StatementEnd
DROP VIEW catalog_usage_item_monthly;
DROP VIEW catalog_usage_item_daily;
DROP TABLE catalog_usage_goal_daily;
DROP TABLE catalog_usage_state;
DROP TABLE catalog_usage_monthly;
DROP TABLE catalog_usage_daily;
DROP TABLE catalog_item_tombstones;
DROP TABLE catalog_item_pins;
DROP TABLE catalog_item_publications;
ALTER TABLE catalog_items DROP COLUMN last_check_at, DROP COLUMN unpublished_at, DROP COLUMN published_at;
DROP INDEX audit_item_at;
ALTER TABLE audit DROP CONSTRAINT audit_result_code_chk;
ALTER TABLE audit DROP COLUMN result_code, DROP COLUMN duration_ms, DROP COLUMN session_id, DROP COLUMN item_id;
