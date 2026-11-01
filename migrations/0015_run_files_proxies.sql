-- FTR.HMR.CMN-0007 tech §2, §3, §9.2: the working copy of a run of a service
-- agent and its changes; proxies of sandboxes, given to service agents in
-- their configuration and to personal and group agents next to the profile.

-- +goose Up
CREATE TABLE proxies (
    name        TEXT PRIMARY KEY CHECK (name ~ '^[a-z][a-z0-9-]{1,39}$'),
    kind        TEXT NOT NULL CHECK (kind IN ('packages', 'hosts')),
    title       TEXT NOT NULL CHECK (char_length(title) BETWEEN 1 AND 60),
    description TEXT NOT NULL DEFAULT '' CHECK (char_length(description) <= 500),
    spec        JSONB NOT NULL DEFAULT '{}',
    allow       JSONB NOT NULL DEFAULT '[]',
    env         JSONB NOT NULL DEFAULT '[]',
    state       TEXT NOT NULL DEFAULT 'checking' CHECK (state IN ('checking', 'ok', 'unreachable')),
    check_error TEXT,
    checked_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by  TEXT
);
-- allow and env of the built-in proxy are filled by the api from the template
-- at start-up while allow is empty (tech §3.2).
INSERT INTO proxies (name, kind, title, description, spec) VALUES
    ('packages', 'packages', 'Реестры пакетов',
     'Go-модули, npm и PyPI. Ставь зависимости проекта: go mod download, npm ci, pip install в venv.',
     '{"ecosystems": ["go", "npm", "pypi"], "registries": []}');

ALTER TABLE sandbox_assignments ADD COLUMN proxies TEXT[] NOT NULL DEFAULT '{}';

ALTER TABLE runs
    ADD COLUMN proxies       TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN files_key     TEXT,
    ADD COLUMN files_count   INTEGER,
    ADD COLUMN files_bytes   BIGINT,
    ADD COLUMN changes_key   TEXT,
    ADD COLUMN changes_count INTEGER,
    ADD COLUMN files_expired BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX runs_files_sweep ON runs (finished_at) WHERE files_key IS NOT NULL AND NOT files_expired;

CREATE TABLE run_uploads (
    id         UUID PRIMARY KEY,
    client_id  UUID NOT NULL REFERENCES service_clients (id) ON DELETE CASCADE,
    key        TEXT NOT NULL,
    files      INTEGER NOT NULL,
    bytes      BIGINT NOT NULL,
    run_id     UUID REFERENCES runs (id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX run_uploads_stale ON run_uploads (created_at) WHERE run_id IS NULL;

-- +goose Down
DROP TABLE run_uploads;
DROP INDEX runs_files_sweep;
ALTER TABLE runs DROP COLUMN files_expired, DROP COLUMN changes_count, DROP COLUMN changes_key,
    DROP COLUMN files_bytes, DROP COLUMN files_count, DROP COLUMN files_key, DROP COLUMN proxies;
ALTER TABLE sandbox_assignments DROP COLUMN proxies;
DROP TABLE proxies;
