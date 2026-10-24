-- FTR.NAB.CMN-0008: the profiles of sandbox images (built-in ones are synced
-- from SANDBOX_PROFILES by the api, custom ones come from the administration),
-- the assignment of profiles to personal and group agents, and the requests of
-- libraries: signals of agents and of sandboxes, mappings of modules to
-- packages, the status of a package and its history (tech §4).

-- +goose Up
CREATE TABLE sandbox_profiles (
    name         TEXT PRIMARY KEY CHECK (name ~ '^[a-z0-9][a-z0-9-]{0,39}$'),
    kind         TEXT NOT NULL CHECK (kind IN ('builtin', 'custom')),
    image_ref    TEXT NOT NULL,
    image_digest TEXT,
    description  TEXT NOT NULL DEFAULT '' CHECK (length(description) <= 1000),
    state        TEXT NOT NULL DEFAULT 'checking' CHECK (state IN ('checking', 'ready', 'error')),
    error        TEXT,
    pending_ref  TEXT,
    -- the report of the probe pod of a custom image (sandbox-probe), read by the worker
    probe_report     JSONB,
    probe_started_at TIMESTAMPTZ,
    inventory    JSONB,
    base_version TEXT,
    nodes_ready  INT NOT NULL DEFAULT 0,
    nodes_total  INT NOT NULL DEFAULT 0,
    checked_at   TIMESTAMPTZ,
    updated_by   TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The profile the sandbox of a space was started with (start metrics, signals).
ALTER TABLE spaces ADD COLUMN profile TEXT;

CREATE TABLE sandbox_assignments (
    target     TEXT PRIMARY KEY CHECK (target IN ('personal', 'group')),
    profile    TEXT NOT NULL REFERENCES sandbox_profiles (name),
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE package_signals (
    id         BIGSERIAL PRIMARY KEY,
    ecosystem  TEXT NOT NULL CHECK (ecosystem IN ('pypi', 'apt', 'binary', 'python-module', 'command')),
    name       TEXT NOT NULL,
    source     TEXT NOT NULL CHECK (source IN ('agent', 'auto')),
    reason     TEXT CHECK (length(reason) <= 300),
    profile    TEXT NOT NULL,
    agent_kind TEXT NOT NULL CHECK (agent_kind IN ('personal', 'group', 'service')),
    subject    TEXT NOT NULL,
    day        DATE NOT NULL DEFAULT current_date,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subject, ecosystem, name, source, day)
);
CREATE INDEX package_signals_day ON package_signals (day);

CREATE TABLE package_aliases (
    ecosystem         TEXT NOT NULL CHECK (ecosystem IN ('python-module', 'command')),
    name              TEXT NOT NULL,
    package_ecosystem TEXT NOT NULL CHECK (package_ecosystem IN ('pypi', 'apt', 'binary')),
    package_name      TEXT NOT NULL,
    builtin           BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (ecosystem, name)
);
INSERT INTO package_aliases (ecosystem, name, package_ecosystem, package_name, builtin) VALUES
    ('python-module', 'PIL', 'pypi', 'pillow', true),
    ('python-module', 'cv2', 'pypi', 'opencv-python', true),
    ('python-module', 'docx', 'pypi', 'python-docx', true),
    ('python-module', 'pptx', 'pypi', 'python-pptx', true),
    ('python-module', 'yaml', 'pypi', 'pyyaml', true),
    ('python-module', 'sklearn', 'pypi', 'scikit-learn', true),
    ('python-module', 'bs4', 'pypi', 'beautifulsoup4', true),
    ('python-module', 'fitz', 'pypi', 'pymupdf', true);

-- A module or a command without a mapping keeps its status under its own
-- ecosystem (python-module, command) until the administrator maps it.
CREATE TABLE package_status (
    package_ecosystem TEXT NOT NULL,
    package_name      TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('new', 'planned', 'added', 'rejected')),
    profile           TEXT,
    version           TEXT,
    note              TEXT CHECK (length(note) <= 300),
    updated_by        TEXT,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (package_ecosystem, package_name)
);

CREATE TABLE package_status_log (
    id                BIGSERIAL PRIMARY KEY,
    package_ecosystem TEXT NOT NULL,
    package_name      TEXT NOT NULL,
    status            TEXT NOT NULL,
    profile           TEXT,
    version           TEXT,
    note              TEXT,
    updated_by        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX package_status_log_package ON package_status_log (package_ecosystem, package_name, created_at);

-- +goose Down
DROP TABLE package_status_log;
DROP TABLE package_status;
DROP TABLE package_aliases;
DROP TABLE package_signals;
DROP TABLE sandbox_assignments;
DROP TABLE sandbox_profiles;
ALTER TABLE spaces DROP COLUMN profile;
