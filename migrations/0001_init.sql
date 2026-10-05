-- FTR.NAB.CMN-0001 tech §10: the schema of the nabu database.
-- +goose Up

CREATE TABLE users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT NOT NULL UNIQUE,              -- lower case
  name TEXT, avatar_url TEXT,
  is_admin BOOLEAN NOT NULL DEFAULT false,
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('invited','active','blocked')),
  language TEXT NOT NULL DEFAULT 'en', theme TEXT NOT NULL DEFAULT 'light',
  timezone TEXT NOT NULL DEFAULT 'Europe/Moscow',
  created_via TEXT NOT NULL DEFAULT 'login' CHECK (created_via IN ('login','invite','delegation')),
  last_seen_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE user_identities (
  issuer TEXT NOT NULL, subject TEXT NOT NULL,
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (issuer, subject)
);
CREATE TABLE user_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  csrf_token TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX ON user_sessions (user_id);
CREATE TABLE channel_links (
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  channel TEXT NOT NULL CHECK (channel IN ('telegram','vkws')),
  external_id TEXT NOT NULL, chat_id TEXT, account TEXT, linked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, channel), UNIQUE (channel, external_id)
);
CREATE TABLE link_codes (
  code_hash BYTEA PRIMARY KEY, user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  channel TEXT NOT NULL, expires_at TIMESTAMPTZ NOT NULL, used_at TIMESTAMPTZ
);
CREATE TABLE agent_profiles (
  user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  name TEXT NOT NULL DEFAULT 'Nabu',
  tone TEXT NOT NULL DEFAULT 'business' CHECK (tone IN ('business','friendly','brief','mentor')),
  model_connection_id UUID, model TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE conversations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('main','topic','task')), title TEXT,
  last_message_at TIMESTAMPTZ, archived_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX conversations_one_main ON conversations (user_id) WHERE kind = 'main';
CREATE INDEX ON conversations (user_id, last_message_at DESC);
CREATE TABLE messages (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  role TEXT NOT NULL CHECK (role IN ('user','assistant')),
  text TEXT NOT NULL, channel TEXT NOT NULL,   -- web | telegram | vkws | client:<name> | task:<id>
  status TEXT NOT NULL DEFAULT 'done' CHECK (status IN ('pending','streaming','done','failed')),
  attachments JSONB NOT NULL DEFAULT '[]', tool_steps JSONB NOT NULL DEFAULT '[]',
  context JSONB, model TEXT, error_class TEXT, error_text TEXT,
  reply_to UUID REFERENCES messages(id) ON DELETE SET NULL,
  retry_of UUID REFERENCES messages(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON messages (conversation_id, created_at);
CREATE TABLE attachments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  file_name TEXT NOT NULL, mime_type TEXT NOT NULL, size_bytes BIGINT NOT NULL,
  s3_key TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE harness_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id UUID REFERENCES conversations(id) ON DELETE CASCADE,
  run_id UUID, harness TEXT NOT NULL, pod TEXT, operator_session TEXT,
  snapshot_key TEXT, persona_hash TEXT,
  last_active_at TIMESTAMPTZ NOT NULL DEFAULT now(), closed_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX harness_sessions_conversation ON harness_sessions (conversation_id) WHERE closed_at IS NULL;
CREATE TABLE memories (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  text TEXT NOT NULL, source TEXT NOT NULL CHECK (source IN ('agent','user')),
  pinned BOOLEAN NOT NULL DEFAULT false, conversation_id UUID,
  search_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON memories USING GIN (search_vector);
CREATE INDEX ON memories (user_id, updated_at DESC);
CREATE TABLE spaces (
  user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  state TEXT NOT NULL DEFAULT 'sleeping' CHECK (state IN ('starting','running','stopping','sleeping')),
  pod TEXT, used_bytes BIGINT NOT NULL DEFAULT 0,
  quota_bytes BIGINT NOT NULL, last_activity_at TIMESTAMPTZ, state_changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE space_files (
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  path TEXT NOT NULL, size BIGINT NOT NULL, sha256 TEXT NOT NULL,
  modified_by TEXT NOT NULL CHECK (modified_by IN ('agent','user')),
  modified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, path)
);
CREATE TABLE model_connections (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL UNIQUE,
  type TEXT NOT NULL CHECK (type IN ('deepseek','openai_compatible')),
  base_url TEXT NOT NULL, api TEXT NOT NULL DEFAULT 'openai-completions',
  api_key_enc BYTEA, key_last4 TEXT,
  models JSONB NOT NULL DEFAULT '[]',
  enabled BOOLEAN NOT NULL DEFAULT true,
  status TEXT NOT NULL DEFAULT 'unchecked', status_reason TEXT, checked_at TIMESTAMPTZ,
  import_ref TEXT UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE settings (
  key TEXT PRIMARY KEY, value JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE catalog_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  type TEXT NOT NULL CHECK (type IN ('mcp','skill')), name TEXT NOT NULL UNIQUE, title TEXT, description TEXT,
  source JSONB NOT NULL, mode TEXT CHECK (mode IN ('personal','platform')),
  personal_auth JSONB, personal_auth_secret_enc BYTEA, platform_auth_enc BYTEA,
  read_only BOOLEAN NOT NULL DEFAULT true, exposure TEXT NOT NULL DEFAULT 'deferred',
  published BOOLEAN NOT NULL DEFAULT false, in_development BOOLEAN NOT NULL DEFAULT false,
  status TEXT, status_reason TEXT, tools JSONB,
  skills_snapshot TEXT, skills JSONB, synced_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE user_connections (
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  item_id UUID NOT NULL REFERENCES catalog_items(id) ON DELETE CASCADE,
  credentials_enc BYTEA, refresh_enc BYTEA, expires_at TIMESTAMPTZ,
  connected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, item_id)
);
CREATE TABLE oauth_states (
  state TEXT PRIMARY KEY, user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  item_id UUID NOT NULL REFERENCES catalog_items(id) ON DELETE CASCADE,
  verifier TEXT NOT NULL, expires_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE service_agents (
  name TEXT PRIMARY KEY, config JSONB NOT NULL, enabled BOOLEAN NOT NULL DEFAULT true,
  updated_by UUID REFERENCES users(id) ON DELETE SET NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE service_clients (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL UNIQUE, url TEXT,
  client_id TEXT NOT NULL UNIQUE,
  secret_hash BYTEA NOT NULL, prev_secret_hash BYTEA, prev_secret_expires_at TIMESTAMPTZ,
  agents TEXT[] NOT NULL DEFAULT '{}', can_delegate BOOLEAN NOT NULL DEFAULT false,
  can_import BOOLEAN NOT NULL DEFAULT false,
  limits JSONB NOT NULL DEFAULT '{}', enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  agent TEXT NOT NULL REFERENCES service_agents(name) ON UPDATE CASCADE,
  client_id UUID NOT NULL REFERENCES service_clients(id),
  initiator_email TEXT, idempotency_key TEXT,
  status TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
  input JSONB NOT NULL, summary TEXT, error_class TEXT, error_text TEXT,
  usage JSONB, last_seq INTEGER NOT NULL DEFAULT 0,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ,
  UNIQUE (client_id, idempotency_key)
);
CREATE INDEX ON runs (agent, started_at DESC);
CREATE TABLE run_events (
  run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  seq INTEGER NOT NULL, type TEXT NOT NULL, data JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, seq)
);
CREATE TABLE workspace_connections (
  workspace_id TEXT PRIMARY KEY, kind TEXT NOT NULL, relay_pod TEXT NOT NULL,
  connected_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_ping_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE scheduled_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title TEXT NOT NULL, instruction TEXT NOT NULL,
  schedule_kind TEXT NOT NULL CHECK (schedule_kind IN ('once','cron')),
  run_at TIMESTAMPTZ, cron TEXT, timezone TEXT NOT NULL,
  channel TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active','paused','done','cancelled')),
  next_run_at TIMESTAMPTZ, failures INTEGER NOT NULL DEFAULT 0, pause_reason TEXT,
  created_from_message UUID, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  cancelled_at TIMESTAMPTZ
);
CREATE INDEX scheduled_tasks_due ON scheduled_tasks (next_run_at) WHERE status = 'active';
CREATE INDEX ON scheduled_tasks (user_id, created_at DESC);
CREATE TABLE task_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id UUID NOT NULL REFERENCES scheduled_tasks(id) ON DELETE CASCADE,
  scheduled_for TIMESTAMPTZ,
  status TEXT NOT NULL CHECK (status IN ('running','succeeded','failed')),
  summary TEXT, error_class TEXT, error_text TEXT, message_id UUID,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ,
  UNIQUE (task_id, scheduled_for)
);
CREATE TABLE usage (
  id BIGSERIAL PRIMARY KEY, user_id UUID, run_id UUID, client_id UUID, agent TEXT,
  connection_id UUID, model TEXT, tokens_in BIGINT NOT NULL DEFAULT 0, tokens_out BIGINT NOT NULL DEFAULT 0,
  cache_read BIGINT NOT NULL DEFAULT 0, cache_write BIGINT NOT NULL DEFAULT 0, cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON usage (created_at);
CREATE TABLE audit (
  id BIGSERIAL, at TIMESTAMPTZ NOT NULL DEFAULT now(),
  agent_kind TEXT NOT NULL CHECK (agent_kind IN ('personal','service')), agent TEXT NOT NULL,
  user_id UUID, client_id UUID, initiator_email TEXT, channel TEXT NOT NULL,
  server TEXT, tool TEXT NOT NULL, args_summary TEXT, result TEXT NOT NULL, error TEXT,
  PRIMARY KEY (id, at)
) PARTITION BY RANGE (at);                            -- monthly partitions are created ahead by the worker
CREATE TABLE audit_default PARTITION OF audit DEFAULT;
CREATE INDEX ON audit (at, user_id);
CREATE INDEX ON audit (at, agent);

-- +goose Down
DROP TABLE audit; DROP TABLE usage; DROP TABLE task_runs; DROP TABLE scheduled_tasks;
DROP TABLE workspace_connections; DROP TABLE run_events; DROP TABLE runs; DROP TABLE service_clients;
DROP TABLE service_agents; DROP TABLE oauth_states; DROP TABLE user_connections; DROP TABLE catalog_items;
DROP TABLE settings; DROP TABLE model_connections; DROP TABLE space_files; DROP TABLE spaces; DROP TABLE memories;
DROP TABLE harness_sessions; DROP TABLE attachments; DROP TABLE messages; DROP TABLE conversations;
DROP TABLE agent_profiles; DROP TABLE link_codes; DROP TABLE channel_links; DROP TABLE user_sessions;
DROP TABLE user_identities; DROP TABLE users;
