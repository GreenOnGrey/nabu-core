-- FTR.NAB.CMN-0002 tech §11: channels, Telegram keys, mail, confirmations,
-- group agents, archiving and restoring accounts.
-- Deviations (tech §13): the data of a group agent belongs to a technical user
-- (users.created_via = 'group'), so conversations, memories and spaces keep one
-- owner column; VK Teams chats of users are kept in channel_contacts.
-- +goose Up

CREATE TABLE channels (
  kind TEXT PRIMARY KEY CHECK (kind IN ('web','hammurapi','email','vkteams','telegram')),
  enabled BOOLEAN NOT NULL DEFAULT false, all_users BOOLEAN NOT NULL DEFAULT false,
  groups_enabled BOOLEAN NOT NULL DEFAULT false,
  settings JSONB NOT NULL DEFAULT '{}', secrets_enc BYTEA,
  status TEXT NOT NULL DEFAULT 'unknown', status_reason TEXT, status_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO channels (kind, enabled, all_users) VALUES ('web', true, true) ON CONFLICT DO NOTHING;
-- delegation of products (FTR.NAB.CMN-0001) stays open to everybody after the
-- update; messengers and mail are switched on by an administrator
INSERT INTO channels (kind, enabled, all_users) VALUES ('hammurapi', true, true) ON CONFLICT DO NOTHING;
INSERT INTO channels (kind) VALUES ('email'), ('vkteams'), ('telegram') ON CONFLICT DO NOTHING;

CREATE TABLE channel_user_access (
  channel TEXT NOT NULL REFERENCES channels(kind) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  PRIMARY KEY (channel, user_id)
);
CREATE INDEX ON channel_user_access (user_id);

-- the private chat of a user with the bot of a channel without binding (VK Teams)
CREATE TABLE channel_contacts (
  channel TEXT NOT NULL REFERENCES channels(kind) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  chat_id TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (channel, user_id)
);

CREATE TABLE telegram_keys (
  user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  key_hash BYTEA NOT NULL UNIQUE, issued_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- telegram_bindings replaces channel_links of telegram (FTR.NAB.CMN-0001)
CREATE TABLE telegram_bindings (
  user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  tg_user_id BIGINT NOT NULL UNIQUE, username TEXT,
  bound_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO telegram_bindings (user_id, tg_user_id, username, bound_at)
  SELECT user_id, external_id::bigint, account, linked_at FROM channel_links
  WHERE channel = 'telegram' AND external_id ~ '^-?[0-9]+$'
  ON CONFLICT DO NOTHING;
CREATE TABLE telegram_bind_attempts (
  tg_user_id BIGINT NOT NULL, at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON telegram_bind_attempts (tg_user_id, at);

CREATE TABLE email_threads (
  conversation_id UUID PRIMARY KEY REFERENCES conversations(id) ON DELETE CASCADE,
  root_message_id TEXT NOT NULL, message_ids TEXT[] NOT NULL, subject TEXT NOT NULL,
  -- the latest letter: whom and how to answer (tech §5.2–5.3)
  reply_to TEXT, reply_mode TEXT CHECK (reply_mode IN ('direct','web_only')), last_message_id TEXT
);
CREATE INDEX email_threads_ids ON email_threads USING GIN (message_ids);
CREATE TABLE email_log (
  id BIGSERIAL PRIMARY KEY, at TIMESTAMPTZ NOT NULL DEFAULT now(),
  message_id TEXT, from_addr TEXT, subject TEXT,
  result TEXT NOT NULL CHECK (result IN ('accepted','rejected','ignored','unavailable')),
  reason TEXT, conversation_id UUID
);
CREATE INDEX ON email_log (message_id);
CREATE INDEX ON email_log (at DESC);
CREATE TABLE email_rate (
  address TEXT NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('reply','unavailable')),
  window_start TIMESTAMPTZ NOT NULL, count INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (address, kind, window_start)
);

CREATE TABLE pending_confirmations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  item_id UUID REFERENCES catalog_items(id) ON DELETE CASCADE,
  server TEXT NOT NULL, tool TEXT NOT NULL, args_enc BYTEA NOT NULL, summary TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected','expired')),
  result TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), expires_at TIMESTAMPTZ NOT NULL, resolved_at TIMESTAMPTZ
);
CREATE INDEX ON pending_confirmations (conversation_id, status);

CREATE TABLE group_agents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  channel TEXT NOT NULL REFERENCES channels(kind), external_chat_id TEXT NOT NULL,
  chat_title TEXT, members_count INTEGER, owner_id UUID REFERENCES users(id) ON DELETE SET NULL,
  -- the technical user that owns the conversation, memory and space of the group
  data_user_id UUID REFERENCES users(id) ON DELETE SET NULL,
  name TEXT NOT NULL DEFAULT 'Nabu', tone TEXT NOT NULL DEFAULT 'business',
  model_connection_id UUID, model TEXT, skills TEXT[] NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled','removed')),
  data_until TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (channel, external_chat_id)
);

CREATE TABLE restore_requests (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  issuer TEXT, subject TEXT, requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','approved','rejected'))
);
CREATE UNIQUE INDEX restore_requests_open ON restore_requests (user_id) WHERE status = 'open';

CREATE TABLE account_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('archive','restore')), step TEXT NOT NULL,
  params JSONB NOT NULL DEFAULT '{}', status TEXT NOT NULL DEFAULT 'running',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ
);
CREATE INDEX account_jobs_running ON account_jobs (created_at) WHERE status = 'running';
-- emails of purged accounts: the archiving API answers "purged" (R22)
CREATE TABLE purged_accounts (
  email TEXT PRIMARY KEY, purged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE users DROP CONSTRAINT IF EXISTS users_status_check;
ALTER TABLE users DROP CONSTRAINT IF EXISTS users_created_via_check;
ALTER TABLE users
  ADD CONSTRAINT users_status_check CHECK (status IN ('invited','active','blocked','archived')),
  ADD CONSTRAINT users_created_via_check CHECK (created_via IN ('login','invite','delegation','group')),
  ADD COLUMN archived_at TIMESTAMPTZ, ADD COLUMN archived_by TEXT,
  ADD COLUMN purge_after TIMESTAMPTZ,
  ADD COLUMN link_identity_on_next_login BOOLEAN NOT NULL DEFAULT false;
CREATE INDEX users_purge ON users (purge_after) WHERE status = 'archived';

ALTER TABLE conversations
  ADD COLUMN source TEXT NOT NULL DEFAULT 'chat' CHECK (source IN ('chat','email','group')),
  ADD COLUMN writes_require_confirmation BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN unread_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE scheduled_tasks ADD COLUMN pause_kind TEXT;
ALTER TABLE task_runs ADD COLUMN delivery_note TEXT;

ALTER TABLE usage ADD COLUMN group_agent_id UUID;
ALTER TABLE audit ADD COLUMN group_agent_id UUID;
ALTER TABLE service_clients
  ADD COLUMN can_archive BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN can_restore BOOLEAN NOT NULL DEFAULT false;
INSERT INTO settings (key, value) VALUES ('archive', '{"retentionDays":180}') ON CONFLICT DO NOTHING;

-- +goose Down
DELETE FROM settings WHERE key = 'archive';
ALTER TABLE service_clients DROP COLUMN can_restore, DROP COLUMN can_archive;
ALTER TABLE audit DROP COLUMN group_agent_id;
ALTER TABLE usage DROP COLUMN group_agent_id;
ALTER TABLE task_runs DROP COLUMN delivery_note;
ALTER TABLE scheduled_tasks DROP COLUMN pause_kind;
ALTER TABLE conversations DROP COLUMN unread_count, DROP COLUMN writes_require_confirmation, DROP COLUMN source;
DROP INDEX users_purge;
ALTER TABLE users DROP COLUMN link_identity_on_next_login, DROP COLUMN purge_after, DROP COLUMN archived_by, DROP COLUMN archived_at;
ALTER TABLE users DROP CONSTRAINT users_created_via_check, DROP CONSTRAINT users_status_check;
ALTER TABLE users ADD CONSTRAINT users_status_check CHECK (status IN ('invited','active','blocked')),
  ADD CONSTRAINT users_created_via_check CHECK (created_via IN ('login','invite','delegation'));
DROP TABLE purged_accounts; DROP TABLE account_jobs; DROP TABLE restore_requests; DROP TABLE group_agents; DROP TABLE pending_confirmations;
DROP TABLE email_rate; DROP TABLE email_log; DROP TABLE email_threads;
DROP TABLE telegram_bind_attempts; DROP TABLE telegram_bindings; DROP TABLE telegram_keys;
DROP TABLE channel_contacts; DROP TABLE channel_user_access; DROP TABLE channels;
