-- FTR.NAB.CMN-0003 tech §11: the memory of communications — notes of mail
-- threads and chats with people, topics and commitments, the prepared memory
-- tasks, runs of the collector agent owned by a user.
-- Deviations (tech §14): the vectors live in comm_note_vectors, created by
-- the application when the vector extension is
-- available (comm.EnsureVectors), so the migration does not need it.

-- +goose Up
ALTER TABLE scheduled_tasks
  ADD COLUMN kind TEXT NOT NULL DEFAULT 'agent'
    CHECK (kind IN ('agent','memory_collect','memory_digest_day','memory_digest_week')),
  ADD COLUMN settings JSONB NOT NULL DEFAULT '{}';
-- one prepared task of a kind per user: switching it off and on keeps the row and its runs
CREATE UNIQUE INDEX scheduled_tasks_memory ON scheduled_tasks (user_id, kind) WHERE kind <> 'agent';
-- pause_kind gets 'source_access', 'budget' and 'user' next to 'archived'

ALTER TABLE runs
  ADD COLUMN user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  ALTER COLUMN client_id DROP NOT NULL,
  ADD CONSTRAINT runs_owner CHECK (client_id IS NOT NULL OR user_id IS NOT NULL);
INSERT INTO service_agents (name, config) VALUES ('memory-collector', '{"builtin": true}')
  ON CONFLICT (name) DO NOTHING;

-- the card of a digest message: the commitments it lists (tech §3.5)
ALTER TABLE messages ADD COLUMN card JSONB;

ALTER TABLE catalog_items
  ADD COLUMN adapter TEXT,
  ADD COLUMN comm_source BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE comm_settings (
  user_id         UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  consent_at      TIMESTAMPTZ,
  consent_version TEXT,
  notes_count     INTEGER NOT NULL DEFAULT 0,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE comm_cursors (
  user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  item_id         UUID NOT NULL REFERENCES catalog_items(id) ON DELETE CASCADE,
  source          TEXT NOT NULL,
  cursor          TEXT NOT NULL,
  pending_cursor  TEXT,  -- becomes the cursor when every batch of the tick is done
  last_success_at TIMESTAMPTZ,
  PRIMARY KEY (user_id, item_id, source)
);
CREATE TABLE comm_batches (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  task_run_id UUID REFERENCES task_runs(id) ON DELETE CASCADE,
  item_id     UUID NOT NULL,
  source      TEXT NOT NULL,
  backfill    BOOLEAN NOT NULL DEFAULT false,
  threads     JSONB NOT NULL,  -- [{ key, dayKey, participants, messages: [{ id, author, sentAt }] }] — never the text
  notes       INTEGER NOT NULL DEFAULT 0,
  messages    INTEGER NOT NULL DEFAULT 0,
  error_class TEXT,
  status      TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','done','failed')),
  run_id      UUID,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at  TIMESTAMPTZ,  -- when a run took it: a run that does not end is told by this time
  finished_at TIMESTAMPTZ
);
CREATE INDEX ON comm_batches (user_id, status);

CREATE TABLE comm_notes (
  id             UUID NOT NULL DEFAULT gen_random_uuid(),
  user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  item_id        UUID NOT NULL,
  source         TEXT NOT NULL,
  thread_key     TEXT NOT NULL,
  day_key        TEXT NOT NULL DEFAULT '',  -- empty for mail: a thread is one note; a chat is a note per day
  title          TEXT NOT NULL,
  summary        TEXT NOT NULL,
  appended       JSONB NOT NULL DEFAULT '[]',
  user_edited    BOOLEAN NOT NULL DEFAULT false,
  first_at       TIMESTAMPTZ NOT NULL,
  last_at        TIMESTAMPTZ NOT NULL,
  time_precision TEXT NOT NULL DEFAULT 'minute' CHECK (time_precision IN ('minute','day')),
  message_count  INTEGER NOT NULL DEFAULT 0,
  search_vector  TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple', title || ' ' || summary)) STORED,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, id),
  UNIQUE (user_id, item_id, source, thread_key, day_key)
) PARTITION BY HASH (user_id);
CREATE INDEX comm_notes_recent ON comm_notes (user_id, last_at DESC, id DESC);
CREATE INDEX comm_notes_search ON comm_notes USING GIN (search_vector);

CREATE TABLE comm_note_messages (
  user_id    UUID NOT NULL,
  note_id    UUID NOT NULL,
  message_id TEXT NOT NULL,
  author     TEXT,
  sent_at    TIMESTAMPTZ,
  title      TEXT,
  url        TEXT,
  PRIMARY KEY (user_id, note_id, message_id),
  FOREIGN KEY (user_id, note_id) REFERENCES comm_notes (user_id, id) ON DELETE CASCADE
) PARTITION BY HASH (user_id);

CREATE TABLE comm_people (
  id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  email   TEXT NOT NULL,  -- lower case
  name    TEXT,
  UNIQUE (user_id, email)
);
CREATE TABLE comm_topics (
  id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name     TEXT NOT NULL,
  name_key TEXT NOT NULL,  -- lower case, single spaces
  UNIQUE (user_id, name_key)
);
CREATE TABLE comm_note_tags (
  user_id UUID NOT NULL,
  note_id UUID NOT NULL,
  kind    TEXT NOT NULL CHECK (kind IN ('person','topic')),
  tag_id  UUID NOT NULL,
  PRIMARY KEY (user_id, note_id, kind, tag_id),
  FOREIGN KEY (user_id, note_id) REFERENCES comm_notes (user_id, id) ON DELETE CASCADE
) PARTITION BY HASH (user_id);
CREATE INDEX comm_note_tags_tag ON comm_note_tags (user_id, kind, tag_id);

-- +goose StatementBegin
DO $$
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format('CREATE TABLE comm_notes_p%s PARTITION OF comm_notes FOR VALUES WITH (MODULUS 32, REMAINDER %s)', lpad(i::text, 2, '0'), i);
    EXECUTE format('CREATE TABLE comm_note_messages_p%s PARTITION OF comm_note_messages FOR VALUES WITH (MODULUS 32, REMAINDER %s)', lpad(i::text, 2, '0'), i);
    EXECUTE format('CREATE TABLE comm_note_tags_p%s PARTITION OF comm_note_tags FOR VALUES WITH (MODULUS 32, REMAINDER %s)', lpad(i::text, 2, '0'), i);
  END LOOP;
END $$;
-- +goose StatementEnd

CREATE TABLE commitments (
  id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id                UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  note_id                UUID NOT NULL,
  text                   TEXT NOT NULL,
  direction              TEXT NOT NULL CHECK (direction IN ('mine','theirs')),
  person_id              UUID REFERENCES comm_people(id) ON DELETE SET NULL,
  due                    DATE,
  state                  TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open','done','cancelled')),
  closed_by              TEXT CHECK (closed_by IN ('user','agent','collector')),
  closed_at              TIMESTAMPTZ,
  basis_message_id       TEXT,
  close_basis_message_id TEXT,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  FOREIGN KEY (user_id, note_id) REFERENCES comm_notes (user_id, id) ON DELETE CASCADE
);
CREATE INDEX commitments_due ON commitments (user_id, state, due);
CREATE INDEX commitments_note ON commitments (user_id, note_id);

CREATE TABLE comm_tombstones (
  user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  item_id    UUID NOT NULL,
  source     TEXT NOT NULL,
  thread_key TEXT NOT NULL,
  day_key    TEXT NOT NULL DEFAULT '',
  deleted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, item_id, source, thread_key, day_key)
);
-- messages the collector read and found nothing to note in: they are not queued again
CREATE TABLE comm_skipped (
  user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  item_id    UUID NOT NULL,
  source     TEXT NOT NULL,
  thread_key TEXT NOT NULL,
  day_key    TEXT NOT NULL DEFAULT '',
  message_id TEXT NOT NULL,
  seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, item_id, source, thread_key, day_key, message_id)
);
CREATE TABLE comm_usage (
  user_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  month    DATE NOT NULL,
  runs     INTEGER NOT NULL DEFAULT 0,
  cost_usd NUMERIC(12,4) NOT NULL DEFAULT 0,
  PRIMARY KEY (user_id, month)
);

-- R22: a connection that has not named the user sees nothing. The owner of the
-- tables is the application itself, hence FORCE; maintenance (the cleaner,
-- the recount of vectors) names itself with nabu.maintenance.
-- +goose StatementBegin
DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['comm_notes','comm_note_messages','comm_note_tags','comm_people','comm_topics',
                           'commitments','comm_tombstones','comm_cursors','comm_batches','comm_skipped'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format($p$CREATE POLICY comm_owner ON %I USING (
      user_id = nullif(current_setting('nabu.user_id', true), '')::uuid
      OR current_setting('nabu.maintenance', true) = 'on')$p$, t);
  END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down
UPDATE scheduled_tasks SET status = 'cancelled', cancelled_at = now(), next_run_at = NULL WHERE kind <> 'agent';
DROP TABLE IF EXISTS comm_note_vectors;
DROP TABLE comm_usage; DROP TABLE comm_skipped; DROP TABLE comm_tombstones; DROP TABLE commitments;
DROP TABLE comm_note_tags; DROP TABLE comm_topics; DROP TABLE comm_people; DROP TABLE comm_note_messages;
DROP TABLE comm_notes; DROP TABLE comm_batches; DROP TABLE comm_cursors; DROP TABLE comm_settings;
ALTER TABLE catalog_items DROP COLUMN comm_source, DROP COLUMN adapter;
ALTER TABLE messages DROP COLUMN card;
DELETE FROM runs WHERE client_id IS NULL;
DELETE FROM service_agents WHERE name = 'memory-collector';
ALTER TABLE runs DROP CONSTRAINT runs_owner, ALTER COLUMN client_id SET NOT NULL, DROP COLUMN user_id;
DELETE FROM scheduled_tasks WHERE kind <> 'agent';
DROP INDEX scheduled_tasks_memory;
ALTER TABLE scheduled_tasks DROP COLUMN settings, DROP COLUMN kind;
