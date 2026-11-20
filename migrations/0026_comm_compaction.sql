-- FTR.NAB.MEM-0001 tech §9: the levels of the records of the memory of communications, the days of communication,
-- the aggregate of the graph by day and the usage by day.
--
-- A record has a level (day — a chat for a day, week, month — compacted chats, thread — a mail thread) and a period.
-- comm_note_days holds the dates of the messages of a record in the time zone of the user: the measure of the graph
-- and of the counters (R20) that does not change when records are compacted. comm_tag_days and comm_pair_days are
-- the graph without filters by person, topic and text, kept up to date by comm_refresh_notes and comm_refresh_days
-- in the transaction that changes records or tags (arch A6).

-- +goose Up
ALTER TABLE comm_notes
  ADD COLUMN level TEXT NOT NULL DEFAULT 'day' CHECK (level IN ('day','week','month','thread')),
  ADD COLUMN period_from DATE,
  ADD COLUMN period_to DATE,
  ADD COLUMN days INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN chars_in INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN compacted_from INTEGER,
  ADD COLUMN compacted_at TIMESTAMPTZ;
CREATE INDEX comm_notes_period ON comm_notes (user_id, level, period_to);

ALTER TABLE comm_note_messages ADD COLUMN key BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE comm_batches ADD COLUMN kind TEXT NOT NULL DEFAULT 'collect' CHECK (kind IN ('collect','compact'));

CREATE TABLE comm_note_days (
  user_id UUID NOT NULL,
  note_id UUID NOT NULL,
  day     DATE NOT NULL,
  PRIMARY KEY (user_id, note_id, day),
  FOREIGN KEY (user_id, note_id) REFERENCES comm_notes (user_id, id) ON DELETE CASCADE
) PARTITION BY HASH (user_id);
CREATE INDEX comm_note_days_day ON comm_note_days (user_id, day);
CREATE TABLE comm_tag_days (
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  day     DATE NOT NULL,
  source  TEXT NOT NULL,
  kind    TEXT NOT NULL,
  tag_id  UUID NOT NULL,
  PRIMARY KEY (user_id, day, source, kind, tag_id)
) PARTITION BY HASH (user_id);
CREATE TABLE comm_pair_days (
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  day     DATE NOT NULL,
  source  TEXT NOT NULL,
  a       UUID NOT NULL,
  b       UUID NOT NULL,
  PRIMARY KEY (user_id, day, source, a, b)
) PARTITION BY HASH (user_id);
-- the tags the user took off a conversation: a compaction does not give them back (R10)
CREATE TABLE comm_tag_removals (
  user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  item_id    UUID NOT NULL,
  source     TEXT NOT NULL,
  thread_key TEXT NOT NULL,
  kind       TEXT NOT NULL,
  tag_id     UUID NOT NULL,
  PRIMARY KEY (user_id, item_id, source, thread_key, kind, tag_id)
);

-- +goose StatementBegin
DO $$
BEGIN
  FOR i IN 0..31 LOOP
    EXECUTE format('CREATE TABLE comm_note_days_p%s PARTITION OF comm_note_days FOR VALUES WITH (MODULUS 32, REMAINDER %s)', lpad(i::text, 2, '0'), i);
    EXECUTE format('CREATE TABLE comm_tag_days_p%s PARTITION OF comm_tag_days FOR VALUES WITH (MODULUS 32, REMAINDER %s)', lpad(i::text, 2, '0'), i);
    EXECUTE format('CREATE TABLE comm_pair_days_p%s PARTITION OF comm_pair_days FOR VALUES WITH (MODULUS 32, REMAINDER %s)', lpad(i::text, 2, '0'), i);
  END LOOP;
END $$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['comm_note_days','comm_tag_days','comm_pair_days','comm_tag_removals'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format($p$CREATE POLICY comm_owner ON %I USING (
      user_id = nullif(current_setting('nabu.user_id', true), '')::uuid
      OR current_setting('nabu.maintenance', true) = 'on')$p$, t);
  END LOOP;
END $$;
-- +goose StatementEnd

-- The usage by day (R25, R26): numbers only, no content, hence no row policy, as comm_usage.
ALTER TABLE comm_usage ADD COLUMN compact_cost_usd NUMERIC(12,4) NOT NULL DEFAULT 0;
CREATE TABLE comm_usage_days (
  user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  day            DATE NOT NULL,
  runs           INTEGER NOT NULL DEFAULT 0,
  created        INTEGER NOT NULL DEFAULT 0,
  cost_collect   NUMERIC(12,4) NOT NULL DEFAULT 0,
  cost_compact   NUMERIC(12,4) NOT NULL DEFAULT 0,
  chars_in       BIGINT NOT NULL DEFAULT 0,
  chars_out      BIGINT NOT NULL DEFAULT 0,
  compact_ok     INTEGER NOT NULL DEFAULT 0,
  compact_failed INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (user_id, day)
);
CREATE INDEX comm_usage_days_day ON comm_usage_days (day);
CREATE TABLE comm_usage_source_days (
  user_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  day      DATE NOT NULL,
  item_id  UUID NOT NULL,
  source   TEXT NOT NULL,
  messages INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (user_id, day, item_id, source)
);
CREATE TABLE comm_level_days (
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  day     DATE NOT NULL,
  level   TEXT NOT NULL,
  records INTEGER NOT NULL,
  PRIMARY KEY (user_id, day, level)
);
CREATE INDEX comm_level_days_day ON comm_level_days (day);
CREATE TABLE comm_compact_state (
  user_id       UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  last_at       TIMESTAMPTZ,
  last_night    DATE,
  failed_nights INTEGER NOT NULL DEFAULT 0
);

-- The graph of the given days of a user: the presence of a tag and of a pair of tags on a day, by source.
-- +goose StatementBegin
CREATE FUNCTION comm_refresh_days(p_user UUID, p_days DATE[]) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF p_days IS NULL OR cardinality(p_days) = 0 THEN
    RETURN;
  END IF;
  DELETE FROM comm_tag_days WHERE user_id = p_user AND day = ANY(p_days);
  DELETE FROM comm_pair_days WHERE user_id = p_user AND day = ANY(p_days);
  INSERT INTO comm_tag_days (user_id, day, source, kind, tag_id)
    SELECT DISTINCT d.user_id, d.day, n.source, t.kind, t.tag_id
    FROM comm_note_days d
    JOIN comm_notes n ON n.user_id = d.user_id AND n.id = d.note_id
    JOIN comm_note_tags t ON t.user_id = d.user_id AND t.note_id = d.note_id
    WHERE d.user_id = p_user AND d.day = ANY(p_days);
  INSERT INTO comm_pair_days (user_id, day, source, a, b)
    SELECT DISTINCT d.user_id, d.day, n.source, a.tag_id, b.tag_id
    FROM comm_note_days d
    JOIN comm_notes n ON n.user_id = d.user_id AND n.id = d.note_id
    JOIN comm_note_tags a ON a.user_id = d.user_id AND a.note_id = d.note_id
    JOIN comm_note_tags b ON b.user_id = d.user_id AND b.note_id = d.note_id AND (a.kind, a.tag_id) < (b.kind, b.tag_id)
    WHERE d.user_id = p_user AND d.day = ANY(p_days);
END $$;
-- +goose StatementEnd

-- The days of records from their messages, in the time zone of the user (a source that gives only the date — by
-- UTC, as it gave it); then the counters and the period of the records of a day or a thread, then the graph of the
-- days that were and are now.
-- +goose StatementBegin
CREATE FUNCTION comm_refresh_notes(p_user UUID, p_notes UUID[]) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  tz TEXT;
  was DATE[];
  now_days DATE[];
BEGIN
  IF p_notes IS NULL OR cardinality(p_notes) = 0 THEN
    RETURN;
  END IF;
  SELECT COALESCE(NULLIF(timezone, ''), 'UTC') INTO tz FROM users WHERE id = p_user;
  BEGIN
    PERFORM now() AT TIME ZONE COALESCE(tz, 'UTC');
  EXCEPTION WHEN others THEN
    tz := 'UTC';
  END;
  tz := COALESCE(tz, 'UTC');
  SELECT COALESCE(array_agg(DISTINCT day), '{}') INTO was FROM comm_note_days WHERE user_id = p_user AND note_id = ANY(p_notes);
  DELETE FROM comm_note_days WHERE user_id = p_user AND note_id = ANY(p_notes);
  INSERT INTO comm_note_days (user_id, note_id, day)
    SELECT DISTINCT n.user_id, n.id,
      (COALESCE(m.sent_at, n.last_at) AT TIME ZONE CASE WHEN n.time_precision = 'day' THEN 'UTC' ELSE tz END)::date
    FROM comm_notes n
    LEFT JOIN comm_note_messages m ON m.user_id = n.user_id AND m.note_id = n.id
    WHERE n.user_id = p_user AND n.id = ANY(p_notes);
  UPDATE comm_notes n SET
      days = GREATEST(1, x.days),
      period_from = CASE WHEN n.level IN ('day','thread') THEN x.first ELSE COALESCE(n.period_from, x.first) END,
      period_to = CASE WHEN n.level IN ('day','thread') THEN x.last ELSE COALESCE(n.period_to, x.last) END
    FROM (SELECT note_id, count(*) AS days, min(day) AS first, max(day) AS last FROM comm_note_days
          WHERE user_id = p_user AND note_id = ANY(p_notes) GROUP BY note_id) x
    WHERE n.user_id = p_user AND n.id = x.note_id;
  SELECT COALESCE(array_agg(DISTINCT day), '{}') INTO now_days FROM comm_note_days WHERE user_id = p_user AND note_id = ANY(p_notes);
  PERFORM comm_refresh_days(p_user, was || now_days);
END $$;
-- +goose StatementEnd

-- The records collected before: a level by the key, the days, the period, the characters; then the graph.
-- +goose StatementBegin
DO $$
DECLARE u UUID;
BEGIN
  PERFORM set_config('nabu.maintenance', 'on', true);
  UPDATE comm_notes SET level = CASE WHEN day_key = '' THEN 'thread' ELSE 'day' END,
    chars_in = char_length(summary) + COALESCE((SELECT sum(char_length(e->>'text')) FROM jsonb_array_elements(appended) e), 0);
  FOR u IN SELECT DISTINCT user_id FROM comm_notes LOOP
    PERFORM comm_refresh_notes(u, ARRAY(SELECT id FROM comm_notes WHERE user_id = u));
  END LOOP;
  PERFORM set_config('nabu.maintenance', 'off', true);
END $$;
-- +goose StatementEnd

-- The retention in months (R16): the days of the administrator rounded up, 180 → 6, at most 60 months.
UPDATE settings SET value = (value - 'retentionDays') || jsonb_build_object('retentionMonths', LEAST(60, GREATEST(1, ceil((value->>'retentionDays')::numeric / 30)::int)))
  WHERE key = 'comm' AND value ? 'retentionDays';

-- +goose Down
UPDATE settings SET value = (value - 'retentionMonths') || jsonb_build_object('retentionDays', (value->>'retentionMonths')::int * 30)
  WHERE key = 'comm' AND value ? 'retentionMonths';
DROP FUNCTION comm_refresh_notes(UUID, UUID[]);
DROP FUNCTION comm_refresh_days(UUID, DATE[]);
DROP TABLE comm_compact_state;
DROP TABLE comm_level_days;
DROP TABLE comm_usage_source_days;
DROP TABLE comm_usage_days;
ALTER TABLE comm_usage DROP COLUMN compact_cost_usd;
DROP TABLE comm_tag_removals;
DROP TABLE comm_pair_days;
DROP TABLE comm_tag_days;
DROP TABLE comm_note_days;
ALTER TABLE comm_batches DROP COLUMN kind;
ALTER TABLE comm_note_messages DROP COLUMN key;
DROP INDEX comm_notes_period;
ALTER TABLE comm_notes DROP COLUMN compacted_at, DROP COLUMN compacted_from, DROP COLUMN chars_in, DROP COLUMN days,
  DROP COLUMN period_to, DROP COLUMN period_from, DROP COLUMN level;
