-- FTR.NAB.CMN-0003 tech §6: the days of a record and the graph of the days are refreshed by a function that deletes
-- the rows and inserts them again — two calls for one user at the same time both delete nothing and both insert the
-- same rows, and the second one fails with a duplicate key (comm_tag_days_p04_pkey). The lock of the user serialises
-- the calls, and the inserts take a row that another call has just written.
--
-- The event: a note saved by the collection of one source while the user answers a commitment of the same day; the
-- audit of the stand showed both calls within one second, and the note of the collection was lost.

-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION comm_refresh_days(p_user UUID, p_days DATE[]) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF p_days IS NULL OR cardinality(p_days) = 0 THEN
    RETURN;
  END IF;
  PERFORM pg_advisory_xact_lock(hashtext('nabu:comm:days:' || p_user::text));
  DELETE FROM comm_tag_days WHERE user_id = p_user AND day = ANY(p_days);
  DELETE FROM comm_pair_days WHERE user_id = p_user AND day = ANY(p_days);
  INSERT INTO comm_tag_days (user_id, day, source, kind, tag_id)
    SELECT DISTINCT d.user_id, d.day, n.source, t.kind, t.tag_id
    FROM comm_note_days d
    JOIN comm_notes n ON n.user_id = d.user_id AND n.id = d.note_id
    JOIN comm_note_tags t ON t.user_id = d.user_id AND t.note_id = d.note_id
    WHERE d.user_id = p_user AND d.day = ANY(p_days)
    ON CONFLICT DO NOTHING;
  INSERT INTO comm_pair_days (user_id, day, source, a, b)
    SELECT DISTINCT d.user_id, d.day, n.source, a.tag_id, b.tag_id
    FROM comm_note_days d
    JOIN comm_notes n ON n.user_id = d.user_id AND n.id = d.note_id
    JOIN comm_note_tags a ON a.user_id = d.user_id AND a.note_id = d.note_id
    JOIN comm_note_tags b ON b.user_id = d.user_id AND b.note_id = d.note_id AND (a.kind, a.tag_id) < (b.kind, b.tag_id)
    WHERE d.user_id = p_user AND d.day = ANY(p_days)
    ON CONFLICT DO NOTHING;
END $$;
-- +goose StatementEnd

-- The same for the days of the records: the lock of the user is taken once and held by the transaction, so a call
-- inside this one (comm_refresh_days) does not wait for it.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION comm_refresh_notes(p_user UUID, p_notes UUID[]) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  tz TEXT;
  was DATE[];
  now_days DATE[];
BEGIN
  IF p_notes IS NULL OR cardinality(p_notes) = 0 THEN
    RETURN;
  END IF;
  PERFORM pg_advisory_xact_lock(hashtext('nabu:comm:days:' || p_user::text));
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
    WHERE n.user_id = p_user AND n.id = ANY(p_notes)
    ON CONFLICT DO NOTHING;
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

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION comm_refresh_days(p_user UUID, p_days DATE[]) RETURNS void LANGUAGE plpgsql AS $$
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
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION comm_refresh_notes(p_user UUID, p_notes UUID[]) RETURNS void LANGUAGE plpgsql AS $$
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
