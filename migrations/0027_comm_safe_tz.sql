-- FTR.NAB.MEM-0001 tech §6.5: the time zone of a user as Postgres knows it. A zone the application accepted (Go's
-- tzdata) but the database does not know becomes UTC, so one user cannot stop the compaction of everyone.

-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION comm_safe_tz(tz TEXT) RETURNS TEXT LANGUAGE plpgsql STABLE AS $$
BEGIN
  IF tz IS NULL OR tz = '' THEN
    RETURN 'UTC';
  END IF;
  PERFORM now() AT TIME ZONE tz;
  RETURN tz;
EXCEPTION WHEN others THEN
  RETURN 'UTC';
END $$;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION comm_safe_tz(TEXT);
