-- FTR.NAB.CMN-0001 tech §5, R17a: a client the configuration created follows the secret of the
-- configuration until an administrator reissues it. The name in bootstrap_clients alone does not
-- say who owns the row: an administrator may delete the bootstrapped client (the name becomes free)
-- and create a client with the same name. The mark separates the two.
--
-- The backfill marks the clients that still have the shape Bootstrap gives a client it created
-- (delegation, no other rights, no agents) and were never reissued. A client an administrator
-- created under such a name usually differs (agents, rights) and stays unmarked. A marked client
-- takes the secret of the configuration now and at the later starts — that is how a stand installed
-- with the hash of a secret with a trailing line end is healed.

-- +goose Up
ALTER TABLE service_clients ADD COLUMN bootstrapped BOOLEAN NOT NULL DEFAULT false;

-- +goose StatementBegin
DO $$
DECLARE marked int;
BEGIN
  UPDATE service_clients SET bootstrapped = true
  WHERE name IN (SELECT name FROM bootstrap_clients)
    AND can_delegate AND NOT can_import AND NOT can_archive AND NOT can_restore
    AND agents = '{}' AND prev_secret_hash IS NULL;
  GET DIAGNOSTICS marked = ROW_COUNT;
  IF marked > 0 THEN
    RAISE NOTICE 'bootstrapped clients marked by the migration: %', marked;
  END IF;
END $$;
-- +goose StatementEnd

-- +goose Down
ALTER TABLE service_clients DROP COLUMN bootstrapped;
