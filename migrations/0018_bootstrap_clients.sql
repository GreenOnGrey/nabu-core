-- FTR.NAB.CMN-0001 R17a, tech §5: names of the service clients that BOOTSTRAP_CLIENTS created. A name stays
-- here after an administrator deletes the client, so the next start does not bring it back.

-- +goose Up
CREATE TABLE bootstrap_clients (
  name TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE bootstrap_clients;
