-- FTR.NAB.CMN-0001 tech §3.2, decision 27: the user stream of the site is resumable. Postgres NOTIFY carries the
-- event to the api pods but keeps nothing: an event published while a listener reconnects is lost, and the SSE
-- connection of the browser survives that (the api pod is alive), so the page waits for a `message.done` that will
-- never come.
--
-- The event: on the stand (2026-11-25) Postgres restarted 11 times (OOMKilled), the listener of the api pod logged
-- `event listener failed, reconnecting`, `message.done` was lost, and the chat showed a finished answer as
-- "The agent stopped unexpectedly" 31 minutes later.

-- +goose Up
-- The journal of the user stream: one row per event a client may miss, kept for an hour by the worker.
CREATE TABLE events (
  id BIGSERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  data JSONB,
  at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- the replay of one connection reads the rows of one user after an id
CREATE INDEX events_user_id_id ON events (user_id, id);
CREATE INDEX events_at ON events (at);

-- +goose Down
DROP TABLE events;
