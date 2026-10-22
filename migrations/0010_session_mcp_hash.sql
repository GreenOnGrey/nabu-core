-- FTR.NAB.CHAT-0002: the fingerprint of the catalog items a session of an
-- agent was opened with (their tools and the tools the administrator
-- allowed). A session whose items changed is opened again from its snapshot:
-- the agent gets the new tools in the next turn, with its context.

-- +goose Up
ALTER TABLE harness_sessions ADD COLUMN mcp_hash TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE harness_sessions DROP COLUMN mcp_hash;
