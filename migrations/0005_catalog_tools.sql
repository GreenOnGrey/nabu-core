-- FTR.NAB.CMN-0006 tech §3.1: the tools an administrator allows in an MCP
-- item and the arguments that are files of the user's space.
-- allowed_tools NULL keeps the old rule of read_only (items never checked,
-- old clients of the API); the update below gives every checked item the
-- tools it exposes now, so nothing changes at the rollout (CAT-05, CAT-06).
-- new_tools are the tools that appeared at a check and wait for the
-- administrator: the form marks them, saving the list clears them (CAT-03).
-- checked_at is the time of the last successful check, shown in the form.

-- +goose Up
ALTER TABLE catalog_items
  ADD COLUMN allowed_tools  JSONB,
  ADD COLUMN file_args      JSONB NOT NULL DEFAULT '[]',
  ADD COLUMN file_max_bytes BIGINT NOT NULL DEFAULT 20971520,
  ADD COLUMN new_tools      JSONB NOT NULL DEFAULT '[]',
  ADD COLUMN checked_at     TIMESTAMPTZ;

UPDATE catalog_items SET allowed_tools = (
  SELECT coalesce(jsonb_agg(t->'name'), '[]'::jsonb) FROM jsonb_array_elements(tools) t
  WHERE NOT read_only OR coalesce((t->>'readOnly')::boolean, false))
WHERE type = 'mcp' AND tools IS NOT NULL AND jsonb_typeof(tools) = 'array' AND jsonb_array_length(tools) > 0;

-- +goose Down
ALTER TABLE catalog_items DROP COLUMN checked_at, DROP COLUMN new_tools, DROP COLUMN file_max_bytes, DROP COLUMN file_args, DROP COLUMN allowed_tools;
