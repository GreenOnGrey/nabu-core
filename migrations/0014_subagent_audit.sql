-- FTR.NAB.CMN-0009 R9, tech §3: a tool call of a subagent is audited with
-- the call of the tool subagent, the task and its description,
-- '<call>/<task> <description>'.

-- +goose Up
ALTER TABLE audit ADD COLUMN subagent text;

-- +goose Down
ALTER TABLE audit DROP COLUMN subagent;
