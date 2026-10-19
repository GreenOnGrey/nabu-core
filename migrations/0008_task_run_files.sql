-- FTR.NAB.CHAT-0001 R33: the files the agent sends during a run of a task wait
-- here for its result; a failed or restarted run drops them.

-- +goose Up
ALTER TABLE task_runs ADD COLUMN attachments JSONB NOT NULL DEFAULT '[]';

-- +goose Down
ALTER TABLE task_runs DROP COLUMN attachments;
