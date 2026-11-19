-- FTR.NAB.CMN-0003 tech, decision 52: the time limit of a run of the collector is a setting of the
-- administration and can change while a batch runs. The batch keeps the end of its own run: the check of stale
-- batches reads it, so a lowered limit does not fail the batches that started with the old one.

-- +goose Up
ALTER TABLE comm_batches ADD COLUMN deadline_at TIMESTAMPTZ;

-- +goose Down
ALTER TABLE comm_batches DROP COLUMN deadline_at;
