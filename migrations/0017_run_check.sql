-- FTR.HMR.CMN-0008 tech §3: the result of the readiness check of a run;
-- FTR.NAB.CMN-0010 tech §3: why the run was stopped.

-- +goose Up
ALTER TABLE runs ADD COLUMN check_result JSONB, ADD COLUMN stop_reason TEXT;

-- +goose Down
ALTER TABLE runs DROP COLUMN stop_reason, DROP COLUMN check_result;
