-- FTR.NAB.ADM-0001 R13: the catalog has no mark «in development». Every item becomes an ordinary one: an
-- item not ready for users is the one not published. The column stays (migrations are expand-only); the
-- code writes false into it and reads it nowhere.

-- +goose Up
UPDATE catalog_items SET in_development = false WHERE in_development;

-- +goose Down
-- the marks are not restored: nothing reads them
SELECT 1;
