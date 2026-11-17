-- FTR.NAB.CMN-0001 tech §12, решение 26, FTR.NAB.CMN-0007 tech §3: the search and the
-- uniqueness of a name do not depend on the case of the letters. The database
-- of the stand is created with the locale C, where lower() and ILIKE fold only
-- ASCII: «кон» did not find «Константин», and «Юристы» went beside «юристы».
-- The collation folds the letters of any language the same way on every
-- server, whatever its locale is. A pair of names that differ only in case
-- ("Юристы" beside "юристы") would break the index: such rows are merged by
-- hand before the deploy, and the migration then finds nothing to stop.

-- +goose Up
CREATE COLLATION IF NOT EXISTS nabu_ci (provider = icu, locale = 'ru');
DROP INDEX IF EXISTS user_groups_name;
CREATE UNIQUE INDEX user_groups_name ON user_groups ((lower(name COLLATE nabu_ci)));

-- +goose Down
DROP INDEX IF EXISTS user_groups_name;
CREATE UNIQUE INDEX user_groups_name ON user_groups (lower(name));
DROP COLLATION IF EXISTS nabu_ci;
