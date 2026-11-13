-- FTR.NAB.ADM-0001 R10, tech §4.3: the chats of VK Teams where a tick stopped at the limit of messages,
-- with how many messages it read ({"<chat>": <count>}). The first batch of a source keeps them; the result
-- of the run of the task names them.

-- +goose Up
ALTER TABLE comm_batches ADD COLUMN truncated JSONB;

-- +goose Down
ALTER TABLE comm_batches DROP COLUMN truncated;
