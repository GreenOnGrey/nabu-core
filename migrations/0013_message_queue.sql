-- FTR.NAB.CHAT-0004 tech §3: the queue of a conversation — user messages
-- waiting for their answer (R1, R3) — and the stop of an answer (R8, R9).

-- +goose Up
ALTER TABLE messages ADD COLUMN queue text CHECK (queue IN ('waiting','started','cancelled'));
CREATE INDEX messages_waiting ON messages (conversation_id, created_at) WHERE queue = 'waiting';
ALTER TABLE messages ADD COLUMN stop_requested_at timestamptz;
ALTER TABLE messages DROP CONSTRAINT messages_status_check;
ALTER TABLE messages ADD CONSTRAINT messages_status_check CHECK (status IN ('pending','streaming','done','failed','stopped'));

-- +goose Down
UPDATE messages SET status = 'failed' WHERE status = 'stopped';
ALTER TABLE messages DROP CONSTRAINT messages_status_check;
ALTER TABLE messages ADD CONSTRAINT messages_status_check CHECK (status IN ('pending','streaming','done','failed'));
ALTER TABLE messages DROP COLUMN stop_requested_at;
DROP INDEX messages_waiting;
ALTER TABLE messages DROP COLUMN queue;
