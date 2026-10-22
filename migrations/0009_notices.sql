-- FTR.NAB.CHAT-0002: notices of products in the topics of users (role
-- 'notice'): the client that wrote it, the data the product draws, the key
-- that makes a repeat return the same message.

-- +goose Up
ALTER TABLE messages DROP CONSTRAINT messages_role_check;
ALTER TABLE messages ADD CONSTRAINT messages_role_check CHECK (role IN ('user','assistant','event','notice'));
ALTER TABLE messages
  ADD COLUMN client_id       UUID REFERENCES service_clients(id) ON DELETE SET NULL,
  ADD COLUMN payload         JSONB,
  ADD COLUMN idempotency_key TEXT;
CREATE UNIQUE INDEX messages_notice_idem ON messages (client_id, conversation_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
CREATE INDEX messages_notice_conv ON messages (conversation_id, created_at) WHERE role = 'notice';

-- +goose Down
DELETE FROM messages WHERE role = 'notice';
DROP INDEX IF EXISTS messages_notice_conv;
DROP INDEX IF EXISTS messages_notice_idem;
ALTER TABLE messages DROP COLUMN idempotency_key, DROP COLUMN payload, DROP COLUMN client_id;
ALTER TABLE messages DROP CONSTRAINT messages_role_check;
ALTER TABLE messages ADD CONSTRAINT messages_role_check CHECK (role IN ('user','assistant','event'));
