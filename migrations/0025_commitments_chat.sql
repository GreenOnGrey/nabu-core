-- FTR.NAB.CMN-0003 tech, decision 57: the personal agent creates a commitment from the chat after the user's yes.
-- It has no note until the collector links the same commitment found in mail to it; the conversation it was
-- created in is kept, and lost with the conversation. A note that goes (deleted by the user, by the retention)
-- releases the commitments from the chat linked to it instead of taking them along: the user created them.

-- +goose Up
ALTER TABLE commitments
  ALTER COLUMN note_id DROP NOT NULL,
  ADD COLUMN origin TEXT NOT NULL DEFAULT 'collected' CHECK (origin IN ('collected','chat')),
  ADD COLUMN conversation_id UUID REFERENCES conversations(id) ON DELETE SET NULL,
  ADD CONSTRAINT commitments_basis CHECK (origin = 'chat' OR note_id IS NOT NULL);
CREATE INDEX commitments_chat ON commitments (user_id, created_at DESC) WHERE note_id IS NULL;

-- +goose StatementBegin
CREATE FUNCTION comm_release_chat_commitments() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  UPDATE commitments SET note_id = NULL, basis_message_id = NULL, updated_at = now()
    WHERE user_id = OLD.user_id AND note_id = OLD.id AND origin = 'chat';
  RETURN OLD;
END $$;
-- +goose StatementEnd
CREATE TRIGGER comm_notes_release_chat BEFORE DELETE ON comm_notes
  FOR EACH ROW EXECUTE FUNCTION comm_release_chat_commitments();

-- +goose Down
DROP TRIGGER comm_notes_release_chat ON comm_notes;
DROP FUNCTION comm_release_chat_commitments();
DELETE FROM commitments WHERE note_id IS NULL;
DROP INDEX commitments_chat;
ALTER TABLE commitments
  DROP CONSTRAINT commitments_basis,
  DROP COLUMN conversation_id,
  DROP COLUMN origin,
  ALTER COLUMN note_id SET NOT NULL;
