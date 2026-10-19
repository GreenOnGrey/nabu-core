-- FTR.NAB.CHAT-0001 tech §3: events of a conversation (a reset, a compaction,
-- a change of the model), the skill and the voice mark of a message, the
-- context of a session, new types of model connections and the capabilities
-- of models with their sources.
-- models[] of a connection gain tools, sources, overrides and gone; the
-- existing models keep tools = true — they worked as agents before.

-- +goose Up
ALTER TABLE messages DROP CONSTRAINT messages_role_check;
ALTER TABLE messages ADD CONSTRAINT messages_role_check CHECK (role IN ('user','assistant','event'));
ALTER TABLE messages
  ADD COLUMN event JSONB,                    -- {kind: reset|compact|model, …}
  ADD COLUMN skill TEXT,
  ADD COLUMN voice BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE harness_sessions
  ADD COLUMN context_tokens BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN context_window BIGINT NOT NULL DEFAULT 0;

ALTER TABLE model_connections DROP CONSTRAINT model_connections_type_check;
ALTER TABLE model_connections ADD CONSTRAINT model_connections_type_check
  CHECK (type IN ('deepseek','openai_compatible','gigachat','zai'));
ALTER TABLE model_connections
  ADD COLUMN options JSONB NOT NULL DEFAULT '{}',
  ADD COLUMN discovered JSONB NOT NULL DEFAULT '[]',   -- new models of the provider, not added
  ADD COLUMN refreshed_at TIMESTAMPTZ;
UPDATE model_connections SET models = (
  SELECT coalesce(jsonb_agg(m || jsonb_build_object('tools', true, 'sources', '{}'::jsonb, 'overrides', '[]'::jsonb, 'gone', false)), '[]')
  FROM jsonb_array_elements(models) m);

-- +goose Down
ALTER TABLE model_connections DROP COLUMN refreshed_at, DROP COLUMN discovered, DROP COLUMN options;
DELETE FROM model_connections WHERE type IN ('gigachat','zai');
ALTER TABLE model_connections DROP CONSTRAINT model_connections_type_check;
ALTER TABLE model_connections ADD CONSTRAINT model_connections_type_check CHECK (type IN ('deepseek','openai_compatible'));
ALTER TABLE harness_sessions DROP COLUMN context_window, DROP COLUMN context_tokens;
DELETE FROM messages WHERE role = 'event';
ALTER TABLE messages DROP COLUMN voice, DROP COLUMN skill, DROP COLUMN event;
ALTER TABLE messages DROP CONSTRAINT messages_role_check;
ALTER TABLE messages ADD CONSTRAINT messages_role_check CHECK (role IN ('user','assistant'));
