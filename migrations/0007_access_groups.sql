-- FTR.NAB.CMN-0007 tech §3: groups of users and what they grant.
-- Members of the built-in groups are implicit: 'all' — every user,
-- 'admins' — users.is_admin; 'admins' also has every enabled resource.
-- channel_user_access, channels.all_users and personal_models.available
-- are no longer read; the next release drops them.

-- +goose Up
CREATE TABLE user_groups (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name        TEXT NOT NULL,
  description TEXT,
  builtin     TEXT UNIQUE CHECK (builtin IN ('all','admins')),
  position    INTEGER NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX user_groups_name ON user_groups (lower(name));

CREATE TABLE user_group_members (
  group_id UUID NOT NULL REFERENCES user_groups(id) ON DELETE CASCADE,
  user_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  added_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (group_id, user_id)
);
CREATE INDEX ON user_group_members (user_id);

CREATE TABLE access_grants (
  id            BIGSERIAL PRIMARY KEY,
  group_id      UUID NOT NULL REFERENCES user_groups(id) ON DELETE CASCADE,
  kind          TEXT NOT NULL CHECK (kind IN ('model','channel','catalog_item')),
  connection_id UUID REFERENCES model_connections(id) ON DELETE CASCADE,
  model         TEXT,
  channel       TEXT REFERENCES channels(kind) ON DELETE CASCADE,
  item_id       UUID REFERENCES catalog_items(id) ON DELETE CASCADE,
  is_default    BOOLEAN NOT NULL DEFAULT false,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK ((kind = 'model') = (connection_id IS NOT NULL AND model IS NOT NULL)),
  CHECK ((kind = 'channel') = (channel IS NOT NULL)),
  CHECK ((kind = 'catalog_item') = (item_id IS NOT NULL)),
  CHECK (channel IS NULL OR channel <> 'web'),
  CHECK (NOT is_default OR kind = 'model')
);
CREATE UNIQUE INDEX access_grants_unique ON access_grants
  (group_id, kind, coalesce(connection_id::text || '/' || model, channel, item_id::text));
CREATE UNIQUE INDEX access_grants_one_default ON access_grants (group_id) WHERE is_default;
CREATE INDEX ON access_grants (item_id) WHERE item_id IS NOT NULL;

CREATE TABLE admin_log (
  id          BIGSERIAL PRIMARY KEY,
  at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  actor_id    UUID REFERENCES users(id) ON DELETE SET NULL,
  actor_email TEXT NOT NULL,
  action      TEXT NOT NULL,   -- group.create | group.update | group.delete | group.order | group.members.add | group.members.remove | group.grants | user.groups | user.admin
  target      TEXT NOT NULL,   -- group:<id> | user:<id>
  before      JSONB, after JSONB
);
CREATE INDEX ON admin_log (at DESC);
CREATE INDEX ON admin_log (target, at DESC);

INSERT INTO user_groups (name, builtin, position) VALUES ('All users', 'all', 2147483647), ('Administrators', 'admins', -1);

-- «Все пользователи» receives what everybody has now (R2)
INSERT INTO access_grants (group_id, kind, connection_id, model, is_default)
  SELECT g.id, 'model', (a->>'connectionId')::uuid, a->>'model',
         coalesce(s.value->'default'->>'connectionId' = a->>'connectionId' AND s.value->'default'->>'model' = a->>'model', false)
  FROM user_groups g, settings s, jsonb_array_elements(s.value->'available') a
  WHERE g.builtin = 'all' AND s.key = 'personal_models'
    AND EXISTS (SELECT 1 FROM model_connections c WHERE c.id = (a->>'connectionId')::uuid)
  ON CONFLICT DO NOTHING;
INSERT INTO access_grants (group_id, kind, channel)
  SELECT g.id, 'channel', c.kind FROM user_groups g, channels c
  WHERE g.builtin = 'all' AND c.all_users AND c.kind <> 'web';
INSERT INTO access_grants (group_id, kind, item_id)
  SELECT g.id, 'catalog_item', i.id FROM user_groups g, catalog_items i WHERE g.builtin = 'all' AND i.published;

-- +goose Down
DROP TABLE admin_log; DROP TABLE access_grants; DROP TABLE user_group_members; DROP TABLE user_groups;
