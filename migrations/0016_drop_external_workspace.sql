-- FTR.HMR.CMN-0007 R12, tech §5, §9.2: the workspace of the caller is gone;
-- its agents (the code scenarios of Hammurapi) move to the sandbox of Nabu
-- with the profile code and the proxy of package registries. The profile code
-- comes with this version; until the api syncs it a run falls back to the
-- default of the stand (FTR.NAB.CMN-0008 R6).

-- +goose Up
-- From the system: an action of an administrator without a user, as
-- proxies.ActorUUID("system") writes it.
INSERT INTO audit (agent_kind, agent, channel, tool, args_summary, result)
SELECT 'personal', 'nabu', 'admin', 'agent.update',
       json_build_object('actor', 'system', 'agent', name, 'reason', 'FTR.HMR.CMN-0007 R12', 'before', json_build_object('workspace', 'external'),
                         'after', json_build_object('workspace', 'nabu', 'workspaceProfile', 'code', 'proxies', '["packages"]'::json))::text, 'ok'
FROM service_agents WHERE config->>'workspace' = 'external';

UPDATE service_agents SET config = config
    || '{"workspace": "nabu", "workspaceProfile": "code"}'::jsonb
    || jsonb_build_object('proxies', CASE WHEN EXISTS (SELECT 1 FROM proxies WHERE name = 'packages')
                                          THEN '["packages"]'::jsonb ELSE '[]'::jsonb END),
    updated_at = now()
WHERE config->>'workspace' = 'external';

-- +goose Down
-- Nothing to restore: the previous version reads the moved agents as agents
-- with the sandbox of Nabu.
SELECT 1;
