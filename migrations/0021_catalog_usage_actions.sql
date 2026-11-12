-- FTR.NAB.CHAT-0005 tech §«Схема БД»: the values of admin_log of the actions
-- of the usage tab, the baseline of the goal metric and the mark of the
-- one-time reconciliation of access at the rollout window.

-- +goose Up
ALTER TABLE admin_log ADD CONSTRAINT admin_log_action_catalog_chk
  CHECK (action NOT IN ('catalog.publish','catalog.unpublish','catalog.grant.revoke','catalog.delete')
         OR target LIKE 'catalog_item:%');

INSERT INTO settings (key, value)
VALUES ('catalog_usage_goal',
        '{"baseline": null, "baselineTo": null, "verifiedManuallyAt": null}')
ON CONFLICT (key) DO NOTHING;

INSERT INTO settings (key, value)
VALUES ('catalog_access_reconcile', '{"done": false, "at": null, "revoked": 0}')
ON CONFLICT (key) DO NOTHING;

-- +goose Down
DELETE FROM settings WHERE key = 'catalog_access_reconcile';
DELETE FROM settings WHERE key = 'catalog_usage_goal';
ALTER TABLE admin_log DROP CONSTRAINT admin_log_action_catalog_chk;
