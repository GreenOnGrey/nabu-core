package catalogusage

import "github.com/GreenOnGrey/nabu-core/internal/config"

// ConfigFrom reads the thresholds and windows of the report from the
// environment (FTR.NAB.CHAT-0005 tech §«Конфигурация»).
func ConfigFrom(c *config.Config) Config {
	if c == nil {
		return DefaultConfig()
	}
	return Config{UnusedDays: c.CatalogUsageUnusedDays, ReferenceDays: c.CatalogUsageReferenceDays, PinDays: c.CatalogUsagePinDays,
		SingleUserMax: c.CatalogUsageSingleUserMax, ErrorsShare: c.CatalogUsageErrorsShare, ErrorsMinCalls: c.CatalogUsageErrorsMinCalls,
		AuthShare: c.CatalogUsageAuthShare, AuthMinCalls: c.CatalogUsageAuthMinCalls, QuickDays: c.CatalogUsageQuickDays,
		DrilldownDays: c.CatalogUsageDrilldownDays, ToolsCandidate: c.CatalogUsageToolsCandidate, Tick: c.CatalogUsageTick,
		CloseAfter: c.CatalogUsageCloseAfter, RetentionDays: c.CatalogUsageRetentionDays, MonthlyMonths: c.CatalogUsageMonthlyMonths}
}
