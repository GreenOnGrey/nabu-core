// Package migrations embeds the goose migrations of the nabu database
// (FTR.NAB.CMN-0001 tech §10).
package migrations

import "embed"

// FS holds the SQL migrations.
//
//go:embed *.sql
var FS embed.FS
