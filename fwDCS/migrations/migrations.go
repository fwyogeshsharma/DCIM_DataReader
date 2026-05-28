// Package migrations embeds versioned SQL migration files and exposes them in
// apply order. DCS runs them at startup through a ledger (schema_migrations)
// so each file is applied exactly once per database.
//
// Convention: files are named NNN_description.sql (zero-padded, monotonically
// increasing). Each file must be idempotent (IF NOT EXISTS / OR REPLACE / ADD
// COLUMN IF NOT EXISTS) so a re-run against a partially-migrated database is
// safe. To add a schema change, drop a new NNN_*.sql file in this directory —
// no Go code changes required; go:embed picks it up.
package migrations

import (
	"embed"
	"sort"
	"strings"
)

//go:embed *.sql
var files embed.FS

// Migration is one versioned SQL file.
type Migration struct {
	Version string // filename, e.g. "002_topology_relation.sql"
	SQL     string
}

// All returns every embedded migration sorted by filename (= apply order).
func All() ([]Migration, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, err
	}
	out := make([]Migration, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		b, err := files.ReadFile(e.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, Migration{Version: e.Name(), SQL: string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	return out, nil
}
