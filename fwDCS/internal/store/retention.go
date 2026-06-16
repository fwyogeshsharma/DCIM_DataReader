package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/faberwork/fwdcs/pkg/config"
)

// retentionTarget binds a hypertable to its 5-minute continuous-aggregate view
// and the columns used as the compression segment key.
type retentionTarget struct {
	policy    config.RetentionPolicy
	table     string // hypertable
	rollup    string // 5m continuous aggregate view
	segmentBy string // timescaledb.compress_segmentby
}

// ApplyRetention reconciles the TimescaleDB lifecycle policies for the metrics
// and energy_metrics hypertables to match cfg. It runs on every boot (called
// from main after store.New), so changing a value in dcs.yaml and restarting is
// all it takes to re-tune disk usage — no migration required.
//
// For each table it: (1) sets the future chunk interval, (2) drops and re-adds
// the raw retention policy, (3) optionally enables compression + adds a
// compression policy, and (4) drops and re-adds the rollup retention policy.
// Empty duration strings disable the corresponding policy. The drop-then-add
// pattern guarantees the configured interval wins even when a previous policy
// (e.g. the 30-day default baked into 001_init.sql) already exists.
func (db *DB) ApplyRetention(ctx context.Context, cfg config.RetentionConfig) error {
	if !cfg.Enabled {
		return nil
	}
	targets := []retentionTarget{
		{cfg.Metrics, "metrics", "metrics_5m", "device_id, metric_name, tag"},
		{cfg.Energy, "energy_metrics", "energy_metrics_5m", "device_id, metric_name, circuit, phase"},
	}
	for _, t := range targets {
		if err := db.applyRetention(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) applyRetention(ctx context.Context, t retentionTarget) error {
	// 1. Chunk interval — affects only chunks created after this call. Existing
	//    chunks keep their original size.
	chunk, err := toInterval(t.policy.ChunkInterval)
	if err != nil {
		return err
	}
	if chunk != "" {
		if _, err := db.pool.Exec(ctx,
			fmt.Sprintf(`SELECT set_chunk_time_interval('%s', INTERVAL '%s')`, t.table, chunk)); err != nil {
			return fmt.Errorf("retention: set chunk interval %s: %w", t.table, err)
		}
	}

	// 2. Raw retention — always drop the existing policy first, then re-add with
	//    the configured value so the dcs.yaml value is authoritative.
	if err := db.reconcileRetention(ctx, t.table, t.policy.RawRetention); err != nil {
		return err
	}

	// 3. Compression (optional). Always remove the existing policy; only enable
	//    + re-add when a CompressAfter is configured.
	if _, err := db.pool.Exec(ctx,
		fmt.Sprintf(`SELECT remove_compression_policy('%s', if_exists => true)`, t.table)); err != nil {
		return fmt.Errorf("retention: remove compression policy %s: %w", t.table, err)
	}
	compress, err := toInterval(t.policy.CompressAfter)
	if err != nil {
		return err
	}
	if compress != "" {
		if _, err := db.pool.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE %s SET (timescaledb.compress, timescaledb.compress_segmentby = '%s', timescaledb.compress_orderby = 'ts DESC')`,
			t.table, t.segmentBy)); err != nil {
			return fmt.Errorf("retention: enable compression %s: %w", t.table, err)
		}
		if _, err := db.pool.Exec(ctx,
			fmt.Sprintf(`SELECT add_compression_policy('%s', INTERVAL '%s', if_not_exists => true)`, t.table, compress)); err != nil {
			return fmt.Errorf("retention: add compression policy %s: %w", t.table, err)
		}
	}

	// 4. Rollup (5m continuous aggregate) retention.
	if err := db.reconcileRetention(ctx, t.rollup, t.policy.RollupRetention); err != nil {
		return err
	}
	return nil
}

// reconcileRetention drops any existing retention policy on relation and re-adds
// one for the given duration. An empty duration leaves the relation with no
// retention policy (kept forever).
func (db *DB) reconcileRetention(ctx context.Context, relation, dur string) error {
	if _, err := db.pool.Exec(ctx,
		fmt.Sprintf(`SELECT remove_retention_policy('%s', if_exists => true)`, relation)); err != nil {
		return fmt.Errorf("retention: remove policy %s: %w", relation, err)
	}
	iv, err := toInterval(dur)
	if err != nil {
		return err
	}
	if iv == "" {
		return nil
	}
	// schedule_interval controls how OFTEN the policy runs (TimescaleDB defaults
	// it to 1 day). With a tiny raw_retention like 20m, a daily sweep lets raw
	// rows pile up for a full day before each drop — the table balloons to ~GBs
	// then collapses (sawtooth). Run the job at half the retention window (capped
	// to a sane [5m, 12h] range) so dropped chunks are reclaimed promptly and the
	// table size stays bounded near the retention window.
	sched := scheduleFor(iv)
	if _, err := db.pool.Exec(ctx,
		fmt.Sprintf(`SELECT add_retention_policy('%s', INTERVAL '%s', schedule_interval => INTERVAL '%s', if_not_exists => true)`, relation, iv, sched)); err != nil {
		return fmt.Errorf("retention: add policy %s: %w", relation, err)
	}
	return nil
}

// scheduleFor picks how often a retention policy runs, given its drop_after
// interval (always "<n> seconds" from toInterval). Half the retention window,
// clamped to [5m, 12h]: frequent enough that a small raw_retention (e.g. 20m)
// reclaims chunks promptly instead of growing for a day, but never hammering
// the scheduler for large windows (e.g. 168h rollup → capped at 12h).
func scheduleFor(iv string) string {
	var secs int64
	fmt.Sscanf(iv, "%d seconds", &secs)
	half := secs / 2
	const minS, maxS = int64(300), int64(43200) // 5m .. 12h
	if half < minS {
		half = minS
	}
	if half > maxS {
		half = maxS
	}
	return fmt.Sprintf("%d seconds", half)
}

// toInterval converts a Go duration string ("20m", "24h") — plus an extra "Nd"
// day suffix ("7d") — into a Postgres INTERVAL literal expressed in whole
// seconds (e.g. "1200 seconds"). Returns "" for an empty input (policy
// disabled). The numeric, validated output is safe to interpolate into SQL.
func toInterval(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil
	}
	var d time.Duration
	if strings.HasSuffix(s, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err != nil {
			return "", fmt.Errorf("retention: invalid duration %q: %w", s, err)
		}
		d = time.Duration(days) * 24 * time.Hour
	} else {
		var err error
		if d, err = time.ParseDuration(s); err != nil {
			return "", fmt.Errorf("retention: invalid duration %q: %w", s, err)
		}
	}
	secs := int64(d.Seconds())
	if secs <= 0 {
		return "", fmt.Errorf("retention: non-positive duration %q", s)
	}
	return fmt.Sprintf("%d seconds", secs), nil
}
