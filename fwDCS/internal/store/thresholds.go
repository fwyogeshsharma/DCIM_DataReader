package store

// thresholds.go — persistent stores for two non-time-series values:
//   - device alert thresholds (device_thresholds table), and
//   - chassis power state (devices.power_state column).
// Both are written from EDR packets routed in the ingest worker; see
// migrations/009_power_state_thresholds.sql.

import (
	"context"
	"time"
)

// FwdThreshold is one per-device threshold resolved to the device's mgmt IP,
// destined for the Aggregator up-feed (so it can confirm + display thresholds).
type FwdThreshold struct {
	DeviceIP  string
	Hostname  string
	Rule      string
	Value     int
	UpdatedAt time.Time
}

// ThresholdsUpdatedSince returns thresholds changed after the cursor for one
// network, oldest first. Joined to devices for the mgmt IP match key.
func (db *DB) ThresholdsUpdatedSince(ctx context.Context, orgID, netID string, since time.Time, limit int) ([]FwdThreshold, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := db.pool.Query(ctx, `
		SELECT COALESCE(d.mgmt_ip::text,''), d.hostname, t.rule, t.value, t.updated_at
		FROM device_thresholds t
		JOIN devices d ON d.id = t.device_id
		WHERE d.org_id = $1 AND d.network_id = $2
		  AND t.updated_at > $3
		ORDER BY t.updated_at ASC
		LIMIT $4`, orgID, netID, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []FwdThreshold
	for rows.Next() {
		var t FwdThreshold
		if err := rows.Scan(&t.DeviceIP, &t.Hostname, &t.Rule, &t.Value, &t.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// UpsertThreshold stores one per-device alert threshold (e.g. HighCPU=85).
// Idempotent: re-fetching the same value is a no-op update.
func (db *DB) UpsertThreshold(ctx context.Context, deviceID, rule string, value int) error {
	// updated_at advances ONLY when the value actually changes — otherwise every
	// poll would churn the cursor and re-forward all thresholds every tick.
	_, err := db.pool.Exec(ctx, `
		INSERT INTO device_thresholds (device_id, rule, value, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (device_id, rule)
		DO UPDATE SET value = EXCLUDED.value,
		              updated_at = CASE WHEN device_thresholds.value IS DISTINCT FROM EXCLUDED.value
		                                THEN now() ELSE device_thresholds.updated_at END`,
		deviceID, rule, value)
	return err
}

// SetDevicePowerState writes the current chassis power state (1=On, 2=Off) to
// the device row so it persists independently of metric retention.
func (db *DB) SetDevicePowerState(ctx context.Context, deviceID string, state int) error {
	_, err := db.pool.Exec(ctx, `
		UPDATE devices SET power_state = $2 WHERE id = $1`,
		deviceID, state)
	return err
}
