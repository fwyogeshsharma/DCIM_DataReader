package store

// commands.go — store access for the downstream control plane (device_commands).
// The forwarder inserts pending commands derived from the Aggregator's
// ui_device_changes; the admin REST API serves them to EDR and records acks.
// See migrations/008_device_commands.sql.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// DeviceCommand is one pending/applied field write targeting a single device.
type DeviceCommand struct {
	ID        int64     `json:"id"`
	OrgID     string    `json:"org_id"`
	NetworkID string    `json:"network_id"`
	DeviceIP  string    `json:"device_ip"`
	Hostname  string    `json:"hostname"`
	Field     string    `json:"field"`
	Value     string    `json:"value"`
	Status    string    `json:"status"`
	Attempts  int       `json:"attempts"`
	LastError string    `json:"last_error"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// InsertCommands upserts a batch of pending commands. A new edit of the same
// (device_ip, field) that is still pending overwrites the old value rather than
// queuing a duplicate (see the partial unique index). Returns rows written.
func (db *DB) InsertCommands(ctx context.Context, cmds []DeviceCommand) (int, error) {
	if len(cmds) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, c := range cmds {
		if c.DeviceIP == "" || c.Field == "" {
			continue
		}
		batch.Queue(`
			INSERT INTO device_commands (org_id, network_id, device_ip, hostname, field, value)
			VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (device_ip, field) WHERE status = 'pending'
			DO UPDATE SET value = EXCLUDED.value,
			              hostname = EXCLUDED.hostname,
			              updated_at = now()`,
			c.OrgID, c.NetworkID, c.DeviceIP, c.Hostname, c.Field, c.Value)
	}
	if batch.Len() == 0 {
		return 0, nil
	}
	br := db.pool.SendBatch(ctx, batch)
	defer br.Close()
	n := 0
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// PendingCommands returns up to limit oldest pending commands (FIFO by id).
func (db *DB) PendingCommands(ctx context.Context, limit int) ([]DeviceCommand, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.pool.Query(ctx, `
		SELECT id, org_id, network_id, device_ip, hostname, field, value,
		       status, attempts, last_error, created_at, updated_at
		FROM device_commands
		WHERE status = 'pending'
		ORDER BY id ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DeviceCommand
	for rows.Next() {
		var c DeviceCommand
		if err := rows.Scan(&c.ID, &c.OrgID, &c.NetworkID, &c.DeviceIP, &c.Hostname,
			&c.Field, &c.Value, &c.Status, &c.Attempts, &c.LastError,
			&c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// MarkCommand records the outcome of an EDR apply attempt. status is
// "applied" (done) or "failed" (will stay pending for retry if requeue=true).
// On failure we keep the row pending and bump attempts so EDR retries next pull;
// on success we mark it applied so it drops out of the pending index.
func (db *DB) MarkCommand(ctx context.Context, id int64, applied bool, errMsg string) error {
	if applied {
		// Mark applied and recover the change details so we can log a config-change
		// event (which the forwarder pushes upstream to the Aggregator/UI).
		var deviceIP, field, value string
		err := db.pool.QueryRow(ctx, `
			UPDATE device_commands
			SET status = 'applied', last_error = '', attempts = attempts + 1, updated_at = now()
			WHERE id = $1
			RETURNING device_ip, field, value`, id).Scan(&deviceIP, &field, &value)
		if err != nil {
			return err
		}
		_ = db.insertConfigEvent(ctx, deviceIP, field, value) // best-effort
		return nil
	}
	// On failure keep it pending for retry — but go terminal ('failed') after
	// maxAttempts so an unapplyable command (bad field, unreachable device) can't
	// loop forever and storm EDR every poll.
	_, err := db.pool.Exec(ctx, `
		UPDATE device_commands
		SET attempts   = attempts + 1,
		    last_error = $2,
		    status     = CASE WHEN attempts + 1 >= $3 THEN 'failed' ELSE status END,
		    updated_at = now()
		WHERE id = $1`, id, truncErr(errMsg), maxCommandAttempts)
	return err
}

// maxCommandAttempts caps retries before a command is marked terminally failed.
const maxCommandAttempts = 5

// CommandsUpdatedSince returns commands whose status changed (applied/failed)
// after the cursor, oldest first — the up-feed the forwarder reports to the
// Aggregator so it can advance rule_changes pending → applied/failed.
func (db *DB) CommandsUpdatedSince(ctx context.Context, since time.Time, limit int) ([]DeviceCommand, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := db.pool.Query(ctx, `
		SELECT id, org_id, network_id, device_ip, hostname, field, value,
		       status, attempts, last_error, created_at, updated_at
		FROM device_commands
		WHERE status IN ('applied','failed')
		  AND updated_at > $1
		ORDER BY updated_at ASC
		LIMIT $2`, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DeviceCommand
	for rows.Next() {
		var c DeviceCommand
		if err := rows.Scan(&c.ID, &c.OrgID, &c.NetworkID, &c.DeviceIP, &c.Hostname,
			&c.Field, &c.Value, &c.Status, &c.Attempts, &c.LastError,
			&c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// insertConfigEvent records a "config.<field>" event for an applied UI edit so
// it shows in the events feed and forwards upstream. Resolves the device by its
// mgmt IP (device_ip may be CIDR — strip the mask). Best-effort: a missing
// device or insert error is non-fatal.
func (db *DB) insertConfigEvent(ctx context.Context, deviceIP, field, value string) error {
	ip := deviceIP
	if i := strings.IndexByte(ip, '/'); i >= 0 {
		ip = ip[:i]
	}
	var devID, hostname string
	_ = db.pool.QueryRow(ctx,
		`SELECT id::text, hostname FROM devices WHERE mgmt_ip = $1::inet LIMIT 1`, ip).
		Scan(&devID, &hostname)

	var did any
	if devID != "" {
		did = devID
	}
	payload := fmt.Sprintf(`{"field":%q,"value":%q}`, field, value)
	_, err := db.pool.Exec(ctx, `
		INSERT INTO events (device_id, source_hostname, ts, kind, event_name, severity, collector_agent, event_payload)
		VALUES ($1, $2, now(), 'event', $3, 'informational', 'EDR', $4::jsonb)`,
		did, hostname, configEventName(field, value), payload)
	if err == nil && db.onEvent != nil {
		db.onEvent() // wake the forwarder so the change reaches the UI promptly
	}
	return err
}

// configEventName builds a human-readable event name for an applied UI edit,
// e.g. "Country changed to India", "Power changed to GracefulShutdown".
func configEventName(field, value string) string {
	label := map[string]string{
		"country":         "Country",
		"datacenter_city": "City",
		"city":            "City",
		"datacenter":      "Datacenter",
		"room":            "Room",
		"floor":           "Floor",
		"rack_row":        "Rack row",
		"rack_num":        "Rack number",
		"rack_unit":       "Rack unit",
		"model":           "Model",
		"model_name":      "Model",
		"name":            "Name",
		"sys_contact":     "Contact",
		"sys_location":    "Location",
		"power_draw_w":    "Power draw",
		"power_action":    "Power",
		"power_state":     "Power",
		"indicator_led":   "Indicator LED",
	}
	name, ok := label[strings.ToLower(field)]
	if !ok {
		// Threshold rules (HighCPU, HighMemory, …) and anything else.
		name = field + " threshold"
	}
	if value == "" {
		return name + " changed"
	}
	return name + " changed to " + value
}

func truncErr(s string) string {
	const max = 512
	if len(s) > max {
		return s[:max]
	}
	return s
}
