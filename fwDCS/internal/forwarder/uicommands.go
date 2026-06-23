package forwarder

// uicommands.go — downstream control plane (UI → device).
//
// The Aggregator owns the user's edits (it serves the DCIM UI). It reports them
// back to DCS in the *response* to the ingest endpoint: a `ui_device_changes`
// array of {mgmt_ip, field, value} since a cursor we supply. We deliberately do
// NOT piggyback this on the telemetry push — that path fans out into many
// chunked per-scope POSTs, so attaching a cursor to "the" request is ambiguous.
// Instead we issue one dedicated, minimal pull per network per tick: a tiny
// POST to the same ingest endpoint carrying only the scope keys + cursor. Each
// returned change becomes a row in device_commands; EDR pulls and applies them.
//
// Cursor: forwarder_cursors name="ui_devices", keyed per (name, network_id),
// reusing the exact same mechanism as the telemetry cursors.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/faberwork/fwdcs/internal/store"
)

const (
	uiCursorName       = "ui_devices"    // down: edits pulled from Aggregator
	uiCmdStatusCursor  = "ui_cmd_status" // up: command results reported to Aggregator
	uiThresholdsCursor = "ui_thresholds" // up: threshold values reported to Aggregator
)

// uiCommandStatus reports the outcome of one applied/failed command up to the
// Aggregator so it can advance rule_changes pending → applied/failed.
type uiCommandStatus struct {
	DeviceIP string `json:"device_ip"`
	Field    string `json:"field"`
	Status   string `json:"status"`
	Error    string `json:"error,omitempty"`
}

// uiThresholdChange reports one current per-device threshold up so the
// Aggregator can confirm + display it.
type uiThresholdChange struct {
	DeviceIP string `json:"device_ip"`
	Hostname string `json:"hostname,omitempty"`
	Rule     string `json:"rule"`
	Value    int    `json:"value"`
}

// uiPullRequest is a minimal ingest body: the five scope keys the endpoint
// requires plus our cursor and the up-feeds. devices is empty — this call
// carries control-plane state both ways (status/thresholds up, edits down).
type uiPullRequest struct {
	OrgID            string              `json:"org_id"`
	DatacenterID     string              `json:"datacenter_id"`
	FloorID          string              `json:"floor_id"`
	NetworkID        string              `json:"network_id"`
	GroupID          string              `json:"group_id"`
	UIChangesSince   string              `json:"ui_changes_since,omitempty"`
	CommandStatus    []uiCommandStatus   `json:"command_status,omitempty"`
	ThresholdChanges []uiThresholdChange `json:"threshold_changes,omitempty"`
	Devices          []interface{}       `json:"devices"`
}

// uiDeviceChange is one field edit the user made in the UI. Matches the
// Aggregator ingest response: {mgmt_ip, field, new_value}.
type uiDeviceChange struct {
	MgmtIP   string `json:"mgmt_ip"`
	Field    string `json:"field"`
	NewValue string `json:"new_value"`
}

// uiPullResponse is the slice of the ingest response we care about here.
type uiPullResponse struct {
	Success         bool             `json:"success"`
	UIChanges       []uiDeviceChange `json:"ui_changes"`
	UIChangesCursor string           `json:"ui_changes_cursor"`
}

// pullUICommands fetches user edits for one network and queues them as device
// commands. Best-effort: a failure logs and returns nil so it never blocks the
// telemetry push; the cursor only advances on success, so nothing is lost.
func (f *Forwarder) pullUICommands(ctx context.Context, netID string) error {
	cursor, _ := f.db.GetForwarderCursor(ctx, uiCursorName, netID)

	// Default to epoch (not empty) on first contact so the Aggregator returns the
	// full pending backlog instead of just a fresh cursor — otherwise edits made
	// before our first pull are skipped forever.
	since := "1970-01-01T00:00:00Z"
	if !cursor.IsZero() {
		since = cursor.UTC().Format(time.RFC3339Nano)
	}

	// Up-feed 1: command results (applied/failed) since last report.
	cmdCursor, _ := f.db.GetForwarderCursor(ctx, uiCmdStatusCursor, netID)
	doneCmds, _ := f.db.CommandsUpdatedSince(ctx, cmdCursor, 200)
	cmdStatus := make([]uiCommandStatus, 0, len(doneCmds))
	var cmdMax time.Time
	for _, c := range doneCmds {
		cmdStatus = append(cmdStatus, uiCommandStatus{
			DeviceIP: c.DeviceIP, Field: c.Field, Status: c.Status, Error: c.LastError,
		})
		if c.UpdatedAt.After(cmdMax) {
			cmdMax = c.UpdatedAt
		}
	}

	// Up-feed 2: current per-device thresholds since last report.
	thrCursor, _ := f.db.GetForwarderCursor(ctx, uiThresholdsCursor, netID)
	thrs, _ := f.db.ThresholdsUpdatedSince(ctx, f.cfg.OrgID, netID, thrCursor, 500)
	thrChanges := make([]uiThresholdChange, 0, len(thrs))
	var thrMax time.Time
	for _, t := range thrs {
		thrChanges = append(thrChanges, uiThresholdChange{
			DeviceIP: t.DeviceIP, Hostname: t.Hostname, Rule: t.Rule, Value: t.Value,
		})
		if t.UpdatedAt.After(thrMax) {
			thrMax = t.UpdatedAt
		}
	}

	// The ingest endpoint rejects (400) any payload with an empty scope key. This
	// minimal pull doesn't carry devices, so dc/floor are irrelevant to the
	// down-feed query — but must be non-empty. Fall back to "unknown" when no
	// default is configured.
	dcID := f.cfg.DefaultDatacenterID
	if dcID == "" {
		dcID = "unknown"
	}
	floorID := f.cfg.DefaultFloorID
	if floorID == "" {
		floorID = "unknown"
	}

	reqBody := uiPullRequest{
		OrgID:            f.cfg.OrgID,
		DatacenterID:     dcID,
		FloorID:          floorID,
		NetworkID:        netID,
		GroupID:          f.cfg.GroupID,
		UIChangesSince:   since,
		CommandStatus:    cmdStatus,
		ThresholdChanges: thrChanges,
		Devices:          []interface{}{},
	}
	body, err := json.Marshal(&reqBody)
	if err != nil {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Ingest-Key", f.cfg.IngestKey)

	resp, err := f.client.Do(req)
	if err != nil {
		f.log.Warn("ui command pull: request failed", zap.String("network_id", netID), zap.Error(err))
		return nil
	}
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		f.log.Warn("ui command pull: non-2xx",
			zap.String("network_id", netID), zap.Int("status", resp.StatusCode))
		return nil
	}

	var pr uiPullResponse
	if err := json.Unmarshal(respBody, &pr); err != nil {
		f.log.Warn("ui command pull: decode failed", zap.String("network_id", netID), zap.Error(err))
		return nil
	}

	// Visibility: log every pull outcome (even zero) so the down-feed is observable.
	f.log.Info("ui command pull ok",
		zap.String("network_id", netID),
		zap.String("since", since),
		zap.Int("ui_changes", len(pr.UIChanges)),
		zap.String("cursor", pr.UIChangesCursor),
		zap.Int("cmd_status_sent", len(cmdStatus)),
		zap.Int("threshold_sent", len(thrChanges)))

	// The up-feeds were delivered with a 2xx — advance their cursors now,
	// independent of whether any edits came back down this cycle.
	if !cmdMax.IsZero() {
		_ = f.db.SetForwarderCursor(ctx, uiCmdStatusCursor, netID, cmdMax)
	}
	if !thrMax.IsZero() {
		_ = f.db.SetForwarderCursor(ctx, uiThresholdsCursor, netID, thrMax)
	}

	// Advance the down-cursor from the Aggregator's explicit cursor — ALWAYS,
	// even when no edits came back. This bootstraps the cursor on first contact
	// (the Aggregator returns rows only once we supply a since), so the next
	// cycle sends a real since and starts receiving edits.
	if c, perr := time.Parse(time.RFC3339Nano, pr.UIChangesCursor); perr == nil {
		_ = f.db.SetForwarderCursor(ctx, uiCursorName, netID, c)
	}

	if len(pr.UIChanges) == 0 {
		return nil
	}

	cmds := make([]store.DeviceCommand, 0, len(pr.UIChanges))
	for _, ch := range pr.UIChanges {
		if ch.MgmtIP == "" || ch.Field == "" {
			continue
		}
		cmds = append(cmds, store.DeviceCommand{
			OrgID:     f.cfg.OrgID,
			NetworkID: netID,
			DeviceIP:  ch.MgmtIP,
			Field:     ch.Field,
			Value:     ch.NewValue,
		})
	}

	n, err := f.db.InsertCommands(ctx, cmds)
	if err != nil {
		f.log.Warn("ui command pull: insert failed", zap.String("network_id", netID), zap.Error(err))
		return nil
	}

	f.log.Info("ui commands queued from aggregator",
		zap.String("network_id", netID),
		zap.Int("changes", len(pr.UIChanges)),
		zap.Int("queued", n))
	return nil
}
