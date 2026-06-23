// Package command implements EDR's side of the downstream control plane.
//
// Flow: the DCIM UI edits a device → the Aggregator records it → DCS turns it
// into a pending row in device_commands → EDR (here) pulls those rows from the
// DCS admin REST API, applies each to the device (SNMP SET for identity / asset
// / location fields, Redfish for power), and acks the outcome back to DCS.
//
// This package is read-from-DCS + write-to-device only; it never touches the
// telemetry/publish path. It is disabled unless command_apply.enabled is set.
package command

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/faberwork/fwedr/pkg/config"
)

// Command mirrors the DCS device_commands row served by GET /admin/commands.
type Command struct {
	ID        int64  `json:"id"`
	OrgID     string `json:"org_id"`
	NetworkID string `json:"network_id"`
	DeviceIP  string `json:"device_ip"`
	Hostname  string `json:"hostname"`
	Field     string `json:"field"`
	Value     string `json:"value"`
}

// dcsClient talks to the DCS admin REST command endpoints.
type dcsClient struct {
	base string // e.g. http://dcs-host:8080
	key  string
	hc   *http.Client
}

func newDCSClient(cfg config.CommandApplyConfig) *dcsClient {
	return &dcsClient{
		base: strings.TrimRight(cfg.DCSBaseURL, "/"),
		key:  cfg.CommandKey,
		hc:   &http.Client{Timeout: 15 * time.Second},
	}
}

type pullResponse struct {
	Commands []Command `json:"commands"`
}

// pull fetches up to limit pending commands.
func (c *dcsClient) pull(ctx context.Context, limit int) ([]Command, error) {
	url := fmt.Sprintf("%s/admin/commands?limit=%d", c.base, limit)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if c.key != "" {
		req.Header.Set("X-Command-Key", c.key)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, fmt.Errorf("pull: HTTP %d: %s", resp.StatusCode, string(body))
	}
	var pr pullResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, fmt.Errorf("pull: decode: %w", err)
	}
	return pr.Commands, nil
}

type ackBody struct {
	ID      int64  `json:"id"`
	Applied bool   `json:"applied"`
	Error   string `json:"error"`
}

// ack reports the outcome of applying one command.
func (c *dcsClient) ack(ctx context.Context, id int64, applied bool, errMsg string) error {
	body, _ := json.Marshal(ackBody{ID: id, Applied: applied, Error: errMsg})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/admin/commands/ack", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.key != "" {
		req.Header.Set("X-Command-Key", c.key)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ack: HTTP %d", resp.StatusCode)
	}
	return nil
}
