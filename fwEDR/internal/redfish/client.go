// Package redfish polls server BMCs over Redfish (HTTP) to collect OS-level
// usage (CPU/mem/disk/network, via the simulator's Oem extension) and hardware
// health (temps, fans, PSU watts, chassis power state). It is the server analog
// of the gNMI collector for switches/routers, and replaces SNMP polling of
// server health. Pull model: one light GET pass per server per interval.
package redfish

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/faberwork/fwedr/pkg/config"
)

// Client is a thin Redfish HTTP client shared across all server BMCs. The
// underlying http.Client keeps connections alive (bounded per host) so repeated
// polls reuse sockets — low memory, no per-poll dial churn.
type Client struct {
	hc      *http.Client
	scheme  string // "http" (sim/default) or "https" (tls_insecure)
	port    int
	authHdr string // precomputed HTTP Basic header
}

// NewClient builds the shared client from config.
func NewClient(cfg config.RedfishConfig) *Client {
	scheme := "http"
	tr := &http.Transport{MaxIdleConns: 64, MaxIdleConnsPerHost: 2}
	if cfg.TLSInsecure {
		scheme = "https"
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // sim/self-signed BMCs
	}
	auth := base64.StdEncoding.EncodeToString([]byte(cfg.Username + ":" + cfg.Password))
	return &Client{
		hc:      &http.Client{Timeout: time.Duration(cfg.TimeoutMs) * time.Millisecond, Transport: tr},
		scheme:  scheme,
		port:    cfg.Port,
		authHdr: "Basic " + auth,
	}
}

func (c *Client) url(ip, path string) string {
	return fmt.Sprintf("%s://%s:%d%s", c.scheme, ip, c.port, path)
}

// get performs an authenticated GET and decodes the JSON body into out.
func (c *Client) get(ctx context.Context, ip, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url(ip, path), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", c.authHdr)
	req.Header.Set("Accept", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("redfish GET %s: %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ─── resource shapes (only the fields EDR consumes) ──────────────────────────

type odataRef struct {
	ID string `json:"@odata.id"`
}

type collection struct {
	Members []odataRef `json:"Members"`
}

type serviceRoot struct {
	Systems  odataRef `json:"Systems"`
	Chassis  odataRef `json:"Chassis"`
	Managers odataRef `json:"Managers"`
}

// The ComputerSystem document is read generically (map[string]any) so the
// OS-usage field locations come from the Redfish profile (see profile.go) rather
// than fixed struct tags. PowerState + the Oem.Simulator fields are extracted by
// dotted path. Thermal/Power/Manager below stay typed — they are standard Redfish
// resources, portable to real BMCs as-is.

type thermal struct {
	Temperatures []struct {
		Name           string   `json:"Name"`
		ReadingCelsius *float64 `json:"ReadingCelsius"`
	} `json:"Temperatures"`
	Fans []struct {
		Name    string   `json:"Name"`
		Reading *float64 `json:"Reading"`
	} `json:"Fans"`
}

type power struct {
	PowerControl []struct {
		PowerConsumedWatts *float64 `json:"PowerConsumedWatts"`
	} `json:"PowerControl"`
	PowerSupplies []struct {
		Name                 string   `json:"Name"`
		LastPowerOutputWatts *float64 `json:"LastPowerOutputWatts"`
	} `json:"PowerSupplies"`
}

type managerDoc struct {
	FirmwareVersion string `json:"FirmwareVersion"`
	Model           string `json:"Model"`
	Manufacturer    string `json:"Manufacturer"`
}

// discover resolves the System, Chassis and Manager resource paths for one BMC
// by reading the service root and the first member of each collection (the ids
// are device-derived, not constant, so they must be discovered, not assumed).
func (c *Client) discover(ctx context.Context, ip string) (sysPath, chassisPath, mgrPath string, err error) {
	var root serviceRoot
	if err = c.get(ctx, ip, "/redfish/v1/", &root); err != nil {
		return "", "", "", err
	}
	sysPath, err = c.firstMember(ctx, ip, root.Systems.ID)
	if err != nil {
		return "", "", "", err
	}
	// Chassis/Managers are best-effort: a missing one just drops those metrics.
	chassisPath, _ = c.firstMember(ctx, ip, root.Chassis.ID)
	mgrPath, _ = c.firstMember(ctx, ip, root.Managers.ID)
	return sysPath, chassisPath, mgrPath, nil
}

func (c *Client) firstMember(ctx context.Context, ip, collPath string) (string, error) {
	if collPath == "" {
		return "", fmt.Errorf("redfish: empty collection ref")
	}
	var coll collection
	if err := c.get(ctx, ip, collPath, &coll); err != nil {
		return "", err
	}
	if len(coll.Members) == 0 {
		return "", fmt.Errorf("redfish: empty collection %s", collPath)
	}
	return coll.Members[0].ID, nil
}
