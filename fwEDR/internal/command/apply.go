package command

// apply.go — turns a Command into an actual device write.
//
// Two channels, both already exposed by the devices (no simulator/device code
// change needed):
//   - SNMP SET (UDP, the SET agent): identity (sysContact/sysName/sysLocation),
//     asset/location (country, datacenter, floor, room, rack_*), model, power_draw.
//   - Redfish (HTTP): chassis power on/off/reset (servers only).
//
// Fields that cannot be written this way (vendor, delete, link-break, live
// metric values) are not mapped and are rejected as unsupported.

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"

	"github.com/faberwork/fwedr/internal/snmp"
	"github.com/faberwork/fwedr/pkg/config"
)

func insecureTLS() *tls.Config { return &tls.Config{InsecureSkipVerify: true} } //nolint:gosec

// oidSpec maps a UI field to its writable OID and SNMP type.
type oidSpec struct {
	oid   string
	isInt bool
}

// snmpFieldMap is the authoritative set of SNMP-SET-writable fields, per the
// simulator SNMP_ARCHITECTURE.md §2.2 (mgmt plane, UDP 1161). Both the canonical
// field name and common aliases map to the same OID.
var snmpFieldMap = map[string]oidSpec{
	// System identity — 1.3.6.1.2.1.1.x
	"sys_contact":  {"1.3.6.1.2.1.1.4.0", false},
	"contact":      {"1.3.6.1.2.1.1.4.0", false},
	"name":         {"1.3.6.1.2.1.1.5.0", false},
	"hostname":     {"1.3.6.1.2.1.1.5.0", false},
	"sys_location": {"1.3.6.1.2.1.1.6.0", false},
	"location":     {"1.3.6.1.2.1.1.6.0", false},
	// Asset / location — enterprise 1.3.6.1.4.1.99999.4.x
	"country":         {"1.3.6.1.4.1.99999.4.1.0", false},
	"datacenter_city": {"1.3.6.1.4.1.99999.4.2.0", false},
	"city":            {"1.3.6.1.4.1.99999.4.2.0", false},
	"datacenter":      {"1.3.6.1.4.1.99999.4.3.0", false},
	"floor":           {"1.3.6.1.4.1.99999.4.4.0", false},
	"room":            {"1.3.6.1.4.1.99999.4.5.0", false},
	"rack_row":        {"1.3.6.1.4.1.99999.4.6.0", true},
	"rack_num":        {"1.3.6.1.4.1.99999.4.7.0", true},
	"rack_unit":       {"1.3.6.1.4.1.99999.4.8.0", true},
	"model":           {"1.3.6.1.4.1.99999.4.9.0", false},
	"model_name":      {"1.3.6.1.4.1.99999.4.9.0", false},
	"power_draw_w":    {"1.3.6.1.4.1.99999.4.10.0", true},

	// Per-device alert thresholds — enterprise 1.3.6.1.4.1.99999.3.x (Integer),
	// the same OIDs the threshold poller reads. Keys are lowercase because Apply
	// lowercases the field name. Airflow values are served/written ×10.
	"highcpu":                   {"1.3.6.1.4.1.99999.3.1.0", true},
	"highcpusustained":          {"1.3.6.1.4.1.99999.3.2.0", true},
	"cpunormal":                 {"1.3.6.1.4.1.99999.3.3.0", true},
	"highmemory":                {"1.3.6.1.4.1.99999.3.4.0", true},
	"hightemperature":           {"1.3.6.1.4.1.99999.3.6.0", true},
	"rackfailuremin":            {"1.3.6.1.4.1.99999.3.9.0", true},
	"sensorambienttemphigh":     {"1.3.6.1.4.1.99999.3.10.0", true},
	"sensorambienttempcritical": {"1.3.6.1.4.1.99999.3.11.0", true},
	"sensorambienttempnormal":   {"1.3.6.1.4.1.99999.3.12.0", true},
	"sensorhighhumidity":        {"1.3.6.1.4.1.99999.3.13.0", true},
	"sensorcriticalhumidity":    {"1.3.6.1.4.1.99999.3.14.0", true},
	"sensorlowhumidity":         {"1.3.6.1.4.1.99999.3.15.0", true},
	"sensorhumiditynormallow":   {"1.3.6.1.4.1.99999.3.16.1.0", true},
	"sensorhumiditynormalhigh":  {"1.3.6.1.4.1.99999.3.16.2.0", true},
	"sensorhighdewpoint":        {"1.3.6.1.4.1.99999.3.17.0", true},
	"sensordewpointnormal":      {"1.3.6.1.4.1.99999.3.18.0", true},
	"sensorhighairflow":         {"1.3.6.1.4.1.99999.3.19.0", true},
	"sensorlowairflow":          {"1.3.6.1.4.1.99999.3.20.0", true},
	"sensorairflownormallow":    {"1.3.6.1.4.1.99999.3.21.1.0", true},
	"sensorairflownormalhigh":   {"1.3.6.1.4.1.99999.3.21.2.0", true},
}

// powerFields are routed to Redfish instead of SNMP.
var powerFields = map[string]bool{
	"power_state":  true,
	"power":        true,
	"power_action": true,
}

// Applier executes commands against devices.
type Applier struct {
	cfg     config.CommandApplyConfig
	redfish config.RedfishConfig
	hc      *http.Client // shared Redfish client — reused, not per-request (avoids conn leak)
}

// NewApplier builds an Applier from the command-apply + redfish config.
func NewApplier(cfg config.CommandApplyConfig, rf config.RedfishConfig) *Applier {
	to := rf.TimeoutMs
	if to <= 0 {
		to = 5000
	}
	tr := &http.Transport{MaxIdleConns: 8, MaxIdleConnsPerHost: 2, IdleConnTimeout: 60 * time.Second}
	if rf.TLSInsecure {
		tr.TLSClientConfig = insecureTLS()
	}
	return &Applier{
		cfg:     cfg,
		redfish: rf,
		hc:      &http.Client{Timeout: time.Duration(to) * time.Millisecond, Transport: tr},
	}
}

// Apply performs the single field write described by cmd. Returns an error
// (suitable for the ack message) on any failure; nil on success.
func (a *Applier) Apply(ctx context.Context, cmd Command) error {
	// device_ip may arrive in CIDR form ("192.168.0.200/32") from an INET column —
	// strip the mask so it's a usable SNMP/Redfish target + community.
	deviceIP := cmd.DeviceIP
	if i := strings.IndexByte(deviceIP, '/'); i >= 0 {
		deviceIP = deviceIP[:i]
	}
	deviceIP = strings.TrimSpace(deviceIP)
	if deviceIP == "" {
		return fmt.Errorf("empty device_ip")
	}
	field := strings.ToLower(strings.TrimSpace(cmd.Field))

	if powerFields[field] {
		return a.applyRedfishPower(ctx, deviceIP, cmd.Value)
	}
	if field == "indicator_led" || field == "led" {
		return a.applyRedfishLED(ctx, deviceIP, cmd.Value)
	}
	spec, ok := snmpFieldMap[field]
	if !ok {
		return fmt.Errorf("unsupported field %q (no SNMP/Redfish write path)", cmd.Field)
	}
	return a.applySNMPSet(ctx, deviceIP, spec, cmd.Value)
}

// applySNMPSet writes one OID via SNMPv2c SET to the device's mgmt plane.
func (a *Applier) applySNMPSet(ctx context.Context, deviceIP string, spec oidSpec, value string) error {
	host := a.cfg.SNMPSetAgent
	if host == "" {
		host = deviceIP // talk straight to the device when no agent host is set
	}
	community := a.cfg.SNMPSetCommunity
	if community == "" {
		community = deviceIP // simulator convention: community == device IP
	}

	// Share the process-wide SNMP socket cap (avoids Windows WSAENOBUFS / wedging
	// the main poller, which holds per-target session locks while waiting on it).
	release, ok := snmp.AcquireSocket(ctx)
	if !ok {
		return ctx.Err()
	}
	defer release()

	g := &gosnmp.GoSNMP{
		Target:    host,
		Port:      uint16(a.cfg.SNMPSetPort),
		Transport: "udp",
		Community: community,
		Version:   gosnmp.Version2c,
		Timeout:   time.Duration(a.cfg.SNMPTimeoutMs) * time.Millisecond,
		Retries:   a.cfg.SNMPRetries,
		MaxOids:   gosnmp.MaxOids,
	}
	if err := g.Connect(); err != nil {
		return fmt.Errorf("snmp connect %s:%d: %w", host, a.cfg.SNMPSetPort, err)
	}
	defer g.Conn.Close()

	var pdu gosnmp.SnmpPDU
	if spec.isInt {
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("field expects integer, got %q: %w", value, err)
		}
		pdu = gosnmp.SnmpPDU{Name: spec.oid, Type: gosnmp.Integer, Value: n}
	} else {
		pdu = gosnmp.SnmpPDU{Name: spec.oid, Type: gosnmp.OctetString, Value: value}
	}

	res, err := g.Set([]gosnmp.SnmpPDU{pdu})
	if err != nil {
		return fmt.Errorf("snmp set %s: %w", spec.oid, err)
	}
	if res != nil && res.Error != gosnmp.NoError {
		return fmt.Errorf("snmp set %s: agent error %v", spec.oid, res.Error)
	}
	return nil
}

// ── Redfish power ────────────────────────────────────────────────────────────

type redfishCollection struct {
	Members []struct {
		ODataID string `json:"@odata.id"`
	} `json:"Members"`
}

// applyRedfishPower maps a UI power value to a ComputerSystem.Reset ResetType
// and POSTs it to the server's first ComputerSystem.
func (a *Applier) applyRedfishPower(ctx context.Context, deviceIP, value string) error {
	resetType := normalizeResetType(value)
	if resetType == "" {
		return fmt.Errorf("unsupported power value %q", value)
	}

	scheme := "http"
	if a.redfish.TLSInsecure {
		scheme = "https"
	}
	port := a.redfish.Port
	if port <= 0 {
		port = 443
	}
	base := fmt.Sprintf("%s://%s:%d", scheme, deviceIP, port)
	auth := "Basic " + base64.StdEncoding.EncodeToString(
		[]byte(a.redfish.Username+":"+a.redfish.Password))

	// 1. Find the first ComputerSystem.
	var coll redfishCollection
	if err := a.redfishGet(ctx, base+"/redfish/v1/Systems", auth, &coll); err != nil {
		return fmt.Errorf("redfish systems: %w", err)
	}
	if len(coll.Members) == 0 {
		return fmt.Errorf("redfish: no ComputerSystem members")
	}
	sysPath := strings.TrimRight(coll.Members[0].ODataID, "/")

	// 2. POST the reset action.
	body, _ := json.Marshal(map[string]string{"ResetType": resetType})
	url := base + sysPath + "/Actions/ComputerSystem.Reset"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", auth)
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.hc.Do(req)
	if err != nil {
		return fmt.Errorf("redfish reset: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("redfish reset: HTTP %d", resp.StatusCode)
	}
	return nil
}

// applyRedfishLED sets the chassis identify LED via PATCH on the ComputerSystem.
// Per REDFISH_ARCHITECTURE.md §5: PATCH /redfish/v1/Systems/{id} with
// {"IndicatorLED": "Lit"|"Off"|"Blinking"}.
func (a *Applier) applyRedfishLED(ctx context.Context, deviceIP, value string) error {
	led := normalizeLED(value)
	if led == "" {
		return fmt.Errorf("unsupported indicator_led value %q", value)
	}
	base, auth, err := a.redfishBaseAuth(deviceIP)
	if err != nil {
		return err
	}
	var coll redfishCollection
	if err := a.redfishGet(ctx, base+"/redfish/v1/Systems", auth, &coll); err != nil {
		return fmt.Errorf("redfish systems: %w", err)
	}
	if len(coll.Members) == 0 {
		return fmt.Errorf("redfish: no ComputerSystem members")
	}
	sysPath := strings.TrimRight(coll.Members[0].ODataID, "/")

	body, _ := json.Marshal(map[string]string{"IndicatorLED": led})
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, base+sysPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", auth)
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.hc.Do(req)
	if err != nil {
		return fmt.Errorf("redfish led: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("redfish led: HTTP %d", resp.StatusCode)
	}
	return nil
}

// normalizeLED maps loose UI input to a valid Redfish IndicatorLED state.
func normalizeLED(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "on", "lit", "true", "1":
		return "Lit"
	case "off", "false", "0":
		return "Off"
	case "blink", "blinking":
		return "Blinking"
	default:
		switch v {
		case "Lit", "Off", "Blinking":
			return v
		}
		return ""
	}
}

// redfishBaseAuth builds the base URL + Basic auth header for a device.
func (a *Applier) redfishBaseAuth(deviceIP string) (string, string, error) {
	scheme := "http"
	if a.redfish.TLSInsecure {
		scheme = "https"
	}
	port := a.redfish.Port
	if port <= 0 {
		port = 443
	}
	base := fmt.Sprintf("%s://%s:%d", scheme, deviceIP, port)
	auth := "Basic " + base64.StdEncoding.EncodeToString(
		[]byte(a.redfish.Username+":"+a.redfish.Password))
	return base, auth, nil
}

func (a *Applier) redfishGet(ctx context.Context, url, auth string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", auth)
	req.Header.Set("Accept", "application/json")
	resp, err := a.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// normalizeResetType maps loose UI inputs to valid Redfish ResetType values.
func normalizeResetType(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "on", "poweron", "power_on", "forceon":
		return "On"
	case "off", "poweroff", "power_off", "forceoff", "shutdown", "gracefulshutdown":
		return "ForceOff"
	case "restart", "reboot", "reset", "powercycle", "forcerestart", "gracefulrestart":
		return "ForceRestart"
	default:
		// Pass through an already-valid ResetType verbatim.
		switch v {
		case "On", "ForceOn", "ForceOff", "GracefulShutdown",
			"GracefulRestart", "ForceRestart", "PowerCycle", "PushPowerButton":
			return v
		}
		return ""
	}
}
