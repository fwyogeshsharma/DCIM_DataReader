package snmp

import (
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"

	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/config"
)

// Client wraps a gosnmp session for one target.
type Client struct {
	g   *gosnmp.GoSNMP
	cfg config.SNMPConfig
}

// ShardPort routes a device to one of N snmpsim responder ports by hashing its
// community (= device IP): base + hash%N. Used by BOTH the poller and discovery
// enrichment so a device's polls and walks hit the SAME responder and total load
// is spread evenly — no single responder becomes a hotspot. shards<=1 disables
// sharding → port 161 (original single-responder behavior).
func ShardPort(community string, shards, base int) uint16 {
	if shards <= 1 || base <= 0 {
		return 161
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(community))
	return uint16(base + int(h.Sum32()%uint32(shards)))
}

// NewClient creates and connects an SNMP session to the given target.
func NewClient(t *target.Target, cfg config.SNMPConfig) (*Client, error) {
	g := &gosnmp.GoSNMP{
		Target:             t.IP,
		Port:               ShardPort(t.Community, cfg.Shards, cfg.ShardBasePort),
		Transport:          "udp",
		Community:          t.Community,
		Timeout:            time.Duration(cfg.Timeout) * time.Millisecond,
		Retries:            cfg.Retries,
		ExponentialTimeout: false,
		MaxOids:            gosnmp.MaxOids,
	}

	switch t.SNMPVersion {
	case 3:
		g.Version = gosnmp.Version3
		g.SecurityModel = gosnmp.UserSecurityModel
		g.MsgFlags = gosnmp.AuthPriv
		priv, auth := protoPriv(cfg.V3.PrivProtocol), protoAuth(cfg.V3.AuthProtocol)
		g.SecurityParameters = &gosnmp.UsmSecurityParameters{
			UserName:                 cfg.V3.Username,
			AuthenticationProtocol:   auth,
			AuthenticationPassphrase: cfg.V3.AuthPassword,
			PrivacyProtocol:          priv,
			PrivacyPassphrase:        cfg.V3.PrivPassword,
		}
	default:
		g.Version = gosnmp.Version2c
	}

	if err := g.Connect(); err != nil {
		return nil, fmt.Errorf("snmp connect %s: %w", t.IP, err)
	}
	return &Client{g: g, cfg: cfg}, nil
}

// Close releases the underlying UDP socket.
func (c *Client) Close() { c.g.Conn.Close() }

// SetCommunity overrides the v2c community for subsequent Get/Walk calls. This
// lets ONE pooled session (keyed by the responder socket, e.g. 127.0.0.1:16100)
// serve many simulator devices — gosnmp sends the community in every PDU, so the
// caller swaps it per device under the session's serializing lock instead of
// opening a separate UDP socket per device (which exhausts the Windows socket
// pool → WSAENOBUFS).
func (c *Client) SetCommunity(community string) { c.g.Community = community }

// ProbeMgmt does one cheap sysName Get against ip:port (the simulator's SNMP SET
// management agent, typically 1161). That agent serves on its own UDP socket,
// independent of the shared snmpsim responder on 161, so it answers even when 161
// is wedged under heavy-walk load. A successful response means the device is alive
// and the 161 timeout was transient overload — not a real outage. Uses a short
// timeout and no retries so a FAST miss isn't slowed materially. Returns false on
// any connect/Get error. SIMULATOR-ONLY: real devices have no such agent, so the
// caller must gate this behind mgmt_port > 0 (off in production).
func ProbeMgmt(ip string, port uint16, community string, timeoutMs int) bool {
	if timeoutMs <= 0 {
		timeoutMs = 1000
	}
	g := &gosnmp.GoSNMP{
		Target:    ip,
		Port:      port,
		Transport: "udp",
		Community: community,
		Version:   gosnmp.Version2c,
		Timeout:   time.Duration(timeoutMs) * time.Millisecond,
		Retries:   0,
		MaxOids:   gosnmp.MaxOids,
	}
	if err := g.Connect(); err != nil {
		return false
	}
	defer g.Conn.Close()
	res, err := g.Get([]string{OIDSysName})
	if err != nil || res == nil || len(res.Variables) == 0 {
		return false
	}
	// Any well-formed varbind (even noSuchObject) proves the agent is responsive.
	return true
}

// MgmtSysName does one sysName (1.3.6.1.2.1.1.5.0) Get against the simulator's SET
// management agent (ip:port, typically 1161) and returns the value. That agent
// serves the LIVE in-memory device name, so a rename shows up instantly — unlike
// the main snmpsim responder on 161, whose .snmprec sysName is patched lazily
// (rename-time patch is conditional, and the periodic StateStore sync is sharded
// and lags minutes). Returns ("", false) on any error or a non-name response.
// SIMULATOR-ONLY: gated by mgmt_port > 0; real devices have no such agent.
func MgmtSysName(ip string, port uint16, community string, timeoutMs int) (string, bool) {
	if timeoutMs <= 0 {
		timeoutMs = 1000
	}
	g := &gosnmp.GoSNMP{
		Target:    ip,
		Port:      port,
		Transport: "udp",
		Community: community,
		Version:   gosnmp.Version2c,
		Timeout:   time.Duration(timeoutMs) * time.Millisecond,
		Retries:   0,
		MaxOids:   gosnmp.MaxOids,
	}
	if err := g.Connect(); err != nil {
		return "", false
	}
	defer g.Conn.Close()
	res, err := g.Get([]string{OIDSysName})
	if err != nil || res == nil || len(res.Variables) == 0 {
		return "", false
	}
	s := PDUString(res.Variables[0])
	switch s {
	case "", "noSuchObject", "noSuchInstance", "endOfMibView":
		return "", false
	}
	return s, true
}

// Get fetches scalar OIDs (appends .0 if needed).
func (c *Client) Get(oids []string) (*gosnmp.SnmpPacket, error) {
	return c.g.Get(oids)
}

// Walk performs a BulkWalk under the given OID prefix.
func (c *Client) Walk(oid string) ([]gosnmp.SnmpPDU, error) {
	var pdus []gosnmp.SnmpPDU
	err := c.g.BulkWalk(oid, func(pdu gosnmp.SnmpPDU) error {
		pdus = append(pdus, pdu)
		return nil
	})
	return pdus, err
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func protoAuth(s string) gosnmp.SnmpV3AuthProtocol {
	switch strings.ToUpper(s) {
	case "SHA224":
		return gosnmp.SHA224
	case "SHA256":
		return gosnmp.SHA256
	case "SHA384":
		return gosnmp.SHA384
	case "SHA512":
		return gosnmp.SHA512
	case "SHA":
		return gosnmp.SHA
	default:
		return gosnmp.MD5
	}
}

func protoPriv(s string) gosnmp.SnmpV3PrivProtocol {
	switch strings.ToUpper(s) {
	case "AES192":
		return gosnmp.AES192
	case "AES256":
		return gosnmp.AES256
	case "DES":
		return gosnmp.DES
	default:
		return gosnmp.AES
	}
}

// ToFloat64 converts a gosnmp PDU value to float64.
func ToFloat64(pdu gosnmp.SnmpPDU) float64 {
	switch v := pdu.Value.(type) {
	case uint:
		return float64(v)
	case uint32:
		return float64(v)
	case uint64:
		return float64(v)
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case float32:
		return float64(v)
	case float64:
		return v
	}
	return 0
}

// LastOIDComponent returns the last numeric component of a dotted OID string.
// e.g. "1.3.6.1.2.1.2.2.1.10.5" → "5"
func LastOIDComponent(oid string) string {
	i := strings.LastIndex(oid, ".")
	if i < 0 {
		return oid
	}
	return oid[i+1:]
}

// OIDColumn strips the last component, returning the column OID prefix.
// e.g. "1.3.6.1.2.1.2.2.1.10.5" → "1.3.6.1.2.1.2.2.1.10"
func OIDColumn(oid string) string {
	i := strings.LastIndex(oid, ".")
	if i < 0 {
		return oid
	}
	return oid[:i]
}
