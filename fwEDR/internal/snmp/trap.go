package snmp

import (
	"net"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"
	"go.uber.org/zap"

	"github.com/faberwork/fwedr/pkg/packet"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

// trapOIDToName maps standard and custom trap OIDs to event names.
var trapOIDToName = map[string]string{
	"1.3.6.1.6.3.1.1.5.1":   "coldStart",
	"1.3.6.1.6.3.1.1.5.2":   "warmStart",
	"1.3.6.1.6.3.1.1.5.3":   "linkDown",
	"1.3.6.1.6.3.1.1.5.4":   "linkUp",
	"1.3.6.1.6.3.1.1.5.5":   "authenticationFailure",
	"1.3.6.1.2.1.15.0.2":    "bgpSessionDown",
	"1.3.6.1.2.1.33.2.0.1":  "upsOnBattery",
	"1.3.6.1.2.1.33.2.0.2":  "upsLowBattery",
	"1.3.6.1.4.1.99999.1.1": "cpuHighUsage",
	"1.3.6.1.4.1.99999.1.2": "memoryHighUsage",
	"1.3.6.1.4.1.99999.1.3": "temperatureAlert",
	"1.3.6.1.4.1.99999.1.4": "linkFlap",
	"1.3.6.1.4.1.99999.1.5": "rackFailure",
	"1.3.6.1.4.1.99999.1.6": "humidityAlert",
	"1.3.6.1.4.1.99999.1.7": "dewPointAlert",
	"1.3.6.1.4.1.99999.1.8": "airflowAlert",
}

// trapOIDToSeverity maps trap OIDs to severity strings.
var trapOIDToSeverity = map[string]string{
	"1.3.6.1.6.3.1.1.5.1":   "informational",
	"1.3.6.1.6.3.1.1.5.2":   "informational",
	"1.3.6.1.6.3.1.1.5.3":   "major",
	"1.3.6.1.6.3.1.1.5.4":   "informational",
	"1.3.6.1.6.3.1.1.5.5":   "major",
	"1.3.6.1.2.1.15.0.2":    "critical",
	"1.3.6.1.2.1.33.2.0.1":  "critical",
	"1.3.6.1.2.1.33.2.0.2":  "critical",
	"1.3.6.1.4.1.99999.1.1": "major",
	"1.3.6.1.4.1.99999.1.2": "major",
	"1.3.6.1.4.1.99999.1.3": "critical",
	"1.3.6.1.4.1.99999.1.4": "critical",
	"1.3.6.1.4.1.99999.1.5": "critical",
	"1.3.6.1.4.1.99999.1.6": "major",
	"1.3.6.1.4.1.99999.1.7": "critical",
	"1.3.6.1.4.1.99999.1.8": "major",
}

// TrapReceiver listens for SNMP traps and emits TelemetryPackets.
type TrapReceiver struct {
	addr     string
	orgID    string
	dcID     string
	floorID  string
	netID    string
	grpID    string
	readerID string
	signer   *packet.Signer
	out      chan<- *v1.TelemetryPacket
	log      *zap.Logger
}

// NewTrapReceiver creates a TrapReceiver. out receives decoded trap packets.
func NewTrapReceiver(
	addr, orgID, dcID, floorID, netID, grpID, readerID string,
	signer *packet.Signer,
	out chan<- *v1.TelemetryPacket,
	log *zap.Logger,
) *TrapReceiver {
	return &TrapReceiver{
		addr:     addr,
		orgID:    orgID,
		dcID:     dcID,
		floorID:  floorID,
		netID:    netID,
		grpID:    grpID,
		readerID: readerID,
		signer:   signer,
		out:      out,
		log:      log,
	}
}

// Listen binds the UDP socket and blocks, sending decoded packets to out.
// Returns on error or when the connection is closed.
func (r *TrapReceiver) Listen() error {
	// Bind IPv4 explicitly for a wildcard/empty host. A bare ":162" resolves to
	// the IPv6 wildcard [::]:162, which on Windows does NOT receive IPv4 datagrams
	// — so traps sent to 127.0.0.1:162 (the simulator, and most real agents) were
	// silently dropped. Forcing udp4 + 0.0.0.0 makes the receiver catch them.
	network, listenAddr := "udp", r.addr
	if host, port, err := net.SplitHostPort(r.addr); err == nil && (host == "" || host == "0.0.0.0" || host == "*") {
		network, listenAddr = "udp4", "0.0.0.0:"+port
	}
	addr, err := net.ResolveUDPAddr(network, listenAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP(network, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	r.log.Info("snmp trap receiver listening", zap.String("addr", listenAddr), zap.String("network", network))

	buf := make([]byte, 65535)
	for {
		n, src, err := conn.ReadFromUDP(buf)
		if err != nil {
			return err
		}
		raw := make([]byte, n)
		copy(raw, buf[:n])
		r.log.Debug("trap datagram received", zap.String("src", src.String()), zap.Int("bytes", n))
		go r.handle(raw, src.IP.String())
	}
}

func (r *TrapReceiver) handle(raw []byte, srcIP string) {
	g := &gosnmp.GoSNMP{Version: gosnmp.Version2c}
	decoded, err := g.SnmpDecodePacket(raw)
	if err != nil {
		r.log.Warn("trap decode failed", zap.Error(err))
		return
	}

	trapOID := extractTrapOID(decoded)
	name := trapOIDToName[trapOID]
	if name == "" {
		name = "snmp.trap"
	}
	severity := trapOIDToSeverity[trapOID]
	if severity == "" {
		severity = "informational"
	}

	// Simulator convention: trap community string carries the originating
	// device's IP (DCS-tracked memory "community=IP for simulator routing").
	// Real production devices also commonly set community = a per-tenant
	// secret, so we accept both — DCS only treats the value as a device IP
	// when it parses as an IP literal. Fall back to UDP source IP otherwise.
	deviceIP := srcIP
	community := decoded.Community
	if community != "" && net.ParseIP(community) != nil {
		deviceIP = community
	}

	meta := map[string]string{
		"trap_oid":  trapOID,
		"source_ip": deviceIP, // logical device IP for DB events.source_ip
		"mgmt_ip":   deviceIP, // explicit hint so DCS resolves device_id by mgmt_ip
		"udp_src":   srcIP,    // raw UDP source for forensics
		"community": community,
		"source":    "SNMP",
	}
	// Include varbinds as meta
	for _, v := range decoded.Variables {
		oid := strings.TrimPrefix(v.Name, ".")
		if oid == "1.3.6.1.2.1.1.3.0" || oid == "1.3.6.1.6.3.1.1.4.1.0" {
			continue // skip sysUpTime and snmpTrapOID
		}
		meta["vb."+oid] = PDUString(v)
	}

	now := time.Now().UnixNano()
	id := packet.NewID()
	nonce := r.signer.NextNonce()
	// SourceId carries the logical device IP. DCS looks up the device by IP
	// against mgmt_ip / prod_ip / loopback_ip / oob_ip when the hostname
	// lookup fails. mgmt_ip is preferred (operator-facing identity).
	canonical := packet.CanonicalBytes(id, deviceIP, now, name, "", 0, nonce)
	sig := r.signer.Sign(canonical)

	r.out <- &v1.TelemetryPacket{
		Id:           id,
		OrgId:        r.orgID,
		DatacenterId: r.dcID,
		FloorId:      r.floorID,
		NetworkId:    r.netID,
		GroupId:      r.grpID,
		SourceType:   "device",
		SourceId:     deviceIP,
		ReaderId:     r.readerID,
		TimestampNs:  now,
		Name:         name,
		Kind:         "trap",
		Severity:     severity,
		Meta:         meta,
		Signature:    sig,
		Nonce:        nonce,
	}
}

// extractTrapOID finds snmpTrapOID.0 (1.3.6.1.6.3.1.1.4.1.0) in varbinds.
func extractTrapOID(pkt *gosnmp.SnmpPacket) string {
	for _, v := range pkt.Variables {
		oid := strings.TrimPrefix(v.Name, ".")
		if oid == "1.3.6.1.6.3.1.1.4.1.0" {
			if s, ok := v.Value.(string); ok {
				return strings.TrimPrefix(s, ".")
			}
			if b, ok := v.Value.([]byte); ok {
				return strings.TrimPrefix(string(b), ".")
			}
		}
	}
	return ""
}
