// Package discovery probes subnets via SNMP to build a dynamic target list.
package discovery

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gosnmp/gosnmp"
	"go.uber.org/zap"

	"github.com/faberwork/fwedr/internal/snmp"
	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/config"
)

const (
	oidSysName        = "1.3.6.1.2.1.1.5.0"
	oidSysDescr       = "1.3.6.1.2.1.1.1.0"
	oidIpAdEntAddr    = "1.3.6.1.2.1.4.20.1.1" // ipAdEntAddr;    OID suffix is the IP address
	oidIpAdEntIfIndex = "1.3.6.1.2.1.4.20.1.2" // ipAdEntIfIndex; maps IP → ifIndex
)

// InterfaceAddress is one per-interface IP discovered during enrichment.
// EDR emits one TelemetryPacket of Kind "interface_address" per entry so the
// DCS pipeline can upsert interface_addresses without bespoke RPC plumbing.
type InterfaceAddress struct {
	IfIndex int
	Address string
	Family  string // "ipv4" | "ipv6"
}

type Sweeper struct {
	subnets   []string
	snmpAgent string // socket target; "127.0.0.1" for simulator, "" = direct device IP
	seedIP    string // first known device IP; used for readiness probe before sweep
	snmp      config.SNMPConfig
	gnmi      config.GNMIConfig
	log       *zap.Logger
}

// New creates a Sweeper.
func New(subnets []string, snmpAgent string, seedIP string, snmp config.SNMPConfig, gnmi config.GNMIConfig, log *zap.Logger) *Sweeper {
	return &Sweeper{subnets: subnets, snmpAgent: snmpAgent, seedIP: seedIP, snmp: snmp, gnmi: gnmi, log: log}
}

// waitForReady probes the seed device until SNMPSim responds, with a maximum
// wait of 60 seconds. Returns true if the simulator answered. If it returns
// false the caller should still run Sweep — it will probably find nothing,
// but the EDR process must not be blocked on a dead simulator. The periodic
// rediscovery loop in main.go retries every IntervalHours (or 60 s when no
// targets are known), so devices appear automatically once SNMPSim recovers.
func (s *Sweeper) waitForReady(ctx context.Context) bool {
	if s.seedIP == "" || s.snmpAgent == "" {
		return true
	}
	const maxWait = 60 * time.Second
	deadline := time.Now().Add(maxWait)
	s.log.Info("waiting for SNMPSim to become ready", zap.String("agent", s.snmpAgent), zap.String("seed", s.seedIP), zap.Duration("max_wait", maxWait))
	for {
		_, _, ok := s.snmpGet(s.snmpAgent, s.seedIP)
		if ok {
			s.log.Info("SNMPSim ready", zap.String("agent", s.snmpAgent))
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Second):
			}
			return true
		}
		if time.Now().After(deadline) {
			s.log.Warn("SNMPSim not responding within max_wait — EDR proceeds; rediscovery loop will retry",
				zap.String("agent", s.snmpAgent), zap.Duration("waited", maxWait))
			return false
		}
		s.log.Info("SNMPSim not ready — retrying in 15s", zap.String("agent", s.snmpAgent))
		select {
		case <-ctx.Done():
			return false
		case <-time.After(15 * time.Second):
		}
	}
}

// Sweep probes all host IPs in the configured subnets and returns live targets.
// If the seed device does not respond within waitForReady's timeout, Sweep
// returns an empty list without grinding every dead IP — the caller must run a
// periodic rediscovery loop to pick up devices when SNMPSim recovers.
func (s *Sweeper) Sweep(ctx context.Context) ([]*target.Target, error) {
	if !s.waitForReady(ctx) {
		return nil, nil
	}
	ips, err := expandSubnets(s.subnets)
	if err != nil {
		return nil, err
	}
	s.log.Info("discovery sweep started", zap.Int("ips", len(ips)), zap.String("snmp_agent", s.snmpAgent))

	// maxSweepConcurrency: SNMPSim handles single Gets fine at 10 parallel;
	// the wedge condition is sustained Walks, not Gets. Sweep does only Gets
	// plus one ipAdEntAddr walk per discovered device, so 10 is safe and ~3x
	// faster than the previous setting of 3.
	const maxSweepConcurrency = 10

	var (
		mu       sync.Mutex
		out      []*target.Target
		wg       sync.WaitGroup
		sem      = make(chan struct{}, maxSweepConcurrency)
		probed   atomic.Uint32
		progress = uint32(50) // log every N probes regardless of hit/miss
	)

	totalIPs := uint32(len(ips))
	for _, ip := range ips {
		select {
		case <-ctx.Done():
			wg.Wait()
			return out, nil
		default:
		}
		ip := ip
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			t := s.probe(ip)
			n := probed.Add(1)
			if t != nil {
				mu.Lock()
				out = append(out, t)
				count := len(out)
				mu.Unlock()
				s.log.Info("discovered device",
					zap.String("ip", ip),
					zap.String("hostname", t.Hostname),
					zap.String("type", t.DeviceType),
					zap.Bool("gnmi", t.Has(target.CapGNMI)))
				_ = count
			}
			if n%progress == 0 || n == totalIPs {
				mu.Lock()
				found := len(out)
				mu.Unlock()
				s.log.Info("sweep progress",
					zap.Uint32("probed", n),
					zap.Uint32("total", totalIPs),
					zap.Int("found", found))
			}
		}()
	}
	wg.Wait()
	s.log.Info("discovery sweep complete", zap.Int("found", len(out)))
	if len(out) == 0 && s.snmpAgent != "" {
		s.log.Warn("sweep found 0 devices — verify simulator is running and SNMPSim is listening",
			zap.String("snmp_agent", s.snmpAgent), zap.Strings("subnets", s.subnets))
	}
	return out, nil
}

func (s *Sweeper) probe(ip string) *target.Target {
	// Determine the SNMP socket target and community list.
	socketTarget := ip
	if s.snmpAgent != "" {
		socketTarget = s.snmpAgent
	}

	// Simulator mode (snmpAgent set): SNMPSim routes by community string →
	// <community>.snmprec. Only try IP-as-community.
	// Production mode: try IP-as-community first (some devices use it), then
	// global community.
	var communities []string
	if s.snmpAgent != "" {
		communities = []string{ip}
	} else {
		communities = []string{ip}
		if s.snmp.Community != "" && s.snmp.Community != ip {
			communities = append(communities, s.snmp.Community)
		}
	}

	var (
		comm     string
		hostname string
		descr    string
	)
	for _, c := range communities {
		h, d, ok := s.snmpGet(socketTarget, c)
		if ok {
			comm, hostname, descr = c, h, d
			break
		}
	}
	if comm == "" {
		return nil
	}

	// gNMI detection: per-device servers bind on the mgmt IP.
	gnmiEnabled := s.tcpReachable(ip, s.gnmi.Port)

	// ipAdEntAddr walk DEFERRED. Doing the walk here means every sweep does
	// ~404 BulkWalks back-to-back, which is the single biggest cause of the
	// SNMPSim wedge. The walk now runs in a paced background enrichment loop
	// in main.go (see EnrichTarget); sweep only does the two-OID sysName /
	// sysDescr Get, which is one packet per device.
	prodIP, oobIP := "", ""

	// target.IP is the address the SNMP poller connects to.
	// In simulator mode this is 127.0.0.1 (SNMPSim routes by community).
	// MgmtIP and GNMIIP always hold the real device IP.
	snmpIP := ip
	if s.snmpAgent != "" {
		snmpIP = s.snmpAgent
	}

	devType := inferDeviceType(descr, hostname)
	// For network devices the 10.x.x.x is typically the loopback used in IGP/iBGP.
	// For servers it's the data-plane production IP. We surface both meanings via
	// ProdIP (always set when we have one) and LoopbackIP (network devices only).
	loopbackIP := ""
	switch devType {
	case "router", "switch", "firewall", "load_balancer":
		loopbackIP = prodIP
	}

	t := &target.Target{
		IP:          snmpIP,
		MgmtIP:      ip,
		ProdIP:      prodIP,
		LoopbackIP:  loopbackIP,
		OOBIP:       oobIP,
		GNMIIP:      ip,
		Hostname:    hostname,
		DeviceType:  devType,
		Vendor:      inferVendor(descr),
		Community:   comm,
		SNMPVersion: s.snmp.Version,
		Caps:        target.CapSNMP,
	}
	if gnmiEnabled {
		t.Caps |= target.CapGNMI
	}
	return t
}

// EnrichTarget runs ipAdEntAddr + ipAdEntIfIndex walks against one target and
// populates its ProdIP / OOBIP / LoopbackIP from the result. Also returns
// the per-interface IP tuples so the caller can emit interface_address packets
// for the interface_addresses table.
//
// Used by the paced background enrichment loop in main.go so the sim never
// sees a burst of walks during sweep. Safe to call from any goroutine; only
// mutates the passed Target.
func (s *Sweeper) EnrichTarget(t *target.Target) []InterfaceAddress {
	if t == nil || t.Community == "" {
		return nil
	}
	socketTarget := t.MgmtIP
	if s.snmpAgent != "" {
		socketTarget = s.snmpAgent
	}
	var (
		addrPdus []gosnmp.SnmpPDU
		err      error
	)
	for attempt := 1; attempt <= 3; attempt++ {
		addrPdus, err = s.snmpWalk(socketTarget, t.Community, oidIpAdEntAddr)
		if err == nil {
			break
		}
		s.log.Warn("interface address enrichment walk failed",
			zap.String("target", t.SourceID()),
			zap.String("community", t.Community),
			zap.Int("attempt", attempt),
			zap.Error(err))
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if err != nil {
		return nil
	}
	addrPrefix := oidIpAdEntAddr + "."
	addresses := make([]string, 0, len(addrPdus))
	for _, p := range addrPdus {
		name := strings.TrimPrefix(p.Name, ".")
		if !strings.HasPrefix(name, addrPrefix) {
			continue
		}
		candidate := strings.TrimPrefix(name, addrPrefix)
		addresses = append(addresses, candidate)
	}

	// Walk ipAdEntIfIndex to map each IP → ifIndex. The IP appears as the OID
	// suffix; the PDU value is the ifIndex.
	var idxPdus []gosnmp.SnmpPDU
	for attempt := 1; attempt <= 3; attempt++ {
		idxPdus, err = s.snmpWalk(socketTarget, t.Community, oidIpAdEntIfIndex)
		if err == nil {
			break
		}
		s.log.Warn("interface index enrichment walk failed",
			zap.String("target", t.SourceID()),
			zap.String("community", t.Community),
			zap.Int("attempt", attempt),
			zap.Error(err))
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	ipToIdx := make(map[string]int, len(idxPdus))
	idxPrefix := oidIpAdEntIfIndex + "."
	for _, p := range idxPdus {
		name := strings.TrimPrefix(p.Name, ".")
		if !strings.HasPrefix(name, idxPrefix) {
			continue
		}
		ip := strings.TrimPrefix(name, idxPrefix)
		switch v := p.Value.(type) {
		case int:
			ipToIdx[ip] = v
		case uint:
			ipToIdx[ip] = int(v)
		case int64:
			ipToIdx[ip] = int(v)
		case uint64:
			ipToIdx[ip] = int(v)
		}
	}

	prodIP, oobIP := "", ""
	for _, candidate := range addresses {
		switch {
		case strings.HasPrefix(candidate, "10.") && prodIP == "":
			prodIP = candidate
		case strings.HasPrefix(candidate, "172.") && oobIP == "":
			oobIP = candidate
		}
	}
	t.ProdIP = prodIP
	t.OOBIP = oobIP
	switch t.DeviceType {
	case "router", "switch", "firewall", "load_balancer":
		t.LoopbackIP = prodIP
	}

	out := make([]InterfaceAddress, 0, len(addresses))
	for _, ip := range addresses {
		idx := ipToIdx[ip]
		if idx <= 0 {
			s.log.Debug("interface address skipped without ifIndex",
				zap.String("target", t.SourceID()),
				zap.String("address", ip))
			continue
		}
		out = append(out, InterfaceAddress{
			IfIndex: idx,
			Address: ip,
			Family:  "ipv4",
		})
	}
	return out
}

func (s *Sweeper) snmpWalk(socketTarget, community, oid string) ([]gosnmp.SnmpPDU, error) {
	timeout := time.Duration(s.snmp.Timeout) * time.Millisecond
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout*4)
	defer cancel()
	release, ok := snmp.AcquireSocket(ctx)
	if !ok {
		return nil, ctx.Err()
	}
	defer release()
	g := &gosnmp.GoSNMP{
		Target:    socketTarget,
		Port:      snmp.ShardPort(community, s.snmp.Shards, s.snmp.ShardBasePort),
		Transport: "udp",
		Community: community,
		Version:   gosnmp.Version2c,
		Timeout:   timeout,
		Retries:   0,
		MaxOids:   gosnmp.MaxOids,
	}
	if err := g.Connect(); err != nil {
		return nil, err
	}
	defer g.Conn.Close()
	return g.BulkWalkAll(oid)
}

func (s *Sweeper) snmpGet(socketTarget, community string) (hostname, descr string, ok bool) {
	timeout := time.Duration(s.snmp.Timeout) * time.Millisecond
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout*2)
	defer cancel()
	release, acquired := snmp.AcquireSocket(ctx)
	if !acquired {
		s.log.Debug("snmp get skipped waiting for socket slot", zap.String("target", socketTarget), zap.String("community", community), zap.Error(ctx.Err()))
		return "", "", false
	}
	defer release()
	g := &gosnmp.GoSNMP{
		Target:    socketTarget,
		Port:      snmp.ShardPort(community, s.snmp.Shards, s.snmp.ShardBasePort),
		Transport: "udp",
		Community: community,
		Version:   gosnmp.Version2c,
		Timeout:   timeout,
		// No retries during sweep: dead IPs vastly outnumber flaky-but-live
		// IPs, so 0-retry shortens dead-IP cost from 6s to 2s. Devices that
		// drop the first probe are picked up by the periodic rediscovery loop.
		Retries: 0,
		MaxOids: gosnmp.MaxOids,
	}
	if err := g.Connect(); err != nil {
		s.log.Debug("snmp connect failed", zap.String("target", socketTarget), zap.String("community", community), zap.Error(err))
		return "", "", false
	}
	defer g.Conn.Close()

	result, err := g.Get([]string{oidSysName, oidSysDescr})
	if err != nil {
		s.log.Debug("snmp get failed", zap.String("target", socketTarget), zap.String("community", community), zap.Error(err))
		return "", "", false
	}
	if result == nil {
		return "", "", false
	}
	for _, pdu := range result.Variables {
		if pdu.Type == gosnmp.NoSuchObject || pdu.Type == gosnmp.NoSuchInstance || pdu.Type == gosnmp.Null {
			continue
		}
		switch {
		case strings.HasSuffix(pdu.Name, oidSysName):
			hostname = pduString(pdu)
		case strings.HasSuffix(pdu.Name, oidSysDescr):
			descr = pduString(pdu)
		}
	}
	if hostname == "" && descr == "" {
		return "", "", false
	}
	if hostname == "" {
		hostname = socketTarget
	}
	return hostname, descr, true
}

// pduString extracts a string from an SNMP PDU.
// gosnmp returns OctetString values as []byte, not string.
func pduString(pdu gosnmp.SnmpPDU) string {
	switch v := pdu.Value.(type) {
	case []byte:
		return strings.TrimSpace(string(v))
	case string:
		return strings.TrimSpace(v)
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", v))
	}
}

func (s *Sweeper) tcpReachable(ip string, port int) bool {
	addr := net.JoinHostPort(ip, fmt.Sprintf("%d", port))
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// ─── subnet expansion ────────────────────────────────────────────────────────

func expandSubnets(cidrs []string) ([]string, error) {
	var ips []string
	for _, cidr := range cidrs {
		_, ipNet, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid cidr %q: %w", cidr, err)
		}
		for ip := cloneIP(ipNet.IP); ipNet.Contains(ip); incrementIP(ip) {
			last := len(ip) - 1
			if ip[last] == 0 || ip[last] == 255 {
				continue
			}
			ips = append(ips, ip.String())
		}
	}
	return ips, nil
}

func cloneIP(ip net.IP) net.IP {
	c := make(net.IP, len(ip))
	copy(c, ip)
	return c
}

func incrementIP(ip net.IP) {
	for i := len(ip) - 1; i >= 0; i-- {
		ip[i]++
		if ip[i] != 0 {
			break
		}
	}
}

// ─── device classification ───────────────────────────────────────────────────

func inferDeviceType(descr, hostname string) string {
	// Hostname patterns take priority — vendor sysDescr may not name the device class
	// (e.g. Cisco ASA sysDescr says "Adaptive Security Appliance", not "firewall").
	h := strings.ToLower(hostname)
	switch {
	case hostnameHas(h, "-fw", "fw-", "-asa", "asa-", "firewall"):
		return "firewall"
	case hostnameHas(h, "pdu", "fpdu"):
		return "pdu"
	}

	d := strings.ToLower(descr)
	switch {
	case strings.Contains(d, "router") || strings.Contains(d, "routing"):
		return "router"
	case strings.Contains(d, "switch") || strings.Contains(d, "nx-os"):
		return "switch"
	case strings.Contains(d, "firewall") || strings.Contains(d, "adaptive security"):
		return "firewall"
	case strings.Contains(d, "load balancer") || strings.Contains(d, "f5"):
		return "load_balancer"
	case strings.Contains(d, "ups"):
		return "ups"
	case strings.Contains(d, "pdu"):
		return "pdu"
	}
	// IOS / IOS XE sysDescr has no explicit type — classify by hostname pattern.
	if strings.Contains(d, "ios") {
		switch {
		case hostnameHas(h, "-sw", "sw-", "switch", "-sp", "spine", "-lf", "leaf", "-agg", "-core", "-dist"):
			return "switch"
		default:
			return "router"
		}
	}
	return "server"
}

func hostnameHas(h string, parts ...string) bool {
	for _, p := range parts {
		if strings.Contains(h, p) {
			return true
		}
	}
	return false
}

func inferVendor(descr string) string {
	d := strings.ToLower(descr)
	switch {
	case strings.Contains(d, "cisco"):
		return "cisco"
	case strings.Contains(d, "juniper"):
		return "juniper"
	case strings.Contains(d, "arista"):
		return "arista"
	case strings.Contains(d, "apc") || strings.Contains(d, "schneider"):
		return "apc"
	case strings.Contains(d, "eaton"):
		return "eaton"
	case strings.Contains(d, "raritan"):
		return "raritan"
	case strings.Contains(d, "vertiv") || strings.Contains(d, "liebert"):
		return "vertiv"
	case strings.Contains(d, "palo alto"):
		return "paloalto"
	case strings.Contains(d, "f5"):
		return "f5"
	default:
		return "generic"
	}
}
