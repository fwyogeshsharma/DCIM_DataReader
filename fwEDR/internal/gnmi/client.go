// Package gnmi subscribes to gNMI telemetry from routers and switches.
package gnmi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/config"
	"github.com/faberwork/fwedr/pkg/packet"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

// gNMI paths to subscribe for routers and switches.
var interfacePaths = []string{
	"/interfaces/interface[name=*]/state/counters",
	"/interfaces/interface[name=*]/state/oper-status",
	"/interfaces/interface[name=*]/state/admin-status",
}

var systemPaths = []string{
	"/system/state/uptime",
	"/system/cpus/cpu[index=ALL]/state",
	"/system/memory/state",
}

var platformPaths = []string{
	"/components/component[name=*]/state/temperature",
}

// Subscriber maintains one gNMI subscription. In single mode it subscribes to one
// device (used for the direct :57400 fallback). In aggregated mode it holds ONE
// subscription to the proxy with an empty prefix.target — the proxy streams every
// device in that single connection — and resolves each notification to its device
// via the prefix.target carried on the wire.
type Subscriber struct {
	target *target.Target // single mode: the one device
	base   basePacket     // single mode: that device's routing keys
	cfg    config.GNMIConfig
	signer *packet.Signer
	log    *zap.Logger

	aggregated bool                           // true → one subscription for all devices
	resolve    func(ip string) *deviceBinding // aggregated mode: prefix.target → device
}

type basePacket struct {
	orgID, dcID, floorID, netID, grpID, readerID string
}

// deviceBinding pairs a device with its packet routing keys, resolved per
// notification in aggregated mode.
type deviceBinding struct {
	t    *target.Target
	base basePacket
}

// NewSubscriber creates a single-device Subscriber (direct mode / fallback).
func NewSubscriber(
	t *target.Target,
	cfg config.GNMIConfig,
	orgID, dcID, floorID, netID, grpID, readerID string,
	signer *packet.Signer,
	log *zap.Logger,
) *Subscriber {
	return &Subscriber{
		target: t,
		base:   basePacket{orgID, dcID, floorID, netID, grpID, readerID},
		cfg:    cfg,
		signer: signer,
		log:    log,
	}
}

// NewAggregator creates a Subscriber that holds ONE subscription to the proxy and
// fans every device's telemetry out of that single stream. resolve maps a
// notification's prefix.target (device mgmt/prod IP) to the device + routing keys.
func NewAggregator(
	cfg config.GNMIConfig,
	resolve func(ip string) *deviceBinding,
	signer *packet.Signer,
	log *zap.Logger,
) *Subscriber {
	return &Subscriber{
		cfg:        cfg,
		signer:     signer,
		log:        log,
		aggregated: true,
		resolve:    resolve,
	}
}

// binding resolves the device for a notification. Aggregated mode looks up by the
// wire prefix.target; single mode always returns its one device.
func (s *Subscriber) binding(ip string) *deviceBinding {
	if s.aggregated {
		if s.resolve == nil {
			return nil
		}
		return s.resolve(ip)
	}
	return &deviceBinding{t: s.target, base: s.base}
}

// Subscribe opens a gNMI ONCE subscription and returns decoded metrics.
// Use ONCE (not STREAM) for one-shot polling fallback. Prefer RunStream for
// long-lived collection — it bypasses the SNMP simulator entirely.
func (s *Subscriber) Subscribe(ctx context.Context) ([]*v1.TelemetryPacket, error) {
	addr := s.dialAddr()
	conn, err := s.dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := gnmipb.NewGNMIClient(conn)

	if s.cfg.Username != "" {
		md := metadata.Pairs("username", s.cfg.Username, "password", s.cfg.Password)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	// Root subscription — see RunStream for why per-leaf wildcard paths return
	// nothing against the simulator's gNMI proxy.
	subList := &gnmipb.SubscriptionList{
		Mode:         gnmipb.SubscriptionList_ONCE,
		Subscription: []*gnmipb.Subscription{{Path: &gnmipb.Path{}}},
	}
	// Aggregated mode sends NO prefix.target → the proxy streams every device in
	// this one subscription. Single mode pins the subscription to its device.
	if !s.aggregated {
		if tgt := s.gnmiTarget(); tgt != "" {
			subList.Prefix = &gnmipb.Path{Target: tgt}
		}
	}
	req := &gnmipb.SubscribeRequest{
		Request: &gnmipb.SubscribeRequest_Subscribe{
			Subscribe: subList,
		},
	}

	stream, err := client.Subscribe(ctx)
	if err != nil {
		return nil, fmt.Errorf("gnmi subscribe %s: %w", addr, err)
	}
	if err := stream.Send(req); err != nil {
		return nil, fmt.Errorf("gnmi send %s: %w", addr, err)
	}

	var pkts []*v1.TelemetryPacket
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		update, ok := resp.Response.(*gnmipb.SubscribeResponse_Update)
		if !ok {
			break // SyncResponse marks end of ONCE
		}
		pkts = append(pkts, s.decodeNotification(update.Update)...)
	}
	return pkts, nil
}

// RunStream opens a long-lived gNMI STREAM subscription. Counters use SAMPLE
// mode with a configured interval (defaults to gnmi.PollIntervalMs); admin /
// oper status use ON_CHANGE so transitions arrive immediately. Decoded packets
// are written to out until ctx is cancelled or the stream errors. On any error
// the function returns nil if ctx is done, otherwise the error — the caller
// reconnects with backoff.
func (s *Subscriber) RunStream(ctx context.Context, out chan<- *v1.TelemetryPacket) error {
	addr := s.dialAddr()
	conn, err := s.dial(addr)
	if err != nil {
		return fmt.Errorf("gnmi dial %s: %w", addr, err)
	}
	defer conn.Close()

	client := gnmipb.NewGNMIClient(conn)

	if s.cfg.Username != "" {
		md := metadata.Pairs("username", s.cfg.Username, "password", s.cfg.Password)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	sampleNs := uint64(s.cfg.PollInterval) * uint64(time.Millisecond)
	if sampleNs == 0 {
		sampleNs = 30 * uint64(time.Second)
	}

	// Subscribe to the device ROOT (empty path). The simulator's gNMI proxy
	// returns the whole OpenConfig device tree as a single JSON-IETF subtree and
	// does NOT resolve wildcard list keys ([name=*]) — a per-leaf wildcard path
	// matches nothing and yields zero updates. decodeNotification walks the root
	// tree and extracts interface counters/status, system CPU/memory/uptime and
	// component temperatures. In STREAM mode the server re-pushes the full
	// snapshot every sample interval (it does not honor per-sub ON_CHANGE), so
	// status changes arrive on the next sample.
	subList := &gnmipb.SubscriptionList{
		Mode: gnmipb.SubscriptionList_STREAM,
		Subscription: []*gnmipb.Subscription{{
			Path:           &gnmipb.Path{}, // root
			Mode:           gnmipb.SubscriptionMode_SAMPLE,
			SampleInterval: sampleNs,
		}},
		UpdatesOnly: false,
	}
	// Aggregated mode sends NO prefix.target → the proxy streams every device in
	// this one subscription. Single mode pins the subscription to its device.
	if !s.aggregated {
		if tgt := s.gnmiTarget(); tgt != "" {
			subList.Prefix = &gnmipb.Path{Target: tgt}
		}
	}

	stream, err := client.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("gnmi stream open %s: %w", addr, err)
	}
	if err := stream.Send(&gnmipb.SubscribeRequest{
		Request: &gnmipb.SubscribeRequest_Subscribe{Subscribe: subList},
	}); err != nil {
		return fmt.Errorf("gnmi stream send %s: %w", addr, err)
	}

	// Graceful close: when ctx is cancelled (EDR shutdown), send a half-close
	// so the server-side coroutine resolves cleanly instead of being abandoned
	// mid-read. This matters for the SNMPSim Python asyncio backend, which
	// leaks tasks across EDR restarts otherwise.
	closeOnce := context.AfterFunc(ctx, func() {
		_ = stream.CloseSend()
	})
	defer closeOnce()

	for {
		resp, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("gnmi stream recv %s: %w", addr, err)
		}
		switch r := resp.Response.(type) {
		case *gnmipb.SubscribeResponse_Update:
			for _, pkt := range s.decodeNotification(r.Update) {
				select {
				case out <- pkt:
				case <-ctx.Done():
					return nil
				}
			}
		case *gnmipb.SubscribeResponse_SyncResponse:
			// Initial snapshot complete — stream continues with live updates.
		}
	}
}

// ─── decoding ────────────────────────────────────────────────────────────────

func (s *Subscriber) decodeNotification(n *gnmipb.Notification) []*v1.TelemetryPacket {
	ts := time.Unix(0, n.GetTimestamp()).UTC().UnixNano()
	if ts <= 0 {
		ts = time.Now().UnixNano()
	}

	// Resolve which device this notification belongs to. Aggregated mode keys on
	// the wire prefix.target; single mode ignores it and uses its one device.
	b := s.binding(n.GetPrefix().GetTarget())
	if b == nil || b.t == nil {
		return nil // unknown device behind the proxy — skip
	}

	var pkts []*v1.TelemetryPacket
	for _, upd := range n.GetUpdate() {
		// Simulator (and any JSON-IETF target) delivers a JSON subtree per update —
		// not a scalar leaf. Walk the OpenConfig tree and emit one packet per leaf.
		if raw := jsonBytesOf(upd.GetVal()); len(raw) > 0 {
			var root map[string]any
			if err := json.Unmarshal(raw, &root); err == nil {
				pkts = append(pkts, s.emitTree(b, root, ts)...)
				continue
			}
			// not a JSON object — fall through to scalar handling
		}
		// Scalar fallback: real OpenConfig targets that encode leaf values directly.
		pathStr := pathToString(upd.GetPath())
		val, ok := extractValue(upd.GetVal())
		if !ok {
			continue
		}
		name, tag := mapPath(pathStr)
		if name == "" {
			continue
		}
		pkts = append(pkts, s.newPkt(b, name, tag, val, ts, pathStr))
	}
	return pkts
}

// emitTree walks an OpenConfig device tree (the JSON-IETF subtree the simulator
// returns for a root subscription) and emits a metric packet per recognised leaf:
// interface counters + admin/oper status, system CPU/memory/uptime, and component
// temperatures. Unknown subtrees (lldp, network-instances) are ignored.
func (s *Subscriber) emitTree(b *deviceBinding, root map[string]any, ts int64) []*v1.TelemetryPacket {
	var out []*v1.TelemetryPacket
	add := func(name, tag string, v float64) {
		out = append(out, s.newPkt(b, name, tag, v, ts, name))
	}

	// ── interfaces ──
	if arr, ok := dig(root, "openconfig-interfaces:interfaces", "interface").([]any); ok {
		for _, it := range arr {
			m, _ := it.(map[string]any)
			if m == nil {
				continue
			}
			name, _ := m["name"].(string)
			st, _ := m["state"].(map[string]any)
			if name == "" || st == nil {
				continue
			}
			if c, ok := st["counters"].(map[string]any); ok {
				counters := [...]struct {
					key, metric string
				}{
					{"in-octets", "interface.bytes_received_hc"},
					{"out-octets", "interface.bytes_sent_hc"},
					{"in-unicast-pkts", "interface.packets_received_unicast"},
					{"out-unicast-pkts", "interface.packets_sent_unicast"},
					{"in-errors", "interface.errors_received"},
					{"out-errors", "interface.errors_sent"},
					{"in-discards", "interface.discards_received"},
					{"out-discards", "interface.discards_sent"},
				}
				for _, cc := range counters {
					if v, ok := num(c[cc.key]); ok {
						add(cc.metric, name, v)
					}
				}
			}
			if v, ok := statusVal(st["oper-status"]); ok {
				add("interface.operational_status", name, v)
			}
			if v, ok := statusVal(st["admin-status"]); ok {
				add("interface.admin_status", name, v)
			}
		}
	}

	// ── system ──
	if sys, ok := dig(root, "openconfig-system:system").(map[string]any); ok {
		if st, ok := sys["state"].(map[string]any); ok {
			// The simulator's gNMI uptime leaf is already centiseconds (matches the
			// SNMP sysUpTime value for the same device); emit as-is. Previously this
			// multiplied by 100 (assuming seconds), making gNMI uptime 100× the SNMP
			// value for the same device.
			if v, ok := num(st["uptime"]); ok {
				add("system.uptime_centiseconds", "", v)
			}
		}
		if mem, ok := dig(sys, "memory", "state").(map[string]any); ok {
			total, hasTotal := num(mem["physical"])
			if hasTotal {
				add("system.memory_total_bytes", "", total)
			}
			if free, ok := num(mem["free"]); ok && hasTotal {
				add("system.memory_used_bytes", "", total-free)
			}
			if v, ok := num(mem["utilized"]); ok {
				add("system.memory_utilization_percent", "", v)
			}
		}
		if cpus, ok := dig(sys, "cpus", "cpu").([]any); ok {
			for _, it := range cpus {
				m, _ := it.(map[string]any)
				if m == nil {
					continue
				}
				idx := fmt.Sprintf("%v", m["index"])
				if tot, ok := dig(m, "state", "total").(map[string]any); ok {
					if v, ok := num(tot["instant"]); ok {
						add("system.cpu_utilization_percent", idx, v)
					}
				}
			}
		}
	}

	// ── component temperatures ──
	if arr, ok := dig(root, "openconfig-platform:components", "component").([]any); ok {
		for _, it := range arr {
			m, _ := it.(map[string]any)
			if m == nil {
				continue
			}
			name, _ := m["name"].(string)
			if name == "" {
				continue
			}
			if tmp, ok := dig(m, "state", "temperature").(map[string]any); ok {
				if v, ok := num(tmp["instant"]); ok {
					add("environment.temperature_c", name, v)
				}
				// Alarm state carried in the telemetry tree — surfaced through the
				// proxy as a 0/1 metric so threshold breaches are visible in near
				// real time without a separate trap channel.
				if a, present := tmp["alarm-status"]; present {
					add("environment.temperature_alarm", name, boolVal(a))
				}
			}
		}
	}

	return out
}

// newPkt builds one signed gNMI metric packet for the resolved device.
func (s *Subscriber) newPkt(b *deviceBinding, name, tag string, val float64, ts int64, gnmiPath string) *v1.TelemetryPacket {
	t := b.t
	meta := map[string]string{
		"hostname":           t.SourceID(),
		"mgmt_ip":            t.MgmtIP,
		"prod_ip":            t.ProdIP,
		"loopback_ip":        t.LoopbackIP,
		"device_type":        t.DeviceType,
		"vendor":             t.Vendor,
		"collector_agent":    "EDR",
		"collector_protocol": "GNMI",
		"gnmi_path":          gnmiPath,
	}
	for k, v := range t.Labels {
		meta[k] = v
	}
	id := packet.NewID()
	nonce := s.signer.NextNonce()
	canonical := packet.CanonicalBytes(id, t.SourceID(), ts, name, tag, val, nonce)
	sig := s.signer.Sign(canonical)
	return &v1.TelemetryPacket{
		Id:           id,
		OrgId:        b.base.orgID,
		DatacenterId: b.base.dcID,
		FloorId:      b.base.floorID,
		NetworkId:    b.base.netID,
		GroupId:      b.base.grpID,
		SourceType:   "device",
		SourceId:     t.SourceID(),
		ReaderId:     b.base.readerID,
		TimestampNs:  ts,
		Name:         name,
		Tag:          tag,
		Value:        val,
		Meta:         meta,
		Kind:         "metric",
		Signature:    sig,
		Nonce:        nonce,
	}
}

// boolVal coerces an OpenConfig boolean (bool or "true"/"false") to 1/0.
func boolVal(v any) float64 {
	switch x := v.(type) {
	case bool:
		if x {
			return 1
		}
	case string:
		if strings.EqualFold(strings.TrimSpace(x), "true") {
			return 1
		}
	}
	return 0
}

// dialAddr returns the gNMI endpoint to connect to: the shared proxy when
// configured (simulator), otherwise the per-device gNMI server.
func (s *Subscriber) dialAddr() string {
	if s.cfg.ProxyAddr != "" {
		return s.cfg.ProxyAddr
	}
	if s.target != nil {
		return fmt.Sprintf("%s:%d", s.target.GNMIIP, s.cfg.Port)
	}
	return ""
}

// gnmiTarget is the subscription prefix.target used to select this device behind
// the proxy. The simulator keys gNMI data by mgmt IP.
func (s *Subscriber) gnmiTarget() string {
	switch {
	case s.target.MgmtIP != "":
		return s.target.MgmtIP
	case s.target.ProdIP != "":
		return s.target.ProdIP
	default:
		return s.target.GNMIIP
	}
}

// jsonBytesOf returns the JSON payload from a gNMI TypedValue regardless of which
// encoding field carries it. The simulator's gNMI stack encodes its JSON-IETF
// value into the field the OpenConfig Go proto decodes as proto_bytes (a gNMI
// proto-version skew), so we accept json_ietf_val, json_val, and proto_bytes.
func jsonBytesOf(tv *gnmipb.TypedValue) []byte {
	if tv == nil {
		return nil
	}
	if b := tv.GetJsonIetfVal(); len(b) > 0 {
		return b
	}
	if b := tv.GetJsonVal(); len(b) > 0 {
		return b
	}
	if b := tv.GetProtoBytes(); len(b) > 0 {
		return b
	}
	return nil
}

// dig walks nested map[string]any by successive keys, returning the value at the
// end of the chain or nil if any step is missing or not an object.
func dig(m map[string]any, keys ...string) any {
	var cur any = m
	for _, k := range keys {
		cm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = cm[k]
	}
	return cur
}

// num coerces a JSON-IETF value (float64 or quoted-number string — gNMI encodes
// 64-bit counters as strings) to float64.
func num(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

// statusVal maps an OpenConfig admin/oper-status string to 1 (up) or 0 (down).
func statusVal(v any) (float64, bool) {
	str, ok := v.(string)
	if !ok {
		return 0, false
	}
	switch strings.ToUpper(strings.TrimSpace(str)) {
	case "UP", "ENABLED":
		return 1, true
	case "DOWN", "DISABLED":
		return 0, true
	}
	return 0, false
}

// mapPath converts a gNMI path string to (metricName, tag).
// tag is the interface name, CPU index, or component name.
func mapPath(p string) (name, tag string) {
	p = strings.ToLower(p)
	switch {
	// Interface counters (gNMI delivers 64-bit by default → "_hc" variant)
	case strings.Contains(p, "counters/in-octets"):
		return "interface.bytes_received_hc", ifaceTag(p)
	case strings.Contains(p, "counters/out-octets"):
		return "interface.bytes_sent_hc", ifaceTag(p)
	case strings.Contains(p, "counters/in-unicast-pkts"):
		return "interface.packets_received_unicast", ifaceTag(p)
	case strings.Contains(p, "counters/out-unicast-pkts"):
		return "interface.packets_sent_unicast", ifaceTag(p)
	case strings.Contains(p, "counters/in-errors"):
		return "interface.errors_received", ifaceTag(p)
	case strings.Contains(p, "counters/out-errors"):
		return "interface.errors_sent", ifaceTag(p)
	case strings.Contains(p, "counters/in-discards"):
		return "interface.discards_received", ifaceTag(p)
	case strings.Contains(p, "counters/out-discards"):
		return "interface.discards_sent", ifaceTag(p)
	case strings.Contains(p, "state/oper-status"):
		return "interface.operational_status", ifaceTag(p)
	case strings.Contains(p, "state/admin-status"):
		return "interface.admin_status", ifaceTag(p)
	// System
	case strings.Contains(p, "system/state/uptime"):
		return "system.uptime_centiseconds", ""
	case strings.Contains(p, "cpus/cpu") && strings.Contains(p, "total/instant"):
		return "system.cpu_utilization_percent", cpuTag(p)
	case strings.Contains(p, "memory/state/utilized"):
		return "system.memory_used_bytes", ""
	case strings.Contains(p, "memory/state/physical"):
		return "system.memory_total_bytes", ""
	// Platform temperature
	case strings.Contains(p, "temperature/instant"):
		return "environment.temperature_c", componentTag(p)
	}
	return "", ""
}

func ifaceTag(p string) string {
	return extractBracketKey(p, "interface[name=")
}

func cpuTag(p string) string {
	return extractBracketKey(p, "cpu[index=")
}

func componentTag(p string) string {
	return extractBracketKey(p, "component[name=")
}

func extractBracketKey(p, prefix string) string {
	i := strings.Index(p, prefix)
	if i < 0 {
		return ""
	}
	rest := p[i+len(prefix):]
	j := strings.Index(rest, "]")
	if j < 0 {
		return rest
	}
	return rest[:j]
}

func extractValue(tv *gnmipb.TypedValue) (float64, bool) {
	if tv == nil {
		return 0, false
	}
	switch v := tv.Value.(type) {
	case *gnmipb.TypedValue_UintVal:
		return float64(v.UintVal), true
	case *gnmipb.TypedValue_IntVal:
		return float64(v.IntVal), true
	case *gnmipb.TypedValue_FloatVal:
		return float64(v.FloatVal), true
	case *gnmipb.TypedValue_DecimalVal:
		if v.DecimalVal != nil {
			d := float64(v.DecimalVal.Digits)
			for i := uint32(0); i < v.DecimalVal.Precision; i++ {
				d /= 10
			}
			return d, true
		}
	case *gnmipb.TypedValue_StringVal:
		// oper-status / admin-status
		switch strings.ToLower(v.StringVal) {
		case "up", "enabled":
			return 1, true
		case "down", "disabled":
			return 0, true
		}
	}
	return 0, false
}

func pathToString(p *gnmipb.Path) string {
	if p == nil {
		return ""
	}
	var parts []string
	for _, e := range p.GetElem() {
		part := e.GetName()
		for k, v := range e.GetKey() {
			part += fmt.Sprintf("[%s=%s]", k, v)
		}
		parts = append(parts, part)
	}
	return "/" + strings.Join(parts, "/")
}

func pathsToSubs(paths []string, mode gnmipb.SubscriptionMode, sampleIntervalNs uint64) []*gnmipb.Subscription {
	subs := make([]*gnmipb.Subscription, 0, len(paths))
	for _, p := range paths {
		subs = append(subs, &gnmipb.Subscription{
			Path:           parsePath(p),
			Mode:           mode,
			SampleInterval: sampleIntervalNs,
		})
	}
	return subs
}

func parsePath(p string) *gnmipb.Path {
	p = strings.TrimPrefix(p, "/")
	parts := strings.Split(p, "/")
	elems := make([]*gnmipb.PathElem, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		if !strings.Contains(part, "[") {
			elems = append(elems, &gnmipb.PathElem{Name: part})
			continue
		}
		name := part[:strings.Index(part, "[")]
		keys := make(map[string]string)
		rest := part[strings.Index(part, "[")+1 : strings.LastIndex(part, "]")]
		for _, kv := range strings.Split(rest, ",") {
			eq := strings.Index(kv, "=")
			if eq >= 0 {
				keys[kv[:eq]] = kv[eq+1:]
			}
		}
		elems = append(elems, &gnmipb.PathElem{Name: name, Key: keys})
	}
	return &gnmipb.Path{Elem: elems}
}

// ─── dial ────────────────────────────────────────────────────────────────────

func (s *Subscriber) dial(addr string) (*grpc.ClientConn, error) {
	var cred credentials.TransportCredentials
	if s.cfg.TLS.Insecure {
		cred = insecure.NewCredentials()
	} else {
		tc, err := buildTLS(s.cfg.TLS)
		if err != nil {
			return nil, err
		}
		cred = credentials.NewTLS(tc)
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(cred))
}

func buildTLS(cfg config.TLSConfig) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS13}
	if cfg.CAFile != "" {
		ca, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("gnmi tls ca: %w", err)
		}
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(ca)
		tc.RootCAs = pool
	}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("gnmi tls cert: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}
