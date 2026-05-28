// Package gnmi subscribes to gNMI telemetry from routers and switches.
package gnmi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
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

// Subscriber maintains one gNMI connection per target.
type Subscriber struct {
	target *target.Target
	cfg    config.GNMIConfig
	base   basePacket
	signer *packet.Signer
	log    *zap.Logger
}

type basePacket struct {
	orgID, dcID, floorID, netID, grpID, readerID string
}

// NewSubscriber creates a Subscriber for one target.
func NewSubscriber(
	t *target.Target,
	cfg config.GNMIConfig,
	orgID, dcID, floorID, netID, grpID, readerID string,
	signer *packet.Signer,
	log *zap.Logger,
) *Subscriber {
	return &Subscriber{
		target: t,
		cfg:    cfg,
		base:   basePacket{orgID, dcID, floorID, netID, grpID, readerID},
		signer: signer,
		log:    log,
	}
}

// Subscribe opens a gNMI ONCE subscription and returns decoded metrics.
// Use ONCE (not STREAM) for one-shot polling fallback. Prefer RunStream for
// long-lived collection — it bypasses the SNMP simulator entirely.
func (s *Subscriber) Subscribe(ctx context.Context) ([]*v1.TelemetryPacket, error) {
	addr := fmt.Sprintf("%s:%d", s.target.GNMIIP, s.cfg.Port)
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

	paths := append(interfacePaths, systemPaths...)
	paths = append(paths, platformPaths...)

	subList := &gnmipb.SubscriptionList{
		Mode:         gnmipb.SubscriptionList_ONCE,
		Subscription: pathsToSubs(paths, gnmipb.SubscriptionMode_SAMPLE, 0),
	}
	if s.target.ProdIP != "" {
		subList.Prefix = &gnmipb.Path{Target: s.target.ProdIP}
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
	addr := fmt.Sprintf("%s:%d", s.target.GNMIIP, s.cfg.Port)
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

	subs := make([]*gnmipb.Subscription, 0, len(interfacePaths)+len(systemPaths)+len(platformPaths))
	// Counters → SAMPLE every sampleNs
	for _, p := range []string{
		"/interfaces/interface[name=*]/state/counters",
		"/system/state/uptime",
		"/system/cpus/cpu[index=ALL]/state",
		"/system/memory/state",
		"/components/component[name=*]/state/temperature",
	} {
		subs = append(subs, &gnmipb.Subscription{
			Path:           parsePath(p),
			Mode:           gnmipb.SubscriptionMode_SAMPLE,
			SampleInterval: sampleNs,
		})
	}
	// Status → ON_CHANGE for immediate link-state transitions
	for _, p := range []string{
		"/interfaces/interface[name=*]/state/oper-status",
		"/interfaces/interface[name=*]/state/admin-status",
	} {
		subs = append(subs, &gnmipb.Subscription{
			Path: parsePath(p),
			Mode: gnmipb.SubscriptionMode_ON_CHANGE,
		})
	}

	subList := &gnmipb.SubscriptionList{
		Mode:         gnmipb.SubscriptionList_STREAM,
		Subscription: subs,
		UpdatesOnly:  false,
	}
	if s.target.ProdIP != "" {
		subList.Prefix = &gnmipb.Path{Target: s.target.ProdIP}
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
	var pkts []*v1.TelemetryPacket
	ts := time.Unix(0, n.GetTimestamp()).UTC().UnixNano()
	if ts == 0 {
		ts = time.Now().UnixNano()
	}

	for _, upd := range n.GetUpdate() {
		pathStr := pathToString(upd.GetPath())
		val, ok := extractValue(upd.GetVal())
		if !ok {
			continue
		}
		name, tag := mapPath(pathStr)
		if name == "" {
			continue
		}
		meta := map[string]string{
			"hostname":           s.target.SourceID(),
			"mgmt_ip":            s.target.MgmtIP,
			"prod_ip":            s.target.ProdIP,
			"loopback_ip":        s.target.LoopbackIP,
			"device_type":        s.target.DeviceType,
			"vendor":             s.target.Vendor,
			"collector_agent":    "EDR",
			"collector_protocol": "GNMI",
			"gnmi_path":          pathStr,
		}
		for k, v := range s.target.Labels {
			meta[k] = v
		}

		id := packet.NewID()
		nonce := s.signer.NextNonce()
		canonical := packet.CanonicalBytes(id, s.target.SourceID(), ts, name, tag, val, nonce)
		sig := s.signer.Sign(canonical)

		pkts = append(pkts, &v1.TelemetryPacket{
			Id:           id,
			OrgId:        s.base.orgID,
			DatacenterId: s.base.dcID,
			FloorId:      s.base.floorID,
			NetworkId:    s.base.netID,
			GroupId:      s.base.grpID,
			SourceType:   "device",
			SourceId:     s.target.SourceID(),
			ReaderId:     s.base.readerID,
			TimestampNs:  ts,
			Name:         name,
			Tag:          tag,
			Value:        val,
			Meta:         meta,
			Kind:         "metric",
			Signature:    sig,
			Nonce:        nonce,
		})
	}
	return pkts
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
