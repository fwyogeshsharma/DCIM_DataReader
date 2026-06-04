package bacnet

import (
	"context"
	"net"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/config"
	"github.com/faberwork/fwedr/pkg/packet"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

const defaultCircuits = 42

var modelCircuitRe = regexp.MustCompile(`EV2-(\d+)`)

type basePacket struct {
	orgID, dcID, floorID, netID, grpID, readerID string
}

// binding pairs a BACnet target with its resolved address, object set, and
// packet routing keys.
type binding struct {
	t     *target.Target
	base  basePacket
	addr  *net.UDPAddr
	conn  *net.UDPConn // connected request/response socket (lazy)
	objs  []objMeta
	index map[[2]int]objMeta
}

// Manager polls Verdigris EV2 energy monitors via BACnet/IP — periodic
// ReadPropertyMultiple plus optional SubscribeCOV push notifications.
type Manager struct {
	cfg      config.BACnetConfig
	identity config.IdentityConfig
	signer   *packet.Signer
	log      *zap.Logger

	bindings []*binding
	byIP     map[string]*binding // COV demux: notification src IP → binding
	invoke   uint32
}

// NewManager builds a Manager over the BACnet-capable targets.
func NewManager(
	targets []*target.Target,
	cfg config.BACnetConfig,
	identity config.IdentityConfig,
	signer *packet.Signer,
	log *zap.Logger,
) *Manager {
	m := &Manager{
		cfg:      cfg,
		identity: identity,
		signer:   signer,
		log:      log,
		byIP:     make(map[string]*binding),
	}
	for _, t := range targets {
		if !t.Has(target.CapBACnet) {
			continue
		}
		ip := t.MgmtIP
		if ip == "" {
			ip = t.IP
		}
		addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(ip, strconv.Itoa(cfg.Port)))
		if err != nil {
			log.Warn("bacnet: bad target address", zap.String("ip", ip), zap.Error(err))
			continue
		}
		circuits := circuitsFor(t.ModelName)
		objs := objectsFor(circuits, cfg.ReadCircuits)
		b := &binding{
			t:     t,
			base:  m.baseFor(t),
			addr:  addr,
			objs:  objs,
			index: metaIndex(objs),
		}
		m.bindings = append(m.bindings, b)
		m.byIP[ip] = b
	}
	return m
}

// Count returns the number of BACnet-capable devices.
func (m *Manager) Count() int { return len(m.bindings) }

func circuitsFor(model string) int {
	if mm := modelCircuitRe.FindStringSubmatch(model); mm != nil {
		if n, err := strconv.Atoi(mm[1]); err == nil && n > 0 {
			return n
		}
	}
	return defaultCircuits
}

func (m *Manager) baseFor(t *target.Target) basePacket {
	dc := m.identity.DatacenterID
	if t.DatacenterID != "" {
		dc = t.DatacenterID
	}
	fl := m.identity.FloorID
	if t.FloorID != "" {
		fl = t.FloorID
	}
	return basePacket{
		orgID:    m.identity.OrgID,
		dcID:     dc,
		floorID:  fl,
		netID:    m.identity.NetworkID,
		grpID:    m.identity.GroupID,
		readerID: m.identity.ReaderID,
	}
}

func (m *Manager) nextInvoke() int { return int(atomic.AddUint32(&m.invoke, 1) & 0xFF) }

// Run starts the BACnet poll loop (and COV listener when enabled). Blocks until
// ctx is cancelled.
func (m *Manager) Run(ctx context.Context, out chan<- *v1.TelemetryPacket) {
	if len(m.bindings) == 0 {
		return
	}
	m.log.Info("bacnet: starting",
		zap.Int("devices", len(m.bindings)),
		zap.Bool("read_circuits", m.cfg.ReadCircuits),
		zap.Bool("use_cov", m.cfg.UseCOV),
		zap.Int("poll_interval_ms", m.cfg.PollIntervalMs))

	if m.cfg.UseCOV {
		go m.runCOV(ctx, out)
	}

	opt := readOpts{
		timeout:     time.Duration(m.cfg.TimeoutMs) * time.Millisecond,
		retries:     m.cfg.Retries,
		objsPerRead: m.cfg.ObjectsPerRead,
	}
	interval := time.Duration(m.cfg.PollIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	m.pollAll(ctx, out, opt) // immediate first pass
	for {
		select {
		case <-ctx.Done():
			m.closeConns()
			return
		case <-ticker.C:
			m.pollAll(ctx, out, opt)
		}
	}
}

func (m *Manager) pollAll(ctx context.Context, out chan<- *v1.TelemetryPacket, opt readOpts) {
	ts := time.Now().UnixNano()
	for _, b := range m.bindings {
		if ctx.Err() != nil {
			return
		}
		if b.conn == nil {
			conn, err := net.DialUDP("udp", nil, b.addr)
			if err != nil {
				m.log.Debug("bacnet: dial failed", zap.String("host", b.t.SourceID()), zap.Error(err))
				continue
			}
			b.conn = conn
		}
		results := readObjects(b.conn, b.objs, opt, m.nextInvoke)
		if len(results) == 0 {
			m.log.Debug("bacnet: no data", zap.String("host", b.t.SourceID()))
			continue
		}
		for _, r := range results {
			meta, ok := b.index[[2]int{r.ObjType, r.Instance}]
			if !ok || !r.OK {
				continue
			}
			m.emit(ctx, out, b, meta, r.Value, ts)
		}
	}
}

func (m *Manager) closeConns() {
	for _, b := range m.bindings {
		if b.conn != nil {
			b.conn.Close()
			b.conn = nil
		}
	}
}

// ─── COV (push notifications) ─────────────────────────────────────────────────

// runCOV subscribes panel + alarm objects on every device and listens for
// UnconfirmedCOVNotifications on a shared socket, demultiplexing by source IP.
// Circuits stay poll-only — subscribing every circuit object would be thousands
// of subscriptions for marginal benefit. Alarms especially benefit from push.
func (m *Manager) runCOV(ctx context.Context, out chan<- *v1.TelemetryPacket) {
	sock, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		m.log.Warn("bacnet: COV socket bind failed — push disabled", zap.Error(err))
		return
	}
	defer sock.Close()

	subscribe := func() {
		pid := 1
		for _, b := range m.bindings {
			for _, o := range panelObjects {
				req := buildSubscribeCOV(m.nextInvoke(), pid, o.objType, o.inst, m.cfg.COVLifetimeSec)
				_, _ = sock.WriteToUDP(req, b.addr)
				pid++
			}
		}
	}
	subscribe()

	// Renew before the subscription lifetime expires.
	renew := time.NewTicker(time.Duration(m.cfg.COVLifetimeSec)*time.Second/2 + time.Second)
	defer renew.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-renew.C:
				subscribe()
			}
		}
	}()

	buf := make([]byte, 1500)
	for {
		if ctx.Err() != nil {
			return
		}
		_ = sock.SetReadDeadline(time.Now().Add(time.Second))
		n, src, err := sock.ReadFromUDP(buf)
		if err != nil {
			continue // deadline tick — re-check ctx
		}
		pdu := parseFrame(buf[:n])
		if pdu == nil || pdu.kind != pduUnconfirmedRequest || pdu.service != svcUnconfirmedCOVNotify {
			continue
		}
		b := m.byIP[src.IP.String()]
		if b == nil {
			continue
		}
		cov, ok := decodeCOVNotification(pdu.data)
		if !ok {
			continue
		}
		meta, ok := b.index[[2]int{cov.objType, cov.instance}]
		if !ok {
			continue
		}
		m.emit(ctx, out, b, meta, cov.presentValue, time.Now().UnixNano())
	}
}

// ─── emission ─────────────────────────────────────────────────────────────────

func (m *Manager) emit(ctx context.Context, out chan<- *v1.TelemetryPacket,
	b *binding, meta objMeta, val float64, ts int64) {
	scale := meta.scale
	if scale == 0 {
		scale = 1
	}
	pkt := m.newMetric(b, meta.name, meta.tag, val/scale, ts)
	select {
	case out <- pkt:
	case <-ctx.Done():
	}
}

func (m *Manager) newMetric(b *binding, name, tag string, val float64, ts int64) *v1.TelemetryPacket {
	t := b.t
	mta := map[string]string{
		"hostname":           t.SourceID(),
		"mgmt_ip":            t.MgmtIP,
		"device_type":        t.DeviceType,
		"vendor":             t.Vendor,
		"model_name":         t.ModelName,
		"country":            t.Country,
		"datacenter":         t.DatacenterName,
		"datacenter_city":    t.DatacenterCity,
		"room":               t.Room,
		"collector_agent":    "EDR",
		"collector_protocol": "BACNET",
	}
	for k, v := range t.Labels {
		mta[k] = v
	}
	id := packet.NewID()
	nonce := m.signer.NextNonce()
	canonical := packet.CanonicalBytes(id, t.SourceID(), ts, name, tag, val, nonce)
	sig := m.signer.Sign(canonical)
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
		Meta:         mta,
		Kind:         "energy", // routed to the dedicated energy_metrics table by DCS
		Signature:    sig,
		Nonce:        nonce,
	}
}
