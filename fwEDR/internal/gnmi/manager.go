package gnmi

import (
	"context"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/config"
	"github.com/faberwork/fwedr/pkg/packet"
	v1 "github.com/faberwork/fwedr/proto/v1"
)

// Manager runs the gNMI collection strategy:
//
//   - Preferred: ONE aggregated subscription to the proxy (cfg.ProxyAddr). A
//     single connection with an empty prefix.target streams telemetry, status and
//     alarm state for ALL devices — no per-device gNMI connections.
//   - Fallback: when the proxy is unreachable, open direct per-device
//     subscriptions on cfg.FallbackPort as a temporary measure.
//   - Recovery: while in fallback, poll the proxy; once it answers, tear the
//     direct connections down and return to the single proxy subscription.
//
// One Manager replaces the previous one-goroutine-per-device gNMI model.
type Manager struct {
	targets  []*target.Target
	cfg      config.GNMIConfig
	identity config.IdentityConfig
	signer   *packet.Signer
	log      *zap.Logger

	resolver map[string]*deviceBinding // device IP (mgmt/prod/gnmi) → binding
	count    int                       // gNMI-capable device count
}

// NewManager builds a Manager and its prefix.target → device resolver.
func NewManager(
	targets []*target.Target,
	cfg config.GNMIConfig,
	identity config.IdentityConfig,
	signer *packet.Signer,
	log *zap.Logger,
) *Manager {
	m := &Manager{
		targets:  targets,
		cfg:      cfg,
		identity: identity,
		signer:   signer,
		log:      log,
		resolver: make(map[string]*deviceBinding, len(targets)*2),
	}
	for _, t := range targets {
		if !t.Has(target.CapGNMI) {
			continue
		}
		m.count++
		b := &deviceBinding{t: t, base: m.baseFor(t)}
		// The proxy may key a notification by any of the device's IPs; index all.
		for _, ip := range []string{t.MgmtIP, t.ProdIP, t.GNMIIP} {
			if ip != "" {
				m.resolver[ip] = b
			}
		}
	}
	return m
}

// baseFor builds the packet routing keys for a device, honoring per-device
// datacenter/floor overrides from the topology over the global identity.
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

func (m *Manager) resolve(ip string) *deviceBinding { return m.resolver[ip] }

// Run blocks until ctx is cancelled, alternating between the proxy subscription
// and the direct fallback as proxy availability changes.
func (m *Manager) Run(ctx context.Context, out chan<- *v1.TelemetryPacket) {
	if m.count == 0 {
		return
	}
	probe := time.Duration(m.cfg.ProxyProbeIntervalMs) * time.Millisecond
	if probe <= 0 {
		probe = 5 * time.Second
	}

	for {
		if ctx.Err() != nil {
			return
		}
		switch {
		case m.cfg.ProxyAddr != "" && proxyUp(m.cfg.ProxyAddr):
			m.log.Info("gnmi: single aggregated subscription via proxy",
				zap.String("proxy", m.cfg.ProxyAddr), zap.Int("devices", m.count))
			err := m.runProxy(ctx, out)
			if ctx.Err() != nil {
				return
			}
			m.log.Warn("gnmi: proxy subscription ended — re-evaluating", zap.Error(err))
		default:
			if m.cfg.ProxyAddr != "" {
				m.log.Warn("gnmi: proxy unavailable — temporary direct fallback",
					zap.Int("fallback_port", m.cfg.FallbackPort), zap.Int("devices", m.count))
			} else {
				m.log.Info("gnmi: no proxy configured — direct per-device mode",
					zap.Int("port", m.cfg.FallbackPort), zap.Int("devices", m.count))
			}
			m.runFallback(ctx, out, probe)
			if ctx.Err() != nil {
				return
			}
			if m.cfg.ProxyAddr != "" {
				m.log.Info("gnmi: proxy recovered — fallback terminated, returning to proxy")
			}
		}
		// Brief backoff before re-attempting the proxy so a flapping proxy doesn't
		// spin the loop.
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// runProxy holds ONE aggregated subscription to the proxy until it errors or ctx
// cancels. All devices arrive on this single stream.
func (m *Manager) runProxy(ctx context.Context, out chan<- *v1.TelemetryPacket) error {
	agg := NewAggregator(m.cfg, m.resolve, m.signer, m.log)
	return agg.RunStream(ctx, out)
}

// runFallback opens direct per-device subscriptions on the fallback port and
// returns as soon as the proxy is reachable again (or ctx cancels). When no proxy
// is configured at all, direct mode is the steady state and this blocks until ctx.
func (m *Manager) runFallback(ctx context.Context, out chan<- *v1.TelemetryPacket, probe time.Duration) {
	fbCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Direct subscriber config: ignore the proxy, dial device:FallbackPort.
	dcfg := m.cfg
	dcfg.ProxyAddr = ""
	dcfg.Port = m.cfg.FallbackPort

	var wg sync.WaitGroup
	for _, t := range m.targets {
		if !t.Has(target.CapGNMI) {
			continue
		}
		t := t
		b := m.baseFor(t)
		sub := NewSubscriber(t, dcfg, b.orgID, b.dcID, b.floorID, b.netID, b.grpID, b.readerID, m.signer, m.log)
		wg.Add(1)
		go func() {
			defer wg.Done()
			backoff := 2 * time.Second
			for fbCtx.Err() == nil {
				_ = sub.RunStream(fbCtx, out)
				if fbCtx.Err() != nil {
					return
				}
				select {
				case <-fbCtx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff < 30*time.Second {
					backoff *= 2
				}
			}
		}()
	}

	// No proxy configured → direct mode is permanent; just run until shutdown.
	if m.cfg.ProxyAddr == "" {
		<-fbCtx.Done()
		wg.Wait()
		return
	}

	// Poll the proxy; when it returns, cancel the direct subscriptions and switch.
	ticker := time.NewTicker(probe)
	defer ticker.Stop()
	for {
		select {
		case <-fbCtx.Done():
			wg.Wait()
			return
		case <-ticker.C:
			if proxyUp(m.cfg.ProxyAddr) {
				cancel()
				wg.Wait()
				return
			}
		}
	}
}

// proxyUp reports whether the proxy endpoint accepts a TCP connection.
func proxyUp(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}
