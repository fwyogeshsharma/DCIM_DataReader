package command

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/faberwork/fwedr/internal/snmp"
	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/config"
)

// Runner ties together the DCS client and the device Applier into a periodic
// pull → apply → ack loop.
type Runner struct {
	cfg     config.CommandApplyConfig
	client  *dcsClient
	applier *Applier
	log     *zap.Logger
	// byIP maps a device mgmt IP to its Target so an applied asset edit can be
	// written back into our in-memory copy (asset fields are seeded from JSON and
	// not re-read from the device, so the next push must carry the new value).
	byIP map[string]*target.Target
}

// New builds a command Runner. rf supplies Redfish credentials for power
// commands; targets lets applied asset edits update the in-memory copy. Call Run
// in a goroutine.
func New(cfg config.CommandApplyConfig, rf config.RedfishConfig, profile *snmp.Profile, targets []*target.Target, log *zap.Logger) *Runner {
	byIP := make(map[string]*target.Target, len(targets))
	for _, t := range targets {
		if t.MgmtIP != "" {
			byIP[t.MgmtIP] = t
		}
		if t.IP != "" {
			byIP[t.IP] = t
		}
	}
	return &Runner{
		cfg:     cfg,
		client:  newDCSClient(cfg),
		applier: NewApplier(cfg, rf, profile),
		log:     log,
		byIP:    byIP,
	}
}

// Run polls DCS for pending commands and applies them until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) {
	interval := time.Duration(r.cfg.PollIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 30 * time.Second
	}
	r.log.Info("command-apply loop started",
		zap.String("dcs_base_url", r.cfg.DCSBaseURL),
		zap.Int("poll_interval_ms", r.cfg.PollIntervalMs),
		zap.Int("snmp_set_port", r.cfg.SNMPSetPort))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

// tick performs one pull-apply-ack cycle.
func (r *Runner) tick(ctx context.Context) {
	cmds, err := r.client.pull(ctx, r.cfg.BatchLimit)
	if err != nil {
		r.log.Debug("command pull failed", zap.Error(err))
		return
	}
	if len(cmds) == 0 {
		return
	}
	applied, failed := 0, 0
	for _, c := range cmds {
		err := r.applier.Apply(ctx, c)
		ok := err == nil
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
			failed++
			r.log.Warn("command apply failed",
				zap.Int64("id", c.ID), zap.String("device_ip", c.DeviceIP),
				zap.String("field", c.Field), zap.Error(err))
		} else {
			applied++
			// Reflect an applied asset/location edit in our in-memory target so the
			// next telemetry push carries the new value (these aren't re-read from
			// the device). No-op for non-asset fields (power/threshold/led).
			if t := r.byIP[stripCIDR(c.DeviceIP)]; t != nil {
				t.PatchAsset(c.Field, c.Value)
			}
			r.log.Info("command applied",
				zap.Int64("id", c.ID), zap.String("device_ip", c.DeviceIP),
				zap.String("field", c.Field), zap.String("value", c.Value))
		}
		if ackErr := r.client.ack(ctx, c.ID, ok, errMsg); ackErr != nil {
			r.log.Warn("command ack failed", zap.Int64("id", c.ID), zap.Error(ackErr))
		}
		if ctx.Err() != nil {
			return
		}
	}
	r.log.Info("command batch done", zap.Int("applied", applied), zap.Int("failed", failed))
}

// stripCIDR removes a trailing /mask so an INET-formatted device_ip matches the
// target map (keyed by bare IP).
func stripCIDR(ip string) string {
	if i := strings.IndexByte(ip, '/'); i >= 0 {
		return ip[:i]
	}
	return ip
}
