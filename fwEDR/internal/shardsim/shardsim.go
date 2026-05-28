// Package shardsim optionally launches the simulator's SNMP responder processes
// from inside EDR, one per shard port, so a single `edr.exe` brings up the whole
// sim stack. It is a sim/test convenience only — gated by snmp.shard_spawn, which
// is OFF by default. In production EDR polls real devices and spawns nothing.
//
// It does NOT modify the simulator: it runs extra copies of the existing
// snmpsim-command-responder tool against the simulator's read-only dataset.
// On Windows the responders are tied to a Job Object so they are killed when EDR
// exits for any reason (normal, crash, force-kill) — no orphaned processes
// holding ports across EDR restarts.
package shardsim

import (
	"fmt"
	"os/exec"

	"go.uber.org/zap"
)

// Spawner owns the launched responder processes.
type Spawner struct {
	cmds []*exec.Cmd
	job  *job
	log  *zap.Logger
}

// Ports returns the responder port set for N shards: base+0 .. base+N-1.
// MUST match the routing in internal/snmp/client.go ShardPort() (base + hash%N).
func Ports(shards, base int) []int {
	if shards < 1 {
		shards = 1
	}
	ports := make([]int, 0, shards)
	for i := 0; i < shards; i++ {
		ports = append(ports, base+i)
	}
	return ports
}

// Spawn launches one snmpsim responder per port against dataDir. Returns a
// Spawner whose Stop() kills them all. If any responder fails to start, the ones
// already started are stopped and an error is returned.
func Spawn(responderPath, dataDir string, ports []int, log *zap.Logger) (*Spawner, error) {
	if responderPath == "" || dataDir == "" {
		return nil, fmt.Errorf("shardsim: shard_responder_path and shard_data_dir must be set")
	}
	j, err := newJob()
	if err != nil {
		log.Warn("shardsim: job object unavailable — responders may survive an EDR crash", zap.Error(err))
		j = nil
	}
	s := &Spawner{job: j, log: log}
	for _, p := range ports {
		cmd := exec.Command(responderPath,
			fmt.Sprintf("--data-dir=%s", dataDir),
			"--log-level=error",
			fmt.Sprintf("--agent-udpv4-endpoint=0.0.0.0:%d", p),
		)
		if err := cmd.Start(); err != nil {
			s.Stop()
			return nil, fmt.Errorf("shardsim: start responder on :%d: %w", p, err)
		}
		if j != nil {
			if err := j.assign(cmd.Process.Pid); err != nil {
				log.Warn("shardsim: assign-to-job failed",
					zap.Int("pid", cmd.Process.Pid), zap.Error(err))
			}
		}
		log.Info("shardsim: responder started", zap.Int("port", p), zap.Int("pid", cmd.Process.Pid))
		s.cmds = append(s.cmds, cmd)
	}
	return s, nil
}

// Stop kills every responder and releases the job handle.
func (s *Spawner) Stop() {
	if s == nil {
		return
	}
	for _, c := range s.cmds {
		if c.Process != nil {
			_ = c.Process.Kill()
		}
	}
	s.job.close()
}
