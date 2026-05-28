// Package config loads YAML configuration for IDR.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// IDRConfig is the full configuration for the Internal Data Reader agent.
type IDRConfig struct {
	Identity   IdentityConfig  `yaml:"identity"`
	DCS        DCSClientConfig `yaml:"dcs"`
	Queue      QueueConfig     `yaml:"queue"`
	Collectors CollectorConfig `yaml:"collectors"`
	Log        LogConfig       `yaml:"log"`
}

// ─── sub-structs ────────────────────────────────────────────────────────────

type IdentityConfig struct {
	OrgID        string `yaml:"org_id"`
	DatacenterID string `yaml:"datacenter_id"`
	FloorID      string `yaml:"floor_id"`
	NetworkID    string `yaml:"network_id"`
	GroupID      string `yaml:"group_id"`
	ReaderID     string `yaml:"reader_id"` // e.g. "idr-1.0.0"
}

type DCSClientConfig struct {
	Endpoint string    `yaml:"endpoint"` // host:port
	TLS      TLSConfig `yaml:"tls"`
}

type TLSConfig struct {
	CertFile string `yaml:"cert"`
	KeyFile  string `yaml:"key"`
	CAFile   string `yaml:"ca"`
	Insecure bool   `yaml:"insecure"` // dev-only: skip verification
}

type QueueConfig struct {
	Path     string `yaml:"path"`      // local bbolt DB path
	MaxBytes int64  `yaml:"max_bytes"` // max queue size (bytes); 0 = 512 MB
}

type CollectorConfig struct {
	IntervalMs int      `yaml:"interval_ms"` // collection interval (default 15000)
	Enable     []string `yaml:"enable"`      // [cpu, memory, disk, network, temperature, os]
}

type GRPCServerConfig struct {
	Addr string `yaml:"addr"` // e.g. ":9090"
}

type LogConfig struct {
	Level  string `yaml:"level"`  // debug|info|warn|error
	Format string `yaml:"format"` // json|console
}

// ─── loaders ────────────────────────────────────────────────────────────────

func LoadIDR(path string) (*IDRConfig, error) {
	cfg := &IDRConfig{}
	cfg.Queue.MaxBytes = 512 * 1024 * 1024
	cfg.Collectors.IntervalMs = 15000
	cfg.Collectors.Enable = []string{"cpu", "memory", "disk", "network", "temperature", "os"}
	if err := loadYAML(path, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func loadYAML(path string, out any) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()
	if err := yaml.NewDecoder(f).Decode(out); err != nil {
		return fmt.Errorf("config: decode %s: %w", path, err)
	}
	return nil
}
