package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	valid := Config{
		NodeID:     "node-1",
		ListenAddr: "127.0.0.1:8081",
		Nodes: []Peer{
			{ID: "node-1", Address: "127.0.0.1:8081"},
			{ID: "node-2", Address: "127.0.0.1:8082"},
		},
	}

	tests := []struct {
		name string
		edit func(*Config)
	}{
		{name: "missing node id", edit: func(c *Config) { c.NodeID = "" }},
		{name: "malformed listen address", edit: func(c *Config) { c.ListenAddr = "not-an-address" }},
		{name: "duplicate node id", edit: func(c *Config) { c.Nodes[1].ID = "node-1" }},
		{name: "duplicate address", edit: func(c *Config) { c.Nodes[1].Address = "127.0.0.1:8081" }},
		{name: "self missing", edit: func(c *Config) { c.Nodes[0].ID = "node-3" }},
		{name: "self address conflicts", edit: func(c *Config) { c.Nodes[0].Address = "127.0.0.1:9091" }},
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("valid configuration rejected: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := valid
			cfg.Nodes = append([]Peer(nil), valid.Nodes...)
			test.edit(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func TestLoadRejectsMalformedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.json")
	if err := os.WriteFile(path, []byte(`{"node_id":`), 0o600); err != nil {
		t.Fatalf("write test config: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("malformed JSON was accepted")
	}
}
