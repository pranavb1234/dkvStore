// Package config loads and validates static node configuration.
package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var nodeIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)

// Config identifies this process and the complete static cluster membership.
// Nodes must include the local node exactly once.
type Config struct {
	NodeID              string `json:"node_id"`
	ListenAddr          string `json:"listen_addr"`
	AdvertiseAddr       string `json:"advertise_addr"`
	Nodes               []Peer `json:"nodes"`
	VirtualNodes        int    `json:"virtual_nodes"`
	RequestTimeout      string `json:"request_timeout"`
	ReplicationFactor   int    `json:"replication_factor"`
	WriteQuorum         int    `json:"write_quorum"`
	ReadQuorum          int    `json:"read_quorum"`
	HealthCheckInterval string `json:"health_check_interval"`
	HealthCheckTimeout  string `json:"health_check_timeout"`
	DemoAdminEnabled    bool   `json:"demo_admin_enabled"`
}

// Peer is one node's stable identity and HTTP listen address.
type Peer struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

// Load reads a JSON configuration file and validates it before returning it.
func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()

	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("decode config: multiple JSON values")
		}
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate verifies identities, addresses, duplicate entries, and the local
// node's matching membership entry.
func (c Config) Validate() error {
	c.ApplyDefaults()
	if !validNodeID(c.NodeID) {
		return fmt.Errorf("invalid node_id %q", c.NodeID)
	}
	if err := validAddress(c.ListenAddr); err != nil {
		return fmt.Errorf("invalid listen_addr: %w", err)
	}
	if err := validAddress(c.AdvertiseAddr); err != nil {
		return fmt.Errorf("invalid advertise_addr: %w", err)
	}
	if c.VirtualNodes <= 0 {
		return fmt.Errorf("virtual_nodes must be positive")
	}
	if _, err := c.Timeout(); err != nil {
		return fmt.Errorf("invalid request_timeout: %w", err)
	}
	interval, intervalErr := time.ParseDuration(c.HealthCheckInterval)
	if intervalErr != nil || interval <= 0 {
		return fmt.Errorf("invalid health_check_interval")
	}
	healthTimeout, healthErr := time.ParseDuration(c.HealthCheckTimeout)
	if healthErr != nil || healthTimeout <= 0 {
		return fmt.Errorf("invalid health_check_timeout")
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("nodes must contain the complete membership list")
	}
	if c.ReplicationFactor < 1 || c.ReplicationFactor > len(c.Nodes) {
		return fmt.Errorf("replication_factor must be between 1 and %d", len(c.Nodes))
	}
	if c.WriteQuorum < 1 || c.WriteQuorum > c.ReplicationFactor {
		return fmt.Errorf("write_quorum must be between 1 and replication_factor")
	}
	if c.ReadQuorum < 1 || c.ReadQuorum > c.ReplicationFactor {
		return fmt.Errorf("read_quorum must be between 1 and replication_factor")
	}

	ids := make(map[string]struct{}, len(c.Nodes))
	addresses := make(map[string]struct{}, len(c.Nodes))
	selfAddress := ""
	for _, peer := range c.Nodes {
		if !validNodeID(peer.ID) {
			return fmt.Errorf("invalid node id %q", peer.ID)
		}
		if err := validAddress(peer.Address); err != nil {
			return fmt.Errorf("invalid address for node %q: %w", peer.ID, err)
		}
		if _, exists := ids[peer.ID]; exists {
			return fmt.Errorf("duplicate node id %q", peer.ID)
		}
		if _, exists := addresses[peer.Address]; exists {
			return fmt.Errorf("duplicate node address %q", peer.Address)
		}
		ids[peer.ID] = struct{}{}
		addresses[peer.Address] = struct{}{}
		if peer.ID == c.NodeID {
			selfAddress = peer.Address
		}
	}

	if selfAddress == "" {
		return fmt.Errorf("node_id %q is missing from nodes", c.NodeID)
	}
	if selfAddress != c.AdvertiseAddr {
		return fmt.Errorf("self address %q does not match advertise_addr %q", selfAddress, c.AdvertiseAddr)
	}
	return nil
}

// ApplyDefaults sets quorum values for omitted JSON fields.
func (c *Config) ApplyDefaults() {
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.ListenAddr
	}
	if c.ReplicationFactor == 0 {
		c.ReplicationFactor = 3
	}
	if c.WriteQuorum == 0 {
		c.WriteQuorum = 2
	}
	if c.ReadQuorum == 0 {
		c.ReadQuorum = 2
	}
	if c.HealthCheckInterval == "" {
		c.HealthCheckInterval = "1s"
	}
	if c.HealthCheckTimeout == "" {
		c.HealthCheckTimeout = "250ms"
	}
}

// Timeout returns the configured bounded deadline for peer HTTP requests.
func (c Config) Timeout() (time.Duration, error) {
	timeout, err := time.ParseDuration(c.RequestTimeout)
	if err != nil || timeout <= 0 {
		return 0, fmt.Errorf("must be a positive Go duration")
	}
	return timeout, nil
}

func validNodeID(id string) bool {
	return nodeIDPattern.MatchString(id)
}

func validAddress(address string) error {
	if strings.TrimSpace(address) != address || address == "" {
		return fmt.Errorf("address must not be empty or contain surrounding whitespace")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return fmt.Errorf("must be host:port")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	return nil
}
