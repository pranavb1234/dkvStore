// Package node contains the HTTP server for one key-value-store process.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pranavb1234/dkvStore/internal/config"
	"github.com/pranavb1234/dkvStore/internal/ring"
	"github.com/pranavb1234/dkvStore/internal/store"
)

const routeHopHeader = "X-DKV-Route-Hop"
const replicationHeader = "X-DKV-Replication"

// NewHandler builds a node handler from validated static cluster configuration.
func NewHandler(cfg config.Config, logger *slog.Logger, values *store.Store) (http.Handler, error) {
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate node config: %w", err)
	}
	timeout, _ := cfg.Timeout()
	hashRing, err := ring.New(cfg.VirtualNodes)
	if err != nil {
		return nil, fmt.Errorf("create hash ring: %w", err)
	}
	peers := make(map[string]string, len(cfg.Nodes))
	for _, peer := range cfg.Nodes {
		if err := hashRing.Add(peer.ID); err != nil {
			return nil, fmt.Errorf("add node %q to hash ring: %w", peer.ID, err)
		}
		peers[peer.ID] = peer.Address
	}

	server := &server{
		nodeID:            cfg.NodeID,
		logger:            logger.With("node_id", cfg.NodeID),
		values:            values,
		ring:              hashRing,
		peers:             peers,
		timeout:           timeout,
		client:            &http.Client{Timeout: timeout},
		replicationFactor: cfg.ReplicationFactor,
		writeQuorum:       cfg.WriteQuorum,
		readQuorum:        cfg.ReadQuorum,
		peerHealth:        make(map[string]bool),
		demoAdminEnabled:  cfg.DemoAdminEnabled,
	}
	for _, peer := range cfg.Nodes {
		if peer.ID != cfg.NodeID {
			server.peerHealth[peer.ID] = true
		}
	}
	interval, _ := time.ParseDuration(cfg.HealthCheckInterval)
	healthTimeout, _ := time.ParseDuration(cfg.HealthCheckTimeout)
	go server.monitorPeers(interval, healthTimeout)
	return server.routes(), nil
}

type server struct {
	nodeID            string
	logger            *slog.Logger
	values            *store.Store
	ring              *ring.Ring
	peers             map[string]string
	timeout           time.Duration
	client            *http.Client
	replicationFactor int
	writeQuorum       int
	readQuorum        int
	healthMu          sync.RWMutex
	peerHealth        map[string]bool
	demoAdminEnabled  bool
	simulatedDown     atomic.Bool
	requests          atomic.Uint64
	forwards          atomic.Uint64
	replicaAcks       atomic.Uint64
	writeQuorumFails  atomic.Uint64
	readQuorumFails   atomic.Uint64
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/kv", emptyKey)
	mux.HandleFunc("/kv/", s.publicKeyValue)
	mux.HandleFunc("/internal/route/kv/", s.internalKeyValue)
	mux.HandleFunc("/internal/replicate/kv/", s.internalReplication)
	mux.HandleFunc("/internal/read/kv/", s.internalRead)
	mux.HandleFunc("/admin/simulate-down", s.simulateDown)
	mux.HandleFunc("/admin/simulate-up", s.simulateUp)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.simulatedDown.Load() && !strings.HasPrefix(r.URL.Path, "/admin/") {
			writeError(w, http.StatusServiceUnavailable, "node_simulated_down", "node is simulated down for the demo")
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (s *server) simulateDown(w http.ResponseWriter, r *http.Request) {
	if !s.demoAdminEnabled {
		writeError(w, http.StatusNotFound, "not_found", "endpoint is disabled")
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	s.simulatedDown.Store(true)
	s.logger.Warn("node simulated down")
	writeJSON(w, http.StatusOK, map[string]any{"node_id": s.nodeID, "simulated_down": true})
}

func (s *server) simulateUp(w http.ResponseWriter, r *http.Request) {
	if !s.demoAdminEnabled {
		writeError(w, http.StatusNotFound, "not_found", "endpoint is disabled")
		return
	}
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	s.simulatedDown.Store(false)
	s.logger.Info("node simulated up")
	writeJSON(w, http.StatusOK, map[string]any{"node_id": s.nodeID, "simulated_down": false})
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	s.healthMu.RLock()
	peers := make(map[string]bool, len(s.peerHealth))
	for id, healthy := range s.peerHealth {
		peers[id] = healthy
	}
	s.healthMu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{"node_id": s.nodeID, "status": "ok", "peers": peers})
}

func (s *server) metrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":                  s.nodeID,
		"requests":                 s.requests.Load(),
		"forwards":                 s.forwards.Load(),
		"replica_acknowledgements": s.replicaAcks.Load(),
		"write_quorum_failures":    s.writeQuorumFails.Load(),
		"read_quorum_failures":     s.readQuorumFails.Load(),
	})
}

func (s *server) monitorPeers(interval, timeout time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.probePeers(timeout)
		<-ticker.C
	}
}

func (s *server) probePeers(timeout time.Duration) {
	for peer, address := range s.peers {
		if peer == s.nodeID {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/health", nil)
		resp, err := (&http.Client{Timeout: timeout}).Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		cancel()
		s.healthMu.Lock()
		previous := s.peerHealth[peer]
		current := err == nil && resp != nil && resp.StatusCode == http.StatusOK
		s.peerHealth[peer] = current
		s.healthMu.Unlock()
		if previous != current {
			s.logger.Info("peer health transition", "peer", peer, "healthy", current)
		}
	}
}

func emptyKey(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusBadRequest, "invalid_key", "key must not be empty")
}

func (s *server) publicKeyValue(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		emptyKey(w, r)
		return
	}
	s.logger.Info("request received", "method", r.Method, "key", key)
	s.requests.Add(1)
	// Any healthy owner may coordinate the quorum operation. The ring still
	// provides deterministic owner order, but a failed primary is not a gate.
	s.serveLocal(w, r, key)
}

func (s *server) internalKeyValue(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/route/kv/")
	if key == "" {
		emptyKey(w, r)
		return
	}
	if r.Header.Get(routeHopHeader) != "1" {
		writeError(w, http.StatusLoopDetected, "routing_loop", "internal routing requires exactly one hop")
		return
	}
	primary, _ := s.ring.Primary(key)
	if primary != s.nodeID {
		writeError(w, http.StatusLoopDetected, "routing_loop", "request reached a non-primary node")
		return
	}
	s.serveLocal(w, r, key)
}

func (s *server) forwardToPrimary(w http.ResponseWriter, r *http.Request, key, primary string) {
	s.forwards.Add(1)
	address, exists := s.peers[primary]
	if !exists {
		writeError(w, http.StatusServiceUnavailable, "no_primary", "primary address is not configured")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
	defer cancel()
	target := "http://" + address + "/internal/route/kv/" + url.PathEscape(key)
	request, err := http.NewRequestWithContext(ctx, r.Method, target, r.Body)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not create forwarding request")
		return
	}
	request.Header.Set(routeHopHeader, "1")
	request.Header.Set("Content-Type", r.Header.Get("Content-Type"))
	s.logger.Info("forwarding request", "method", r.Method, "key", key, "routing_target", primary)
	response, err := s.client.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			writeError(w, http.StatusGatewayTimeout, "primary_timeout", "primary did not respond before the deadline")
			return
		}
		writeError(w, http.StatusBadGateway, "primary_unreachable", "could not reach primary node")
		return
	}
	defer response.Body.Close()
	if contentType := response.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	if allow := response.Header.Get("Allow"); allow != "" {
		w.Header().Set("Allow", allow)
	}
	w.WriteHeader(response.StatusCode)
	if _, err := io.Copy(w, response.Body); err != nil {
		s.logger.Error("copy forwarded response", "routing_target", primary, "error", err)
	}
}

func (s *server) serveLocal(w http.ResponseWriter, r *http.Request, key string) {
	switch r.Method {
	case http.MethodPut:
		s.putValue(w, r, key)
	case http.MethodGet:
		s.getValue(w, r, key)
	case http.MethodDelete:
		s.deleteValue(w, r, key)
	default:
		methodNotAllowed(w, "GET, PUT, DELETE")
	}
}

func (s *server) putValue(w http.ResponseWriter, r *http.Request, key string) {
	r.Body = http.MaxBytesReader(w, r.Body, store.DefaultMaxValueBytes+1024)
	defer r.Body.Close()

	var request struct {
		Value *string `json:"value"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeJSONDecodeError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must contain one JSON object")
		return
	}
	if request.Value == nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must include a string value")
		return
	}

	record, created, err := s.values.PutNext(key, *request.Value)
	if errors.Is(err, store.ErrValueTooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "value_too_large", "value exceeds 1048576 bytes")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not store value")
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	owners := s.ring.Owners(key, s.replicationFactor)
	acks := s.writeAcks(r.Context(), key, record, owners)
	if acks < s.writeQuorum {
		s.writeQuorumFails.Add(1)
		s.logger.Warn("write quorum unavailable", "key", key, "acknowledgements", acks, "required", s.writeQuorum)
		writeError(w, http.StatusServiceUnavailable, "write_quorum_unavailable", "write quorum could not be reached")
		return
	}
	s.replicaAcks.Add(uint64(acks))
	s.logger.Info("write quorum acknowledged", "key", key, "acknowledgements", acks, "required", s.writeQuorum)
	writeJSON(w, status, map[string]any{
		"key": key, "value": *request.Value, "version": record.Version,
		"replication": "acknowledged", "acknowledgements": acks,
	})
}

func (s *server) writeAcks(parent context.Context, key string, record store.Record, owners []string) int {
	ctx, cancel := context.WithTimeout(parent, s.timeout)
	defer cancel()
	acks := 0
	pending := 0
	for _, peer := range owners {
		if peer == s.nodeID {
			acks = 1
		} else {
			pending++
		}
	}
	results := make(chan bool, pending)
	for _, peer := range owners {
		if peer == s.nodeID {
			continue
		}
		go func(peer string) { results <- s.replicate(ctx, key, record, peer) == nil }(peer)
	}
	for ; pending > 0; pending-- {
		if <-results {
			acks++
		}
	}
	return acks
}

func (s *server) replicate(parent context.Context, key string, record store.Record, replica string) error {
	address, exists := s.peers[replica]
	if !exists {
		return fmt.Errorf("replica address is not configured")
	}
	body, err := json.Marshal(struct {
		Value   string `json:"value"`
		Version uint64 `json:"version"`
		Deleted bool   `json:"deleted"`
	}{Value: record.Value, Version: record.Version, Deleted: record.Deleted})
	if err != nil {
		return fmt.Errorf("encode replication request: %w", err)
	}
	ctx, cancel := context.WithTimeout(parent, s.timeout)
	defer cancel()
	target := "http://" + address + "/internal/replicate/kv/" + url.PathEscape(key)
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, target, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create replication request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(replicationHeader, "1")
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("send replication request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("replica returned status %d", response.StatusCode)
	}
	return nil
}

func (s *server) internalReplication(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/replicate/kv/")
	if key == "" {
		emptyKey(w, r)
		return
	}
	if r.Method != http.MethodPut {
		methodNotAllowed(w, http.MethodPut)
		return
	}
	if r.Header.Get(replicationHeader) != "1" {
		writeError(w, http.StatusForbidden, "internal_only", "replication endpoint requires an internal request")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, store.DefaultMaxValueBytes+1024)
	defer r.Body.Close()
	var request struct {
		Value   *string `json:"value"`
		Version *uint64 `json:"version"`
		Deleted bool    `json:"deleted"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeJSONDecodeError(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must contain one JSON object")
		return
	}
	if request.Value == nil || request.Version == nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "replication requires value and version")
		return
	}
	applied, err := s.values.Apply(key, *request.Value, *request.Version, request.Deleted)
	switch {
	case errors.Is(err, store.ErrValueTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "value_too_large", "value exceeds 1048576 bytes")
	case errors.Is(err, store.ErrStaleVersion):
		writeError(w, http.StatusConflict, "stale_version", "replica has a newer version")
	case errors.Is(err, store.ErrVersionConflict):
		writeError(w, http.StatusConflict, "version_conflict", "replica has a different value at this version")
	case errors.Is(err, store.ErrInvalidVersion):
		writeError(w, http.StatusBadRequest, "invalid_version", "version must be positive")
	case err != nil:
		writeError(w, http.StatusInternalServerError, "internal_error", "could not apply replication")
	default:
		writeJSON(w, http.StatusOK, map[string]any{"key": key, "version": *request.Version, "applied": applied})
	}
}

func (s *server) internalRead(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/read/kv/")
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if key == "" {
		emptyKey(w, r)
		return
	}
	record, err := s.values.GetRecord(key)
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusOK, map[string]any{"found": false})
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read value")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"found": true, "value": record.Value, "version": record.Version, "deleted": record.Deleted})
}

func (s *server) getValue(w http.ResponseWriter, r *http.Request, key string) {
	owners := s.ring.Owners(key, s.replicationFactor)
	type result struct {
		record store.Record
		found  bool
		ok     bool
	}
	results := make(chan result, len(owners))
	for _, peer := range owners {
		if peer == s.nodeID {
			record, err := s.values.GetRecord(key)
			results <- result{record: record, found: err == nil, ok: err == nil || errors.Is(err, store.ErrNotFound)}
			continue
		}
		go func(peer string) {
			ctx, cancel := context.WithTimeout(r.Context(), s.timeout)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+s.peers[peer]+"/internal/read/kv/"+url.PathEscape(key), nil)
			if err != nil {
				results <- result{}
				return
			}
			resp, err := s.client.Do(req)
			if err != nil || resp.StatusCode != http.StatusOK {
				if resp != nil {
					resp.Body.Close()
				}
				results <- result{}
				return
			}
			defer resp.Body.Close()
			var body struct {
				Found   bool   `json:"found"`
				Value   string `json:"value"`
				Version uint64 `json:"version"`
				Deleted bool   `json:"deleted"`
			}
			if json.NewDecoder(resp.Body).Decode(&body) != nil {
				results <- result{}
				return
			}
			results <- result{record: store.Record{Value: body.Value, Version: body.Version, Deleted: body.Deleted}, found: body.Found, ok: true}
		}(peer)
	}
	successes := 0
	var best store.Record
	found := false
	for range owners {
		result := <-results
		if !result.ok {
			continue
		}
		successes++
		if result.found && (!found || result.record.Version > best.Version || (result.record.Version == best.Version && result.record.Value > best.Value)) {
			best = result.record
			found = true
		}
	}
	if successes < s.readQuorum {
		s.readQuorumFails.Add(1)
		s.logger.Warn("read quorum unavailable", "key", key, "responses", successes, "required", s.readQuorum)
		writeError(w, http.StatusServiceUnavailable, "read_quorum_unavailable", "read quorum could not be reached")
		return
	}
	if !found || best.Deleted {
		writeError(w, http.StatusNotFound, "key_not_found", "key does not exist")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "value": best.Value, "version": best.Version})
}

func (s *server) deleteValue(w http.ResponseWriter, r *http.Request, key string) {
	record, existed := s.values.DeleteNext(key)
	if !existed {
		writeError(w, http.StatusNotFound, "key_not_found", "key does not exist")
		return
	}
	acks := s.writeAcks(r.Context(), key, record, s.ring.Owners(key, s.replicationFactor))
	if acks < s.writeQuorum {
		s.writeQuorumFails.Add(1)
		writeError(w, http.StatusServiceUnavailable, "write_quorum_unavailable", "delete quorum could not be reached")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "deleted": true, "version": record.Version, "acknowledgements": acks})
}

func writeJSONDecodeError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeError(w, http.StatusRequestEntityTooLarge, "value_too_large", "request body exceeds the maximum size")
		return
	}
	writeError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method is not allowed for this endpoint")
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
