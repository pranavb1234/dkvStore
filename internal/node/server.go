// Package node contains the HTTP server for one key-value-store process.
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pranavb1234/dkvStore/internal/config"
	"github.com/pranavb1234/dkvStore/internal/ring"
	"github.com/pranavb1234/dkvStore/internal/store"
)

const routeHopHeader = "X-DKV-Route-Hop"

// NewHandler builds a node handler from validated static cluster configuration.
func NewHandler(cfg config.Config, logger *slog.Logger, values *store.Store) (http.Handler, error) {
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
		nodeID:  cfg.NodeID,
		logger:  logger.With("node_id", cfg.NodeID),
		values:  values,
		ring:    hashRing,
		peers:   peers,
		timeout: timeout,
		client:  &http.Client{Timeout: timeout},
	}
	return server.routes(), nil
}

type server struct {
	nodeID  string
	logger  *slog.Logger
	values  *store.Store
	ring    *ring.Ring
	peers   map[string]string
	timeout time.Duration
	client  *http.Client
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/kv", emptyKey)
	mux.HandleFunc("/kv/", s.publicKeyValue)
	mux.HandleFunc("/internal/route/kv/", s.internalKeyValue)
	return mux
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"node_id": s.nodeID, "status": "ok"})
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
	primary, ok := s.ring.Primary(key)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "no_primary", "no primary node is configured")
		return
	}
	if primary != s.nodeID {
		s.forwardToPrimary(w, r, key, primary)
		return
	}
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
		putValue(w, r, key, s.values)
	case http.MethodGet:
		getValue(w, key, s.values)
	case http.MethodDelete:
		deleteValue(w, key, s.values)
	default:
		methodNotAllowed(w, "GET, PUT, DELETE")
	}
}

func putValue(w http.ResponseWriter, r *http.Request, key string, values *store.Store) {
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

	created, err := values.Put(key, *request.Value)
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
	writeJSON(w, status, map[string]string{"key": key, "value": *request.Value})
}

func getValue(w http.ResponseWriter, key string, values *store.Store) {
	value, err := values.Get(key)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "key_not_found", "key does not exist")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "could not read value")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": value})
}

func deleteValue(w http.ResponseWriter, key string, values *store.Store) {
	if !values.Delete(key) {
		writeError(w, http.StatusNotFound, "key_not_found", "key does not exist")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "deleted": true})
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
