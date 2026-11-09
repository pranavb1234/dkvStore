// Package node contains the HTTP server for one key-value-store process.
package node

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// NewHandler returns the public HTTP handler for a node. Phase 1 intentionally
// exposes only liveness; storage, hashing, and replication arrive later.
func NewHandler(nodeID string, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(struct {
			NodeID string `json:"node_id"`
			Status string `json:"status"`
		}{NodeID: nodeID, Status: "ok"}); err != nil {
			logger.Error("write health response", "error", err)
		}
	})
	return mux
}
