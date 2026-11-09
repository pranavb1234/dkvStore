// Package node contains the HTTP server for one key-value-store process.
package node

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/pranavb1234/dkvStore/internal/store"
)

// NewHandler returns the public HTTP handler for a node. The supplied store is
// local only; routing and replication are deliberately not part of Phase 2.
func NewHandler(nodeID string, logger *slog.Logger, values *store.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
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
	mux.HandleFunc("/kv", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusBadRequest, "invalid_key", "key must not be empty")
	})
	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/kv/")
		if key == "" {
			writeError(w, http.StatusBadRequest, "invalid_key", "key must not be empty")
			return
		}

		switch r.Method {
		case http.MethodPut:
			putValue(w, r, key, values)
		case http.MethodGet:
			getValue(w, key, values)
		case http.MethodDelete:
			deleteValue(w, key, values)
		default:
			methodNotAllowed(w, "GET, PUT, DELETE")
		}
	})
	return mux
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
