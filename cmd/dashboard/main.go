// dashboard provides a dependency-free browser UI for the Docker demo.
package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

//go:embed web/index.html
var page []byte

type dashboard struct {
	nodes  map[string]string
	client *http.Client
	logger *slog.Logger
}

func main() {
	nodes := map[string]string{}
	for _, id := range strings.Split(defaultEnv("DKV_NODES", "node1,node2,node3"), ",") {
		nodes[id] = "http://" + id + ":8080"
	}
	d := &dashboard{nodes: nodes, client: &http.Client{Timeout: time.Second}, logger: slog.New(slog.NewJSONHandler(os.Stdout, nil))}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(page)
	})
	mux.HandleFunc("/api/nodes", d.nodesHandler)
	mux.HandleFunc("/api/kv", d.kvHandler)
	mux.HandleFunc("/api/nodes/", d.actionHandler)
	d.logger.Info("dashboard listening", "address", ":8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		d.logger.Error("dashboard stopped", "error", err)
	}
}

func (d *dashboard) nodesHandler(w http.ResponseWriter, _ *http.Request) {
	type item struct {
		ID     string          `json:"id"`
		Health json.RawMessage `json:"health,omitempty"`
		Error  string          `json:"error,omitempty"`
	}
	results := make(chan item, len(d.nodes))
	for id, base := range d.nodes {
		go func(id, base string) {
			r, err := d.client.Get(base + "/health")
			if err != nil {
				results <- item{ID: id, Error: err.Error()}
				return
			}
			defer r.Body.Close()
			body, _ := io.ReadAll(r.Body)
			if r.StatusCode != http.StatusOK {
				results <- item{ID: id, Error: string(body)}
				return
			}
			results <- item{ID: id, Health: body}
		}(id, base)
	}
	items := make([]item, 0, len(d.nodes))
	for range d.nodes {
		items = append(items, <-results)
	}
	writeJSON(w, http.StatusOK, items)
}

func (d *dashboard) kvHandler(w http.ResponseWriter, r *http.Request) {
	node, key := r.URL.Query().Get("node"), r.URL.Query().Get("key")
	base, ok := d.nodes[node]
	if !ok || key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "node and key are required"})
		return
	}
	method, body := r.Method, r.Body
	if method != http.MethodGet && method != http.MethodPut && method != http.MethodDelete {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "use GET, PUT, or DELETE"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, base+"/kv/"+url.PathEscape(key), body)
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	d.proxy(w, req)
}

func (d *dashboard) actionHandler(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/nodes/"), "/")
	if len(parts) != 2 || r.Method != http.MethodPost {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	base, ok := d.nodes[parts[0]]
	if !ok || (parts[1] != "down" && parts[1] != "up") {
		writeJSON(w, 404, map[string]string{"error": "unknown node"})
		return
	}
	req, _ := http.NewRequest(http.MethodPost, base+"/admin/simulate-"+parts[1], bytes.NewReader(nil))
	d.proxy(w, req)
}

func (d *dashboard) proxy(w http.ResponseWriter, req *http.Request) {
	resp, err := d.client.Do(req)
	if err != nil {
		writeJSON(w, 502, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func defaultEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
