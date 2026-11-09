package node

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/pranavb1234/dkvStore/internal/config"
	"github.com/pranavb1234/dkvStore/internal/store"
)

func TestHealth(t *testing.T) {
	handler := testHandler()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "application/json") {
		t.Fatalf("content type = %q, want application/json", contentType)
	}
	var body struct {
		NodeID string `json:"node_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || body.NodeID != "node-1" || body.Status != "ok" {
		t.Fatalf("health body = %q, error = %v", recorder.Body.String(), err)
	}
}

func TestHealthRejectsNonGET(t *testing.T) {
	handler := testHandler()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/health", nil))

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
	}
}

func TestKeyValueAPI(t *testing.T) {
	handler := testHandler()

	response := request(t, handler, http.MethodPut, "/kv/course", `{"value":"go"}`)
	if response.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", response.Code, http.StatusCreated)
	}
	assertJSON(t, response.Body.String(), map[string]any{
		"key": "course", "value": "go", "version": float64(1),
		"replication": "acknowledged", "acknowledgements": float64(1),
	})

	response = request(t, handler, http.MethodPut, "/kv/course", `{"value":"systems"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("replace status = %d, want %d", response.Code, http.StatusOK)
	}
	response = request(t, handler, http.MethodGet, "/kv/course", "")
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", response.Code, http.StatusOK)
	}
	assertJSON(t, response.Body.String(), map[string]any{"key": "course", "value": "systems", "version": float64(2)})

	response = request(t, handler, http.MethodDelete, "/kv/course", "")
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want %d", response.Code, http.StatusOK)
	}
	var deleted map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &deleted); err != nil || deleted["deleted"] != true {
		t.Fatalf("delete response = %q, error = %v", response.Body.String(), err)
	}
	response = request(t, handler, http.MethodGet, "/kv/course", "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing get status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestKeyValueAPIRejectsInvalidInput(t *testing.T) {
	handler := testHandler()
	tests := []struct {
		name   string
		method string
		path   string
		body   string
		status int
		code   string
	}{
		{name: "empty key", method: http.MethodGet, path: "/kv/", status: http.StatusBadRequest, code: "invalid_key"},
		{name: "malformed JSON", method: http.MethodPut, path: "/kv/key", body: `{`, status: http.StatusBadRequest, code: "invalid_json"},
		{name: "unknown JSON field", method: http.MethodPut, path: "/kv/key", body: `{"other":"value"}`, status: http.StatusBadRequest, code: "invalid_json"},
		{name: "missing value", method: http.MethodPut, path: "/kv/key", body: `{}`, status: http.StatusBadRequest, code: "invalid_json"},
		{name: "missing key", method: http.MethodDelete, path: "/kv/missing", status: http.StatusNotFound, code: "key_not_found"},
		{name: "wrong method", method: http.MethodPost, path: "/kv/key", status: http.StatusMethodNotAllowed, code: "method_not_allowed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := request(t, handler, test.method, test.path, test.body)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d", response.Code, test.status)
			}
			var body struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if body.Error.Code != test.code {
				t.Fatalf("error code = %q, want %q", body.Error.Code, test.code)
			}
		})
	}
}

func TestKeyValueAPIRejectsOversizedValue(t *testing.T) {
	handler := testHandler()
	body := `{"value":"` + strings.Repeat("x", store.DefaultMaxValueBytes+1) + `"}`
	response := request(t, handler, http.MethodPut, "/kv/key", body)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
}

func testHandler() http.Handler {
	handler, err := NewHandler(config.Config{
		NodeID:            "node-1",
		ListenAddr:        "127.0.0.1:8081",
		VirtualNodes:      8,
		RequestTimeout:    "500ms",
		ReplicationFactor: 1, WriteQuorum: 1, ReadQuorum: 1,
		Nodes: []config.Peer{{ID: "node-1", Address: "127.0.0.1:8081"}},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), store.New(store.DefaultMaxValueBytes))
	if err != nil {
		panic(err)
	}
	return handler
}

func request(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(method, path, bytes.NewBufferString(body)))
	return response
}

func assertJSON(t *testing.T, actual string, want map[string]any) {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal([]byte(actual), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response = %#v, want %#v", got, want)
	}
}
