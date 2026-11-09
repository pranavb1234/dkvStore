package node

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/pranavb1234/dkvStore/internal/config"
	"github.com/pranavb1234/dkvStore/internal/ring"
	"github.com/pranavb1234/dkvStore/internal/store"
)

func TestRoutesRequestsFromEveryEntryNodeToPrimary(t *testing.T) {
	cluster := startTestCluster(t)
	key := "routing-key"
	primary := primaryFor(t, cluster.nodes, key)
	client := &http.Client{Timeout: time.Second}

	for index, nodeID := range cluster.nodeIDs {
		body := []byte(fmt.Sprintf(`{"value":"from-%s"}`, nodeID))
		response, err := client.Do(newRequest(t, http.MethodPut, cluster.urls[nodeID]+"/kv/"+key, body))
		if err != nil {
			t.Fatalf("PUT through %s: %v", nodeID, err)
		}
		response.Body.Close()
		wantStatus := http.StatusOK
		if index == 0 {
			wantStatus = http.StatusCreated
		}
		if response.StatusCode != wantStatus {
			t.Fatalf("PUT through %s status = %d, want %d", nodeID, response.StatusCode, wantStatus)
		}
	}

	value, err := cluster.stores[primary].Get(key)
	if err != nil || value != "from-node-c" {
		t.Fatalf("primary %q value = (%q, %v)", primary, value, err)
	}
	for _, nodeID := range cluster.nodeIDs {
		if nodeID == primary {
			continue
		}
		if _, err := cluster.stores[nodeID].Get(key); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("non-primary %q stored routed key: %v", nodeID, err)
		}
	}

	for _, nodeID := range cluster.nodeIDs {
		response, err := client.Get(cluster.urls[nodeID] + "/kv/" + key)
		if err != nil {
			t.Fatalf("GET through %s: %v", nodeID, err)
		}
		var body struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			response.Body.Close()
			t.Fatalf("decode GET through %s: %v", nodeID, err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK || body.Value != "from-node-c" {
			t.Fatalf("GET through %s = (%d, %q)", nodeID, response.StatusCode, body.Value)
		}
	}
}

func TestReturnsUsefulErrorWhenPrimaryIsUnavailable(t *testing.T) {
	listener := listen(t)
	defer listener.Close()
	config := config.Config{
		NodeID:         "node-b",
		ListenAddr:     listener.Addr().String(),
		VirtualNodes:   8,
		RequestTimeout: "100ms",
		Nodes: []config.Peer{
			{ID: "node-a", Address: "127.0.0.1:1"},
			{ID: "node-b", Address: listener.Addr().String()},
		},
	}
	key := keyForPrimary(t, config.Nodes, config.VirtualNodes, "node-a")
	handler, err := NewHandler(config, testLogger(), store.New(store.DefaultMaxValueBytes))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	defer server.Close()

	response, err := (&http.Client{Timeout: time.Second}).Get("http://" + listener.Addr().String() + "/kv/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusBadGateway)
	}
	assertErrorCode(t, response.Body, "primary_unreachable")
}

func TestInternalRouteRejectsMissingHopHeader(t *testing.T) {
	handler := testHandler()
	response := request(t, handler, http.MethodGet, "/internal/route/kv/key", "")
	if response.Code != http.StatusLoopDetected {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusLoopDetected)
	}
}

type testCluster struct {
	nodeIDs []string
	nodes   []config.Peer
	urls    map[string]string
	stores  map[string]*store.Store
	servers []*http.Server
}

func startTestCluster(t *testing.T) *testCluster {
	t.Helper()
	nodeIDs := []string{"node-a", "node-b", "node-c"}
	listeners := make(map[string]net.Listener, len(nodeIDs))
	nodes := make([]config.Peer, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		listener := listen(t)
		listeners[nodeID] = listener
		nodes = append(nodes, config.Peer{ID: nodeID, Address: listener.Addr().String()})
	}
	cluster := &testCluster{
		nodeIDs: nodeIDs,
		nodes:   nodes,
		urls:    make(map[string]string, len(nodeIDs)),
		stores:  make(map[string]*store.Store, len(nodeIDs)),
	}
	for _, nodeID := range nodeIDs {
		values := store.New(store.DefaultMaxValueBytes)
		handler, err := NewHandler(config.Config{
			NodeID:         nodeID,
			ListenAddr:     listeners[nodeID].Addr().String(),
			VirtualNodes:   32,
			RequestTimeout: "500ms",
			Nodes:          nodes,
		}, testLogger(), values)
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{Handler: handler}
		cluster.urls[nodeID] = "http://" + listeners[nodeID].Addr().String()
		cluster.stores[nodeID] = values
		cluster.servers = append(cluster.servers, server)
		go server.Serve(listeners[nodeID])
	}
	t.Cleanup(func() {
		for _, server := range cluster.servers {
			_ = server.Close()
		}
	})
	return cluster
}

func primaryFor(t *testing.T, nodes []config.Peer, key string) string {
	t.Helper()
	hashRing, err := ring.New(32)
	if err != nil {
		t.Fatal(err)
	}
	for _, peer := range nodes {
		if err := hashRing.Add(peer.ID); err != nil {
			t.Fatal(err)
		}
	}
	primary, ok := hashRing.Primary(key)
	if !ok {
		t.Fatal("no primary")
	}
	return primary
}

func keyForPrimary(t *testing.T, nodes []config.Peer, virtualNodes int, want string) string {
	t.Helper()
	hashRing, err := ring.New(virtualNodes)
	if err != nil {
		t.Fatal(err)
	}
	for _, peer := range nodes {
		if err := hashRing.Add(peer.ID); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; ; index++ {
		key := fmt.Sprintf("key-%d", index)
		owner, _ := hashRing.Primary(key)
		if owner == want {
			return key
		}
	}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func newRequest(t *testing.T, method, target string, body []byte) *http.Request {
	t.Helper()
	request, err := http.NewRequest(method, target, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	return request
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func assertErrorCode(t *testing.T, body io.Reader, want string) {
	t.Helper()
	var response struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != want {
		t.Fatalf("error code = %q, want %q", response.Error.Code, want)
	}
}
