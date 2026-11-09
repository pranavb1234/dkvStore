package node

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/pranavb1234/dkvStore/internal/config"
	"github.com/pranavb1234/dkvStore/internal/store"
)

func TestReplicatesPrimaryWriteAndHandlesDuplicateAndStaleVersions(t *testing.T) {
	cluster := startTestCluster(t)
	key := "replicated-key"
	owners := ownersFor(t, cluster.nodes, key, 2)
	primary, replica := owners[0], owners[1]
	entry := cluster.nodeIDs[0]
	if entry == primary {
		entry = cluster.nodeIDs[1]
	}
	client := &http.Client{Timeout: time.Second}

	response, err := client.Do(newRequest(t, http.MethodPut, cluster.urls[entry]+"/kv/"+key, []byte(`{"value":"first"}`)))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated {
		response.Body.Close()
		t.Fatalf("write status = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	var writeBody struct {
		Version     uint64 `json:"version"`
		Replication string `json:"replication"`
		Replica     string `json:"replica"`
	}
	if err := json.NewDecoder(response.Body).Decode(&writeBody); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if writeBody.Version != 1 || writeBody.Replication != "acknowledged" || writeBody.Replica != replica {
		t.Fatalf("write response = %+v, want version 1 acknowledged by %q", writeBody, replica)
	}
	for _, nodeID := range []string{primary, replica} {
		record, err := cluster.stores[nodeID].GetRecord(key)
		if err != nil || record != (store.Record{Value: "first", Version: 1}) {
			t.Fatalf("%s record = (%+v, %v)", nodeID, record, err)
		}
	}

	duplicate := newRequest(t, http.MethodPut, cluster.urls[replica]+"/internal/replicate/kv/"+key, []byte(`{"value":"first","version":1}`))
	duplicate.Header.Set(replicationHeader, "1")
	response, err = client.Do(duplicate)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		t.Fatalf("duplicate status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	var duplicateBody struct {
		Applied bool `json:"applied"`
	}
	if err := json.NewDecoder(response.Body).Decode(&duplicateBody); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if duplicateBody.Applied {
		t.Fatal("duplicate replication reported applied=true")
	}

	response, err = client.Do(newRequest(t, http.MethodPut, cluster.urls[entry]+"/kv/"+key, []byte(`{"value":"second"}`)))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("second write status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	stale := newRequest(t, http.MethodPut, cluster.urls[replica]+"/internal/replicate/kv/"+key, []byte(`{"value":"stale","version":1}`))
	stale.Header.Set(replicationHeader, "1")
	response, err = client.Do(stale)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("stale status = %d, want %d", response.StatusCode, http.StatusConflict)
	}
	assertErrorCode(t, response.Body, "stale_version")
}

func TestWriteIsPartialWhenReplicaIsUnavailable(t *testing.T) {
	listener := listen(t)
	config := config.Config{
		NodeID:         "node-a",
		ListenAddr:     listener.Addr().String(),
		VirtualNodes:   8,
		RequestTimeout: "100ms",
		Nodes: []config.Peer{
			{ID: "node-a", Address: listener.Addr().String()},
			{ID: "node-b", Address: "127.0.0.1:1"},
		},
	}
	key := keyForPrimary(t, config.Nodes, config.VirtualNodes, "node-a")
	values := store.New(store.DefaultMaxValueBytes)
	handler, err := NewHandler(config, testLogger(), values)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go server.Serve(listener)
	t.Cleanup(func() {
		_ = server.Close()
	})

	response, err := (&http.Client{Timeout: time.Second}).Do(newRequest(t, http.MethodPut, "http://"+listener.Addr().String()+"/kv/"+key, []byte(`{"value":"kept-by-primary"}`)))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusAccepted)
	}
	var body struct {
		Replication string `json:"replication"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Replication != "partial" {
		t.Fatalf("replication = %q, want partial", body.Replication)
	}
	record, err := values.GetRecord(key)
	if err != nil || record != (store.Record{Value: "kept-by-primary", Version: 1}) {
		t.Fatalf("primary record = (%+v, %v)", record, err)
	}
}

func TestReplicationEndpointDoesNotRoutePublicRequests(t *testing.T) {
	handler := testHandler()
	response := request(t, handler, http.MethodPut, "/internal/replicate/kv/key", `{"value":"value","version":1}`)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
}
