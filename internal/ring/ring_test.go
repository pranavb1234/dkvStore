package ring

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestDeterministicMappingRegardlessOfAddOrder(t *testing.T) {
	first := mustRing(t, 32, "node-c", "node-a", "node-b")
	second := mustRing(t, 32, "node-b", "node-c", "node-a")
	for _, key := range []string{"alpha", "beta", "course", "student-42", "zebra"} {
		firstOwner, firstOK := first.Primary(key)
		secondOwner, secondOK := second.Primary(key)
		if !firstOK || !secondOK || firstOwner != secondOwner {
			t.Fatalf("primary for %q = (%q, %t) and (%q, %t)", key, firstOwner, firstOK, secondOwner, secondOK)
		}
		if got, want := first.Owners(key, 3), second.Owners(key, 3); !reflect.DeepEqual(got, want) {
			t.Fatalf("owners for %q = %v, want %v", key, got, want)
		}
	}
}

func TestAddRemoveAndEmptyRing(t *testing.T) {
	ring, err := New(4)
	if err != nil {
		t.Fatal(err)
	}
	if owner, ok := ring.Primary("key"); ok || owner != "" {
		t.Fatalf("empty Primary = (%q, %t), want (empty, false)", owner, ok)
	}
	if owners := ring.Owners("key", 2); owners != nil {
		t.Fatalf("empty Owners = %v, want nil", owners)
	}
	if err := ring.Add("node-a"); err != nil {
		t.Fatal(err)
	}
	if err := ring.Add("node-a"); err != ErrNodeExists {
		t.Fatalf("duplicate Add error = %v, want %v", err, ErrNodeExists)
	}
	if !ring.Remove("node-a") || ring.Remove("node-a") {
		t.Fatal("Remove did not report presence correctly")
	}
}

func TestOwnersAreDistinctPhysicalNodes(t *testing.T) {
	ring := mustRing(t, 64, "node-a", "node-b", "node-c")
	for _, key := range []string{"alpha", "beta", "gamma", "delta"} {
		owners := ring.Owners(key, 10)
		if len(owners) != 3 {
			t.Fatalf("Owners(%q) has %d nodes, want 3", key, len(owners))
		}
		seen := make(map[string]struct{}, len(owners))
		for _, owner := range owners {
			if _, exists := seen[owner]; exists {
				t.Fatalf("Owners(%q) repeated physical node %q: %v", key, owner, owners)
			}
			seen[owner] = struct{}{}
		}
	}
}

func TestWrapAround(t *testing.T) {
	low := [32]byte{0x10}
	high := [32]byte{0xf0}
	ring := &Ring{entries: []entry{{token: low, nodeID: "low"}, {token: high, nodeID: "high"}}}
	if got := ring.entryIndex([32]byte{0xff}); got != 0 {
		t.Fatalf("entry index after final token = %d, want 0", got)
	}
	if got := ring.entryIndex([32]byte{0x80}); got != 1 {
		t.Fatalf("entry index between tokens = %d, want 1", got)
	}
}

func TestDistributionAcrossSampleKeys(t *testing.T) {
	ring := mustRing(t, 64, "node-a", "node-b", "node-c")
	counts := make(map[string]int)
	for index := 0; index < 10000; index++ {
		owner, ok := ring.Primary(fmt.Sprintf("key-%d", index))
		if !ok {
			t.Fatal("non-empty ring returned no owner")
		}
		counts[owner]++
	}
	for _, nodeID := range ring.Nodes() {
		if counts[nodeID] < 1000 {
			t.Fatalf("node %q received only %d of 10000 sample keys", nodeID, counts[nodeID])
		}
	}
}

func TestTokenOrderingTieBreaksByNodeThenIndex(t *testing.T) {
	token := [32]byte{0x99}
	entries := []entry{
		{token: token, nodeID: "node-b", index: 1},
		{token: token, nodeID: "node-a", index: 2},
		{token: token, nodeID: "node-a", index: 1},
	}
	sortEntries(entries)
	if got := []string{entries[0].nodeID, entries[1].nodeID, entries[2].nodeID}; !reflect.DeepEqual(got, []string{"node-a", "node-a", "node-b"}) {
		t.Fatalf("node tie order = %v", got)
	}
	if entries[0].index != 1 || entries[1].index != 2 {
		t.Fatalf("index tie order = %d, %d; want 1, 2", entries[0].index, entries[1].index)
	}
	if bytes.Compare(entries[0].token[:], token[:]) != 0 {
		t.Fatal("sort changed token")
	}
}

func mustRing(t *testing.T, virtualNodes int, nodes ...string) *Ring {
	t.Helper()
	ring, err := New(virtualNodes)
	if err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range nodes {
		if err := ring.Add(nodeID); err != nil {
			t.Fatal(err)
		}
	}
	return ring
}
