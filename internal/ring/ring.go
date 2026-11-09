// Package ring implements deterministic consistent-hash key placement.
package ring

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"sort"
	"strconv"
	"strings"
)

var (
	// ErrInvalidVirtualNodes reports a non-positive virtual-node count.
	ErrInvalidVirtualNodes = errors.New("virtual node count must be positive")
	// ErrEmptyNodeID reports an unusable physical-node identity.
	ErrEmptyNodeID = errors.New("node id must not be empty")
	// ErrNodeExists reports an attempt to add an existing physical node.
	ErrNodeExists = errors.New("node already exists")
)

type entry struct {
	token  [sha256.Size]byte
	nodeID string
	index  int
}

// Ring maps keys to physical nodes using a fixed number of virtual nodes per
// physical node. It is intended to be built during configuration, before a
// node starts serving requests.
type Ring struct {
	virtualNodes int
	nodes        map[string]struct{}
	entries      []entry
}

// New creates an empty ring with virtualNodes entries per physical node.
func New(virtualNodes int) (*Ring, error) {
	if virtualNodes <= 0 {
		return nil, ErrInvalidVirtualNodes
	}
	return &Ring{
		virtualNodes: virtualNodes,
		nodes:        make(map[string]struct{}),
	}, nil
}

// Add adds a physical node and all of its virtual nodes to the ring.
func (r *Ring) Add(nodeID string) error {
	if strings.TrimSpace(nodeID) == "" {
		return ErrEmptyNodeID
	}
	if _, exists := r.nodes[nodeID]; exists {
		return ErrNodeExists
	}

	entries := make([]entry, 0, len(r.entries)+r.virtualNodes)
	entries = append(entries, r.entries...)
	for index := 0; index < r.virtualNodes; index++ {
		entries = append(entries, entry{
			token:  nodeToken(nodeID, index),
			nodeID: nodeID,
			index:  index,
		})
	}
	sortEntries(entries)
	r.entries = entries
	r.nodes[nodeID] = struct{}{}
	return nil
}

// Remove removes all virtual nodes belonging to nodeID. It reports whether the
// physical node was present.
func (r *Ring) Remove(nodeID string) bool {
	if _, exists := r.nodes[nodeID]; !exists {
		return false
	}
	entries := make([]entry, 0, len(r.entries)-r.virtualNodes)
	for _, candidate := range r.entries {
		if candidate.nodeID != nodeID {
			entries = append(entries, candidate)
		}
	}
	r.entries = entries
	delete(r.nodes, nodeID)
	return true
}

// Primary returns the first physical node clockwise from key's token.
func (r *Ring) Primary(key string) (string, bool) {
	if len(r.entries) == 0 {
		return "", false
	}
	return r.entries[r.entryIndex(keyToken(key))].nodeID, true
}

// Owners returns up to count distinct physical nodes clockwise from key's
// token. The primary is first; subsequent nodes are suitable replica targets.
func (r *Ring) Owners(key string, count int) []string {
	if count <= 0 || len(r.entries) == 0 {
		return nil
	}
	if count > len(r.nodes) {
		count = len(r.nodes)
	}

	owners := make([]string, 0, count)
	seen := make(map[string]struct{}, count)
	start := r.entryIndex(keyToken(key))
	for offset := 0; offset < len(r.entries) && len(owners) < count; offset++ {
		candidate := r.entries[(start+offset)%len(r.entries)].nodeID
		if _, exists := seen[candidate]; exists {
			continue
		}
		seen[candidate] = struct{}{}
		owners = append(owners, candidate)
	}
	return owners
}

// Nodes returns physical node IDs in lexical order.
func (r *Ring) Nodes() []string {
	nodes := make([]string, 0, len(r.nodes))
	for nodeID := range r.nodes {
		nodes = append(nodes, nodeID)
	}
	sort.Strings(nodes)
	return nodes
}

func (r *Ring) entryIndex(token [sha256.Size]byte) int {
	index := sort.Search(len(r.entries), func(index int) bool {
		return bytes.Compare(r.entries[index].token[:], token[:]) >= 0
	})
	if index == len(r.entries) {
		return 0
	}
	return index
}

func nodeToken(nodeID string, index int) [sha256.Size]byte {
	return sha256.Sum256([]byte("node\x00" + nodeID + "\x00" + strconv.Itoa(index)))
}

func keyToken(key string) [sha256.Size]byte {
	return sha256.Sum256([]byte("key\x00" + key))
}

func sortEntries(entries []entry) {
	sort.Slice(entries, func(left, right int) bool {
		comparison := bytes.Compare(entries[left].token[:], entries[right].token[:])
		if comparison != 0 {
			return comparison < 0
		}
		if entries[left].nodeID != entries[right].nodeID {
			return entries[left].nodeID < entries[right].nodeID
		}
		return entries[left].index < entries[right].index
	})
}
