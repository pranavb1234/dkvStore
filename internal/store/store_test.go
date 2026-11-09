package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestStorePutGetDelete(t *testing.T) {
	store := New(16)
	created, err := store.Put("course", "go")
	if err != nil || !created {
		t.Fatalf("first Put = (%v, %v), want (true, nil)", created, err)
	}
	created, err = store.Put("course", "distributed")
	if err != nil || created {
		t.Fatalf("replacement Put = (%v, %v), want (false, nil)", created, err)
	}
	value, err := store.Get("course")
	if err != nil || value != "distributed" {
		t.Fatalf("Get = (%q, %v), want (distributed, nil)", value, err)
	}
	if !store.Delete("course") {
		t.Fatal("Delete returned false for an existing key")
	}
	if store.Delete("course") {
		t.Fatal("Delete returned true for a missing key")
	}
	if _, err := store.Get("course"); err != ErrNotFound {
		t.Fatalf("Get missing error = %v, want %v", err, ErrNotFound)
	}
}

func TestStoreRejectsOversizedValue(t *testing.T) {
	store := New(3)
	if _, err := store.Put("key", "four"); err != ErrValueTooLarge {
		t.Fatalf("Put error = %v, want %v", err, ErrValueTooLarge)
	}
}

func TestStoreAppliesReplicaVersionsIdempotently(t *testing.T) {
	store := New(16)
	applied, err := store.Apply("key", "first", 2)
	if err != nil || !applied {
		t.Fatalf("Apply newer = (%v, %v), want (true, nil)", applied, err)
	}
	applied, err = store.Apply("key", "first", 2)
	if err != nil || applied {
		t.Fatalf("Apply duplicate = (%v, %v), want (false, nil)", applied, err)
	}
	if _, err := store.Apply("key", "stale", 1); err != ErrStaleVersion {
		t.Fatalf("Apply stale error = %v, want %v", err, ErrStaleVersion)
	}
	if _, err := store.Apply("key", "conflict", 2); err != ErrVersionConflict {
		t.Fatalf("Apply conflict error = %v, want %v", err, ErrVersionConflict)
	}
	record, err := store.GetRecord("key")
	if err != nil || record != (Record{Value: "first", Version: 2}) {
		t.Fatalf("GetRecord = (%+v, %v)", record, err)
	}
}

func TestStoreConcurrentReadsAndWrites(t *testing.T) {
	store := New(1024)
	const workers = 16
	const operations = 200

	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for operation := 0; operation < operations; operation++ {
				key := fmt.Sprintf("key-%d", operation%8)
				if _, err := store.Put(key, fmt.Sprintf("worker-%d", worker)); err != nil {
					t.Errorf("Put: %v", err)
				}
				if _, err := store.Get(key); err != nil {
					t.Errorf("Get: %v", err)
				}
			}
		}(worker)
	}
	group.Wait()
}
