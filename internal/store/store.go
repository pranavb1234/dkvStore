// Package store provides concurrency-safe local key-value storage.
package store

import (
	"errors"
	"sync"
)

var (
	// ErrNotFound reports that a key has no local value.
	ErrNotFound = errors.New("key not found")
	// ErrValueTooLarge reports that a value exceeds the store's configured limit.
	ErrValueTooLarge = errors.New("value exceeds maximum size")
	// ErrStaleVersion reports a replica update older than its local record.
	ErrStaleVersion = errors.New("record version is stale")
	// ErrVersionConflict reports unequal values carrying the same version.
	ErrVersionConflict = errors.New("record version conflicts with local value")
	// ErrInvalidVersion reports a zero version in a replication request.
	ErrInvalidVersion = errors.New("record version must be positive")
)

// DefaultMaxValueBytes is the maximum accepted local value size (1 MiB).
const DefaultMaxValueBytes = 1 << 20

// Store is an in-memory key-value store. Its contents are lost on restart.
type Store struct {
	mu            sync.RWMutex
	values        map[string]Record
	maxValueBytes int
}

// Record is a local value with a primary-assigned per-key version.
type Record struct {
	Value   string
	Version uint64
}

// New creates an empty Store that accepts values up to maxValueBytes.
func New(maxValueBytes int) *Store {
	return &Store{
		values:        make(map[string]Record),
		maxValueBytes: maxValueBytes,
	}
}

// Put stores value under key and reports whether the key was newly created.
func (s *Store) Put(key, value string) (bool, error) {
	_, created, err := s.PutNext(key, value)
	return created, err
}

// PutNext stores value using the next version for key. It is for the primary
// node only; replicas must use Apply with the primary's assigned version.
func (s *Store) PutNext(key, value string) (Record, bool, error) {
	if len(value) > s.maxValueBytes {
		return Record{}, false, ErrValueTooLarge
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.values[key]
	record := Record{Value: value, Version: current.Version + 1}
	s.values[key] = record
	return record, !exists, nil
}

// Get returns the value stored under key.
func (s *Store) Get(key string) (string, error) {
	record, err := s.GetRecord(key)
	if err != nil {
		return "", err
	}
	return record.Value, nil
}

// GetRecord returns the value and version stored under key.
func (s *Store) GetRecord(key string) (Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, exists := s.values[key]
	if !exists {
		return Record{}, ErrNotFound
	}
	return record, nil
}

// Apply stores a primary-assigned version on a replica. An identical repeat is
// successful but reports applied=false; stale or conflicting records are not
// written.
func (s *Store) Apply(key, value string, version uint64) (bool, error) {
	if len(value) > s.maxValueBytes {
		return false, ErrValueTooLarge
	}
	if version == 0 {
		return false, ErrInvalidVersion
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.values[key]
	if exists {
		switch {
		case current.Version > version:
			return false, ErrStaleVersion
		case current.Version == version && current.Value == value:
			return false, nil
		case current.Version == version:
			return false, ErrVersionConflict
		}
	}
	s.values[key] = Record{Value: value, Version: version}
	return true, nil
}

// Delete removes key and reports whether it existed.
func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.values[key]; !exists {
		return false
	}
	delete(s.values, key)
	return true
}
