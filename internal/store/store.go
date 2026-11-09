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
)

// DefaultMaxValueBytes is the maximum accepted local value size (1 MiB).
const DefaultMaxValueBytes = 1 << 20

// Store is an in-memory key-value store. Its contents are lost on restart.
type Store struct {
	mu            sync.RWMutex
	values        map[string]string
	maxValueBytes int
}

// New creates an empty Store that accepts values up to maxValueBytes.
func New(maxValueBytes int) *Store {
	return &Store{
		values:        make(map[string]string),
		maxValueBytes: maxValueBytes,
	}
}

// Put stores value under key and reports whether the key was newly created.
func (s *Store) Put(key, value string) (bool, error) {
	if len(value) > s.maxValueBytes {
		return false, ErrValueTooLarge
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.values[key]
	s.values[key] = value
	return !exists, nil
}

// Get returns the value stored under key.
func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.values[key]
	if !exists {
		return "", ErrNotFound
	}
	return value, nil
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
