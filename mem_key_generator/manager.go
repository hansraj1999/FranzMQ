package mem_key_generator

import (
	"sync"
)

// SafeMap struct to handle concurrent updates with better performance
type SafeMap struct {
	data  sync.Map // Stores key-value pairs
	locks sync.Map // Per-key locks
}

// NewSafeMap initializes the SafeMap
func NewSafeMap() *SafeMap {
	s := &SafeMap{}
	// Start background cleanup for unused locks
	return s
}

// Get retrieves a value by key
func (s *SafeMap) Get(key string) (int, bool) {
	val, exists := s.data.Load(key)
	if !exists {
		return 0, false
	}
	return val.(int), true
}

// INCR atomically increments the value for a key
func (s *SafeMap) INCR(key string) int {
	lock := s.getLock(key)
	lock.Lock()
	defer lock.Unlock()

	val, _ := s.Get(key)
	newVal := val + 1
	s.data.Store(key, newVal)
	return newVal
}

// INCRBY atomically increments the value for a key by a given amount
func (s *SafeMap) INCRBY(key string, value int) int {
	lock := s.getLock(key)
	lock.Lock()
	defer lock.Unlock()

	val, _ := s.Get(key)
	newVal := val + value
	s.data.Store(key, newVal)
	return newVal
}

// getLock retrieves or initializes a lock for a key
func (s *SafeMap) getLock(key string) *sync.Mutex {
	actual, _ := s.locks.LoadOrStore(key, &sync.Mutex{})
	return actual.(*sync.Mutex)
}
