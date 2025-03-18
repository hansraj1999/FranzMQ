package mem_key_generator

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("franzmq_mem_manager")

// SafeMap struct to handle concurrent updates with better performance
type SafeMap struct {
	data  sync.Map // Stores key-value pairs
	locks sync.Map // Per-key locks
}

// NewSafeMap initializes the SafeMap
func NewSafeMap() *SafeMap {
	s := &SafeMap{}
	return s
}

// Get retrieves a value by key
func (s *SafeMap) Get(ctx context.Context, key string) (int, bool) {
	ctx, span := tracer.Start(ctx, "Get")
	defer span.End()
	val, exists := s.data.Load(key)
	if !exists {
		return 0, false
	}
	return val.(int), true
}

// INCR atomically increments the value for a key
func (s *SafeMap) INCR(ctx context.Context, key string) int {
	ctx, span := tracer.Start(ctx, "INCR")
	defer span.End()

	lock := s.getLock(ctx, key)
	lock.Lock()
	defer lock.Unlock()

	val, _ := s.Get(ctx, key)
	newVal := val + 1
	s.data.Store(key, newVal)
	return newVal
}

// INCRBY atomically increments the value for a key by a given amount
func (s *SafeMap) INCRBY(ctx context.Context, key string, value int) int {
	ctx, span := tracer.Start(ctx, "INCRBY")
	defer span.End()

	lock := s.getLock(ctx, key)
	lock.Lock()
	defer lock.Unlock()

	val, _ := s.Get(ctx, key)
	newVal := val + value
	s.data.Store(key, newVal)
	return newVal
}

// getLock retrieves or initializes a lock for a key
func (s *SafeMap) getLock(ctx context.Context, key string) *sync.Mutex {
	ctx, span := tracer.Start(ctx, "getLock")
	defer span.End()

	actual, _ := s.locks.LoadOrStore(key, &sync.Mutex{})
	return actual.(*sync.Mutex)
}
