// internal/queue/memory.go — REEMPLAZO dictado por ruling del controlador
//
// El brief original llamaba a Enqueue ANTES de que Consume registrara el
// handler, pero su MemoryQueue devolvía error "sin consumidor" en ese caso:
// el propio test del plan (TestMemoryQueueRoundTrip) no podría pasar.
// Ruling: Enqueue, si no hay handler registrado, bufferiza el item en el
// stream; Consume lo entrega al registrarse (semántica idéntica al
// XGROUP CREATE ... 0 del RedisQueue: se lee lo pre-existente). Si ya hay
// handler, handoff directo. Determinista y sin carreras (mutex).
package queue

import (
	"context"
	"sync"
)

type memKey struct {
	pending []Item
	handler func(Item) error
}

type MemoryQueue struct {
	mu   sync.Mutex
	keys map[string]*memKey
}

func NewMemory() *MemoryQueue {
	return &MemoryQueue{keys: map[string]*memKey{}}
}

func (q *MemoryQueue) stream(key string) *memKey {
	k, ok := q.keys[key]
	if !ok {
		k = &memKey{}
		q.keys[key] = k
	}
	return k
}

func (q *MemoryQueue) Enqueue(_ context.Context, key string, it Item) error {
	q.mu.Lock()
	k := q.stream(key)
	if k.handler == nil {
		k.pending = append(k.pending, it)
		q.mu.Unlock()
		return nil
	}
	h := k.handler
	q.mu.Unlock()
	return h(it)
}

func (q *MemoryQueue) Consume(_ context.Context, key, _ string, fn func(Item) error) error {
	q.mu.Lock()
	k := q.stream(key)
	k.handler = fn
	pending := k.pending
	k.pending = nil
	q.mu.Unlock()

	for _, it := range pending {
		if err := fn(it); err != nil {
			return err
		}
	}
	<-make(chan struct{})
	return nil
}

func (q *MemoryQueue) Ack(_ context.Context, _, _, _ string) error { return nil }
