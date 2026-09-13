package store

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("not found")

type MemoryRepo struct {
	mu   sync.Mutex
	msgs map[string]*Message
}

func NewMemory() *MemoryRepo {
	return &MemoryRepo{msgs: map[string]*Message{}}
}

func (r *MemoryRepo) CreateMessage(_ context.Context, m *Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *m
	cp.CreatedAt = time.Now()
	r.msgs[m.ID] = &cp
	return nil
}

func (r *MemoryRepo) UpdateState(_ context.Context, id, state string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.msgs[id]
	if !ok {
		return ErrNotFound
	}
	m.State = state
	return nil
}

func (r *MemoryRepo) SetSmscMsgid(_ context.Context, id, smscMsgid string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.msgs[id]
	if !ok {
		return ErrNotFound
	}
	m.SmscMsgid = smscMsgid
	return nil
}

func (r *MemoryRepo) GetMessage(_ context.Context, id string) (*Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.msgs[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *m
	return &cp, nil
}
