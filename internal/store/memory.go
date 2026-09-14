package store

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("not found")

type MemoryRepo struct {
	mu       sync.Mutex
	msgs     map[string]*Message
	hooks    []Webhook
	nextHook int
}

func NewMemory() *MemoryRepo {
	return &MemoryRepo{msgs: map[string]*Message{}}
}

func (r *MemoryRepo) CreateMessage(_ context.Context, m *Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := *m
	cp.CreatedAt = time.Now()
	cp.UpdatedAt = cp.CreatedAt
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
	m.UpdatedAt = time.Now()
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
	m.State = "accepted"
	m.UpdatedAt = time.Now()
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

func (r *MemoryRepo) IncrementTry(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.msgs[id]
	if !ok {
		return ErrNotFound
	}
	m.TryCount++
	return nil
}

func (r *MemoryRepo) SetConnector(_ context.Context, id string, connectorID int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.msgs[id]
	if !ok {
		return ErrNotFound
	}
	m.ConnectorID = connectorID
	return nil
}

func (r *MemoryRepo) ListStaleAccepted(_ context.Context, before time.Time) ([]Message, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Message
	for _, m := range r.msgs {
		if m.State == "accepted" && m.UpdatedAt.Before(before) {
			out = append(out, *m)
		}
	}
	return out, nil
}

func (r *MemoryRepo) ListWebhooks(_ context.Context, tenantID string) ([]Webhook, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Webhook
	for _, w := range r.hooks {
		if w.TenantID == tenantID {
			out = append(out, w)
		}
	}
	return out, nil
}

func (r *MemoryRepo) ListActiveByEvent(_ context.Context, tenantID, event string) ([]Webhook, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Webhook
	for _, w := range r.hooks {
		if w.TenantID == tenantID && w.Active {
			for _, e := range w.Events {
				if e == event {
					out = append(out, w)
					break
				}
			}
		}
	}
	return out, nil
}

func (r *MemoryRepo) CreateWebhook(_ context.Context, w *Webhook) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextHook++
	w.ID = r.nextHook
	w.CreatedAt = time.Now()
	r.hooks = append(r.hooks, *w)
	return nil
}

func (r *MemoryRepo) UpdateWebhook(_ context.Context, w Webhook) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.hooks {
		if r.hooks[i].ID == w.ID {
			r.hooks[i] = w
			return nil
		}
	}
	return nil
}

func (r *MemoryRepo) DeleteWebhook(_ context.Context, id int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.hooks {
		if r.hooks[i].ID == id {
			r.hooks = append(r.hooks[:i], r.hooks[i+1:]...)
			return nil
		}
	}
	return nil
}
