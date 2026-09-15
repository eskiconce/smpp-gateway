package store

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

var ErrNotFound = errors.New("not found")

type MemoryRepo struct {
	mu          sync.Mutex
	msgs        map[string]*Message
	hooks       []Webhook
	nextHook    int
	tenants     map[string]*Tenant
	rateTables  map[string][]*RateTable
	rateEntries map[int][]*RateEntry
	nextTable   int
	nextEntry   int
	transactions []Transaction
	nextTxnID   int64
	debitedMsg  map[string]bool
}

func NewMemory() *MemoryRepo {
	return &MemoryRepo{
		msgs:        make(map[string]*Message),
		tenants:     make(map[string]*Tenant),
		rateTables:  make(map[string][]*RateTable),
		rateEntries: make(map[int][]*RateEntry),
		debitedMsg:  make(map[string]bool),
	}
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

func (r *MemoryRepo) BackdoorSetUpdatedAt(id string, t time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m := r.msgs[id]; m != nil {
		m.UpdatedAt = t
	}
}

func (r *MemoryRepo) ListTenants(ctx context.Context) ([]Tenant, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Tenant
	for _, t := range r.tenants {
		out = append(out, *t)
	}
	return out, nil
}

func (r *MemoryRepo) GetTenant(_ context.Context, id string) (*Tenant, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t, ok := r.tenants[id]
	if !ok {
		return nil, ErrNotFound
	}
	c := *t
	return &c, nil
}

func (r *MemoryRepo) GetTenantByAPIKey(_ context.Context, apiKey string) (*Tenant, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range r.tenants {
		if t.ApiKey == apiKey {
			c := *t
			return &c, nil
		}
	}
	return nil, ErrNotFound
}

func (r *MemoryRepo) CreateTenant(_ context.Context, t *Tenant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if t.Mode == "" {
		t.Mode = "prepaid"
	}
	if t.Status == "" {
		t.Status = "active"
	}
	if _, dup := r.tenants[t.ID]; dup {
		return errors.New("store: tenant ya existe")
	}
	t.CreatedAt = time.Now()
	c := *t
	r.tenants[t.ID] = &c
	return nil
}

func (r *MemoryRepo) GetActiveRateTable(_ context.Context, tenantID string) (*RateTable, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, t := range r.rateTables[tenantID] {
		if t.Active {
			c := *t
			return &c, nil
		}
	}
	return nil, nil
}

func (r *MemoryRepo) ListRateTables(_ context.Context, tenantID string) ([]RateTable, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RateTable, 0, len(r.rateTables[tenantID]))
	for _, t := range r.rateTables[tenantID] {
		out = append(out, *t)
	}
	return out, nil
}

func (r *MemoryRepo) ListRateEntries(_ context.Context, tableID int) ([]RateEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries := r.rateEntries[tableID]
	out := make([]RateEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, *e)
	}
	return out, nil
}

func (r *MemoryRepo) CreateRateTable(_ context.Context, t *RateTable) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextTable++
	t.ID = r.nextTable
	t.CreatedAt = time.Now()
	c := *t
	r.rateTables[t.TenantID] = append(r.rateTables[t.TenantID], &c)
	return nil
}

func (r *MemoryRepo) CreateRateEntry(_ context.Context, e *RateEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextEntry++
	e.ID = r.nextEntry
	c := *e
	r.rateEntries[e.TableID] = append(r.rateEntries[e.TableID], &c)
	return nil
}

func (r *MemoryRepo) DeleteRateEntry(_ context.Context, id int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for tid, entries := range r.rateEntries {
		for i, e := range entries {
			if e.ID == id {
				r.rateEntries[tid] = append(entries[:i], entries[i+1:]...)
				return nil
			}
		}
	}
	return ErrNotFound
}

func (r *MemoryRepo) Debit(_ context.Context, tenantID, messageID string, amount float64) (float64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.debitedMsg[messageID] {
		t := r.tenants[tenantID]
		if t == nil {
			return 0, ErrNotFound
		}
		return t.Balance, nil
	}
	t := r.tenants[tenantID]
	if t == nil {
		return 0, ErrNotFound
	}
	t.Balance = round4(t.Balance - amount)
	r.debitedMsg[messageID] = true
	r.nextTxnID++
	r.transactions = append(r.transactions, Transaction{
		ID: r.nextTxnID, TenantID: tenantID, MessageID: messageID, Type: "debit",
		Amount: amount, ResultBalance: t.Balance, CreatedAt: time.Now(),
	})
	return t.Balance, nil
}

func (r *MemoryRepo) Credit(_ context.Context, tenantID string, amount float64) (float64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.tenants[tenantID]
	if t == nil {
		return 0, ErrNotFound
	}
	t.Balance = round4(t.Balance + amount)
	r.nextTxnID++
	r.transactions = append(r.transactions, Transaction{
		ID: r.nextTxnID, TenantID: tenantID, Type: "credit",
		Amount: amount, ResultBalance: t.Balance, CreatedAt: time.Now(),
	})
	return t.Balance, nil
}

func (r *MemoryRepo) ListTransactions(_ context.Context, tenantID string, limit int) ([]Transaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var out []Transaction
	for i := len(r.transactions) - 1; i >= 0 && len(out) < limit; i-- {
		if r.transactions[i].TenantID == tenantID {
			out = append(out, r.transactions[i])
		}
	}
	return out, nil
}

func round4(v float64) float64 {
	return math.Round(v*10000) / 10000
}
