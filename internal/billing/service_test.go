package billing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/store"
)

type fakeRates struct {
	table   *store.RateTable
	entries []store.RateEntry
}

func (f fakeRates) GetActiveRateTable(_ context.Context, tenantID string) (*store.RateTable, error) {
	if f.table == nil || f.table.TenantID != tenantID {
		return nil, nil
	}
	return f.table, nil
}
func (fakeRates) ListRateTables(context.Context, string) ([]store.RateTable, error) { return nil, nil }
func (f fakeRates) ListRateEntries(_ context.Context, _ int) ([]store.RateEntry, error) {
	return f.entries, nil
}
func (fakeRates) CreateRateTable(context.Context, *store.RateTable) error { return nil }
func (fakeRates) CreateRateEntry(context.Context, *store.RateEntry) error { return nil }
func (fakeRates) DeleteRateEntry(context.Context, int) error              { return nil }

func TestPriceLongestPrefix(t *testing.T) {
	r := fakeRates{table: &store.RateTable{ID: 1, TenantID: "t1", Active: true}, entries: []store.RateEntry{
		{Prefix: "56", Price: 0.1},
		{Prefix: "569", Price: 0.25, ConnectorID: 2},
		{Prefix: "", Price: 0.5, ConnectorID: 7},
	}}
	s := New(r, store.NewMemory(), store.NewMemory())

	got, err := s.Price(context.Background(), "t1", 2, "56912345678", 1)
	if err != nil || got != 0.25 {
		t.Fatalf("precio=%v err=%v (esperado 0.25)", got, err)
	}
	if got, _ := s.Price(context.Background(), "t1", 7, "56912345678", 1); got != 0.5 {
		t.Fatalf("conn7 precio=%v", got)
	}
	if got, _ := s.Price(context.Background(), "t1", 99, "56912345678", 2); got != 0.2 {
		t.Fatalf("multiparte precio=%v (esperado 0.2)", got)
	}
	if _, err := s.Price(context.Background(), "tx", 2, "5691", 1); !errors.Is(err, ErrNoRate) {
		t.Fatalf("sin tabla debio dar ErrNoRate, got %v", err)
	}
}

func TestPriceVigencia(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	r := fakeRates{table: &store.RateTable{ID: 1, TenantID: "t1", Active: true}, entries: []store.RateEntry{
		{Prefix: "569", Price: 0.9, ValidTo: &past},
		{Prefix: "569", Price: 0.3, ValidFrom: &past, ValidTo: &future},
		{Prefix: "", Price: 0.5},
	}}
	s := New(r, store.NewMemory(), store.NewMemory())
	got, err := s.Price(context.Background(), "t1", 1, "569123", 1)
	if err != nil || got != 0.3 {
		t.Fatalf("precio=%v err=%v (esperado 0.3)", got, err)
	}
}

func TestReserveSaldo(t *testing.T) {
	repo := store.NewMemory()
	repo.CreateTenant(context.Background(), &store.Tenant{
		ID: "pre", Balance: 10, Mode: "prepaid",
	})
	repo.CreateTenant(context.Background(), &store.Tenant{
		ID: "post", Balance: 0, Mode: "postpaid",
	})
	s := New(fakeRates{}, repo, repo)

	if err := s.Reserve(context.Background(), "pre", 10); err != nil {
		t.Fatalf("prepaid suficiente: %v", err)
	}
	if err := s.Reserve(context.Background(), "pre", 10.0001); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("prepaid insuficiente: got %v", err)
	}
	if err := s.Reserve(context.Background(), "post", 1000); err != nil {
		t.Fatalf("postpaid no se valida bancariamente: %v", err)
	}
}

func TestDebitCreditServicio(t *testing.T) {
	repo := store.NewMemory()
	repo.CreateTenant(context.Background(), &store.Tenant{ID: "t1", Balance: 20})
	s := New(store.NewMemory(), repo, repo)
	bal, err := s.Debit(context.Background(), "t1", "m1", 4)
	if err != nil || bal != 16 {
		t.Fatalf("bal=%v err=%v", bal, err)
	}
	if bal, _ := s.Credit(context.Background(), "t1", 2); bal != 18 {
		t.Fatalf("credit bal=%v", bal)
	}
}
