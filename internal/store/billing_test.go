package store

import (
	"context"
	"testing"
	"time"
)

func TestBillingMemory(t *testing.T) {
	ctx := context.Background()
	repo := NewMemory()

	if err := repo.CreateTenant(ctx, &Tenant{
		ID: "t1", Name: "kiki", Status: "active", Balance: 100, Mode: "prepaid", ApiKey: "key-t1",
	}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetTenant(ctx, "t1")
	if err != nil || got.Balance != 100 || got.Mode != "prepaid" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	byKey, err := repo.GetTenantByAPIKey(ctx, "key-t1")
	if err != nil || byKey.ID != "t1" {
		t.Fatalf("byKey=%+v err=%v", byKey, err)
	}
	if _, err := repo.GetTenantByAPIKey(ctx, "nada"); err == nil {
		t.Fatal("api key desconocida debio fallar")
	}

	tbl := &RateTable{TenantID: "t1", Name: "estandar", Active: true}
	if err := repo.CreateRateTable(ctx, tbl); err != nil {
		t.Fatal(err)
	}
	if tbl.ID == 0 {
		t.Fatal("sin id de tabla")
	}
	if err := repo.CreateRateEntry(ctx, &RateEntry{TableID: tbl.ID, Prefix: "569", Price: 0.2500}); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateRateEntry(ctx, &RateEntry{TableID: tbl.ID, Prefix: "", Price: 0.5000, ConnectorID: 7}); err != nil {
		t.Fatal(err)
	}
	active, err := repo.GetActiveRateTable(ctx, "t1")
	if err != nil || active.ID != tbl.ID {
		t.Fatalf("active=%+v err=%v", active, err)
	}
	entries, err := repo.ListRateEntries(ctx, tbl.ID)
	if err != nil || len(entries) != 2 {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
	if entries[0].Price != 0.25 || entries[1].ConnectorID != 7 {
		t.Fatalf("entries[0]=%+v entries[1]=%+v", entries[0], entries[1])
	}

	bal, err := repo.Debit(ctx, "t1", "m-1", 10)
	if err != nil || bal != 90 {
		t.Fatalf("debit bal=%v err=%v", bal, err)
	}
	_, err = repo.Debit(ctx, "t1", "m-1", 10)
	if err != nil {
		t.Fatalf("segundo debit no debio fallar: %v", err)
	}
	t2, _ := repo.GetTenant(ctx, "t1")
	if t2.Balance != 90 {
		t.Fatalf("debit doble: %v", t2.Balance)
	}
	balCredit, err := repo.Credit(ctx, "t1", 5)
	if err != nil || balCredit != 95 {
		t.Fatalf("credit bal=%v err=%v", balCredit, err)
	}
	txns, err := repo.ListTransactions(ctx, "t1", 10)
	if err != nil || len(txns) != 2 {
		t.Fatalf("txns=%+v err=%v", txns, err)
	}
	if txns[0].Type != "credit" || txns[0].Amount != 5 || txns[0].ResultBalance != 95 {
		t.Fatalf("txns[0]=%+v", txns[0])
	}
	if txns[1].Type != "debit" || txns[1].Amount != 10 || txns[1].ResultBalance != 90 {
		t.Fatalf("txns[1]=%+v", txns[1])
	}
}

func TestRateEntryWithVigencia(t *testing.T) {
	ctx := context.Background()
	repo := NewMemory()
	repo.CreateTenant(ctx, &Tenant{ID: "t1", Status: "active"})
	tbl := &RateTable{TenantID: "t1", Name: "abc", Active: true}
	repo.CreateRateTable(ctx, tbl)
	future := time.Now().Add(24 * time.Hour)
	if err := repo.CreateRateEntry(ctx, &RateEntry{TableID: tbl.ID, Prefix: "569", Price: 1, ValidFrom: &future}); err != nil {
		t.Fatal(err)
	}
	entries, _ := repo.ListRateEntries(ctx, tbl.ID)
	if len(entries) != 1 || entries[0].ValidFrom == nil {
		t.Fatalf("entries=%+v", entries)
	}
}
