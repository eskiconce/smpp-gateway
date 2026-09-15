package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMessageRepo(t *testing.T) {
	ctx := context.Background()
	repo := NewMemory()

	m := &Message{
		ID: "m1", TenantID: "t1", Msisdn: "56912345678",
		Text: "hola", Segments: 1, ConnectorID: 3, State: "buffered",
	}
	if err := repo.CreateMessage(ctx, m); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetSmscMsgid(ctx, "m1", "smsc-abc"); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateState(ctx, "m1", "accepted"); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetMessage(ctx, "m1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "accepted" || got.SmscMsgid != "smsc-abc" {
		t.Fatalf("got %+v", got)
	}
	if _, err := repo.GetMessage(ctx, "nope"); err == nil {
		t.Fatal("esperaba error para id inexistente")
	}
}

func TestMessageRepoRouteAndTries(t *testing.T) {
	ctx := context.Background()
	repo := NewMemory()

	m := &Message{
		ID: "m2", TenantID: "t1", Msisdn: "56912345678",
		Text: "hola", Segments: 1, ConnectorID: 3, RouteID: 7, State: "buffered",
	}
	if err := repo.CreateMessage(ctx, m); err != nil {
		t.Fatal(err)
	}

	if err := repo.IncrementTry(ctx, "m2"); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetConnector(ctx, "m2", 9); err != nil {
		t.Fatal(err)
	}

	got, err := repo.GetMessage(ctx, "m2")
	if err != nil {
		t.Fatal(err)
	}
	if got.RouteID != 7 || got.TryCount != 1 || got.ConnectorID != 9 {
		t.Fatalf("got %+v", got)
	}
}

func TestMessageUpdatedAtAndStale(t *testing.T) {
	ctx := context.Background()
	repo := NewMemory()

	old := &Message{ID: "m1", TenantID: "t1", Msisdn: "5691", Text: "a", State: "accepted"}
	if err := repo.CreateMessage(ctx, old); err != nil {
		t.Fatal(err)
	}
	fresh := &Message{ID: "m2", TenantID: "t1", Msisdn: "5692", Text: "b", State: "accepted"}
	if err := repo.CreateMessage(ctx, fresh); err != nil {
		t.Fatal(err)
	}

	// Create a second, earlier message and backdate it via UpdateState
	// by manually setting UpdatedAt in the repo's internal map
	repo.mu.Lock()
	m1 := repo.msgs["m1"]
	m1.UpdatedAt = time.Now().Add(-time.Hour)
	repo.mu.Unlock()

	stale, err := repo.ListStaleAccepted(ctx, time.Now().Add(-10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(stale) != 1 || stale[0].ID != "m1" {
		t.Fatalf("stale=%+v", stale)
	}

	if err := repo.UpdateState(ctx, "m1", "delivered"); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.GetMessage(ctx, "m1")
	if got.UpdatedAt.Before(time.Now().Add(-time.Minute)) {
		t.Fatalf("updated_at no avanzo: %v", got.UpdatedAt)
	}
}

func TestGetTenantBySMPPSystemID(t *testing.T) {
	repo := NewMemory()
	ctx := context.Background()
	if err := repo.CreateTenant(ctx, &Tenant{
		ID: "t1", Status: "active",
		SmppSystemID: "esme-01", SmppPassword: "pass",
	}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetTenantBySMPPSystemID(ctx, "esme-01")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "t1" || got.SmppPassword != "pass" {
		t.Fatalf("got=%+v", got)
	}
	if _, err := repo.GetTenantBySMPPSystemID(ctx, "no-existe"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("esperaba ErrNotFound, got %v", err)
	}
}

func TestMessageSourceChannel(t *testing.T) {
	repo := NewMemory()
	ctx := context.Background()
	m := &Message{ID: "m1", TenantID: "t1", Msisdn: "569", Text: "hola", SourceChannel: "smpp"}
	if err := repo.CreateMessage(ctx, m); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.GetMessage(ctx, "m1")
	if got.SourceChannel != "smpp" {
		t.Fatalf("source_channel=%q", got.SourceChannel)
	}
}
