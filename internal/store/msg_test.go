package store

import (
	"context"
	"testing"
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
