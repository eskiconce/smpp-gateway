package store

import (
	"context"
	"testing"
)

func TestWebhookRepoMemory(t *testing.T) {
	ctx := context.Background()
	repo := NewMemory()

	w := &Webhook{TenantID: "t1", URL: "https://cli.example.com/hook", AuthToken: "abc",
		Events: []string{"delivered", "undeliv"}, Active: true}
	if err := repo.CreateWebhook(ctx, w); err != nil {
		t.Fatal(err)
	}
	if w.ID == 0 {
		t.Fatal("sin id asignado")
	}

	got, err := repo.ListActiveByEvent(ctx, "t1", "delivered")
	if err != nil || len(got) != 1 {
		t.Fatalf("active=%+v err=%v", got, err)
	}
	if n, _ := repo.ListActiveByEvent(ctx, "t1", "expired"); len(n) != 0 {
		t.Fatalf("no debia matchear expired: %+v", n)
	}
	if n, _ := repo.ListActiveByEvent(ctx, "t2", "delivered"); len(n) != 0 {
		t.Fatalf("tenant distinto no debia matchear: %+v", n)
	}

	w.Active = false
	if err := repo.UpdateWebhook(ctx, *w); err != nil {
		t.Fatal(err)
	}
	if n, _ := repo.ListActiveByEvent(ctx, "t1", "delivered"); len(n) != 0 {
		t.Fatalf("inactivo no debia aparecer: %+v", n)
	}
	all, err := repo.ListWebhooks(ctx, "t1")
	if err != nil || len(all) != 1 || all[0].Active {
		t.Fatalf("all=%+v err=%v", all, err)
	}
	if err := repo.DeleteWebhook(ctx, w.ID); err != nil {
		t.Fatal(err)
	}
	if all, _ := repo.ListWebhooks(ctx, "t1"); len(all) != 0 {
		t.Fatalf("no borro: %+v", all)
	}
}
