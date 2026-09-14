package dlr

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/store"
)

type countNotifier struct {
	mu  sync.Mutex
	cnt int
	evs []Event
}

func (c *countNotifier) Notify(_ context.Context, ev Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cnt++
	c.evs = append(c.evs, ev)
	return nil
}

func (c *countNotifier) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cnt
}

func TestReconcileOnceMarksExpired(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()

	old := &store.Message{ID: "m1", TenantID: "t1", Msisdn: "5691", Text: "a", SmscMsgid: "smsc-9", State: "accepted"}
	if err := repo.CreateMessage(ctx, old); err != nil {
		t.Fatal(err)
	}
	repo.BackdoorSetUpdatedAt("m1", time.Now().Add(-time.Hour))

	fresh := &store.Message{ID: "m2", TenantID: "t1", Msisdn: "5692", Text: "b", State: "accepted"}
	if err := repo.CreateMessage(ctx, fresh); err != nil {
		t.Fatal(err)
	}

	n := &countNotifier{}
	r := NewReconciler(repo, n, 10*time.Minute, time.Minute)
	count, err := r.ReconcileOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("count=%d", count)
	}
	m, _ := repo.GetMessage(ctx, "m1")
	if m.State != "expired" {
		t.Fatalf("state=%q", m.State)
	}
	if m2, _ := repo.GetMessage(ctx, "m2"); m2.State != "accepted" {
		t.Fatalf("m2 no debia tocarse: %q", m2.State)
	}
	deadline := time.Now().Add(time.Second)
	for n.total() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.total() != 1 || n.evs[0].State != "expired" || n.evs[0].MessageID != "m1" {
		t.Fatalf("notificado=%d evs=%+v", n.total(), n.evs)
	}
}
