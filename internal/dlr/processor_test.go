package dlr

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/store"
)

type recNotifier struct {
	mu  sync.Mutex
	evs []Event
}

func (r *recNotifier) Notify(_ context.Context, ev Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, ev)
	return nil
}

func (r *recNotifier) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.evs)
}

func (r *recNotifier) last() Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.evs[len(r.evs)-1]
}

func newTestProcessor(t *testing.T) (*Processor, *store.MemoryRepo, *recNotifier) {
	t.Helper()
	repo := store.NewMemory()
	n := &recNotifier{}
	if err := repo.CreateMessage(context.Background(), &store.Message{
		ID: "m1", TenantID: "t1", Msisdn: "569123", Text: "hola", State: "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	return NewProcessor(NewMemCache(), repo, n), repo, n
}

func TestHandleDelivered(t *testing.T) {
	p, repo, n := newTestProcessor(t)
	ctx := context.Background()
	if err := p.Register(ctx, "smsc-1", "m1"); err != nil {
		t.Fatal(err)
	}
	if err := p.Handle(ctx, "smsc-1", "DELIVRD"); err != nil {
		t.Fatal(err)
	}
	m, _ := repo.GetMessage(ctx, "m1")
	if m.State != "delivered" {
		t.Fatalf("state=%q", m.State)
	}
	deadline := time.Now().Add(time.Second)
	for n.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if ev := n.last(); ev.State != "delivered" || ev.MessageID != "m1" || ev.SmscMsgid != "smsc-1" {
		t.Fatalf("event=%+v", ev)
	}
}

func TestHandleUndelivAndExpired(t *testing.T) {
	p, repo, n := newTestProcessor(t)
	ctx := context.Background()
	if err := p.Register(ctx, "s1", "m1"); err != nil {
		t.Fatal(err)
	}
	if err := p.Handle(ctx, "s1", "UNDELIV"); err != nil {
		t.Fatal(err)
	}
	m, _ := repo.GetMessage(ctx, "m1")
	if m.State != "undeliv" {
		t.Fatalf("state=%q", m.State)
	}
	deadline := time.Now().Add(time.Second)
	for n.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.last().State != "undeliv" {
		t.Fatalf("event=%+v", n.last())
	}
}

func TestHandleNonFinalKeepsAccepted(t *testing.T) {
	p, repo, n := newTestProcessor(t)
	ctx := context.Background()
	if err := p.Register(ctx, "s1", "m1"); err != nil {
		t.Fatal(err)
	}
	if err := p.Handle(ctx, "s1", "ENROUTE"); err != nil {
		t.Fatal(err)
	}
	m, _ := repo.GetMessage(ctx, "m1")
	if m.State != "accepted" {
		t.Fatalf("state=%q", m.State)
	}
	if n.count() != 0 {
		t.Fatalf("no debia notificar")
	}
}

func TestHandleWithoutCorrelationIsNoop(t *testing.T) {
	p, repo, n := newTestProcessor(t)
	ctx := context.Background()
	if err := p.Handle(ctx, "smsc-desconocido", "DELIVRD"); err != nil {
		t.Fatal(err)
	}
	m, _ := repo.GetMessage(ctx, "m1")
	if m.State != "accepted" || n.count() != 0 {
		t.Fatalf("state=%q n=%d", m.State, n.count())
	}
}
