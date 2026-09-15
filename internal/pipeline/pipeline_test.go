package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/eskiconce/smpp-gateway/internal/billing"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type testRouter struct{ id int }

func (t *testRouter) Route(_ context.Context, in router.RouteInput) (router.RouteResult, error) {
	return router.RouteResult{Connectors: []int{t.id}}, nil
}

type fixedRouter struct {
	id  int
	ids []int
}

func (f *fixedRouter) Route(_ context.Context, _ router.RouteInput) (router.RouteResult, error) {
	if f.ids != nil {
		return router.RouteResult{Connectors: f.ids}, nil
	}
	return router.RouteResult{Connectors: []int{f.id}}, nil
}

func TestSubmitRoutesWithTagAndRouteID(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	q := queue.NewMemory()
	go q.Consume(ctx, queue.Key(7, 0), "g", func(it queue.Item) error { return nil })

	p := NewPipeline(repo, q, &testRouter{id: 7})
	msgID, segs, err := p.Submit(ctx, Outgoing{
		TenantID: "t1", SourceAddr: "shield", Msisdn: "569123",
		Text: "hola [PROMO] mundo", Priority: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if segs != 1 {
		t.Fatalf("segs=%d", segs)
	}
	m, _ := repo.GetMessage(ctx, msgID)
	if m == nil || m.State != "enqueued" || m.ConnectorID != 7 || m.Text != "hola  mundo" {
		t.Fatalf("msg=%+v (se esperaba texto sin [PROMO])", m)
	}
}

type fakeBiller struct {
	price      float64
	reserveErr error
	priceErr   error
	reserved   bool
}

func (f *fakeBiller) Price(context.Context, string, int, string, int) (float64, error) {
	return f.price, f.priceErr
}
func (f *fakeBiller) Reserve(_ context.Context, _ string, _ float64) error {
	f.reserved = true
	return f.reserveErr
}

func TestSubmitConBilling(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	q := queue.NewMemory()
	b := &fakeBiller{price: 0.5}
	p := NewPipeline(repo, q, &fixedRouter{id: 3}, WithBiller(b))

	msgID, segs, err := p.Submit(ctx, Outgoing{TenantID: "t1", Msisdn: "569123", Text: "hola"})
	if err != nil {
		t.Fatal(err)
	}
	if segs != 1 {
		t.Fatalf("segments=%d", segs)
	}
	m, _ := repo.GetMessage(ctx, msgID)
	if m.Amount != 0.5 || m.ConnectorID != 3 {
		t.Fatalf("monto=%v conn=%d", m.Amount, m.ConnectorID)
	}
	if !b.reserved {
		t.Fatal("Reserve no fue llamado")
	}
	var item queue.Item
	done := make(chan struct{})
	go q.Consume(ctx, queue.Key(3, 0), "g", func(x queue.Item) error { item = x; close(done); return nil })
	<-done
	if item.Amount != 0.5 {
		t.Fatalf("item amount=%v", item.Amount)
	}
}

func TestSubmitSaldoInsuficiente(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	q := queue.NewMemory()
	p := NewPipeline(repo, q, &fixedRouter{id: 3},
		WithBiller(&fakeBiller{price: 0.5, reserveErr: billing.ErrInsufficientBalance}))

	if _, _, err := p.Submit(ctx, Outgoing{TenantID: "t1", Msisdn: "569123", Text: "hola"}); !errors.Is(err, billing.ErrInsufficientBalance) {
		t.Fatalf("esperaba ErrInsufficientBalance, got %v", err)
	}
}
