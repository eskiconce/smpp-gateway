package pipeline

import (
	"context"
	"testing"

	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type testRouter struct{ id int }

func (t *testRouter) Route(_ context.Context, in router.RouteInput) (router.RouteResult, error) {
	return router.RouteResult{Connectors: []int{t.id}}, nil
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
