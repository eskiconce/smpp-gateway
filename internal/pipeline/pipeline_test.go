package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type testRouter struct{ id int }

func (t *testRouter) Route(_ context.Context, _ router.RouteInput) (router.RouteResult, error) {
	return router.RouteResult{RuleID: 1, Connectors: []int{t.id}}, nil
}

func TestSubmitEndToEndPipeline(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	q := queue.NewMemory()
	key := "stream:con:7:0"
	go q.Consume(ctx, key, "g", func(it queue.Item) error { return nil })
	time.Sleep(time.Millisecond)

	p := NewPipeline(repo, q, &testRouter{id: 7})
	msgID, segs, err := p.Submit(ctx, Outgoing{
		TenantID: "t1", SourceAddr: "shield", Msisdn: "569123", Text: "hola", RoutingTag: "", Priority: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if msgID == "" {
		t.Fatal("msgID vacio")
	}
	_ = segs

	m, err := repo.GetMessage(ctx, msgID)
	if err != nil {
		t.Fatal(err)
	}
	if m.State != "enqueued" || m.ConnectorID != 7 || m.TenantID != "t1" {
		t.Fatalf("msg %+v", m)
	}
}
