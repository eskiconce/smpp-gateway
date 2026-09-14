package worker

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/session"
	"github.com/eskiconce/smpp-gateway/internal/smscsim"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

func TestWorkerSendsAndTracksDLR(t *testing.T) {
	sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	defer sim.Close()

	_, port, err := net.SplitHostPort(sim.Addr())
	if err != nil {
		t.Fatal(err)
	}
	pn, _ := strconv.Atoi(port)

	repo := store.NewMemory()
	q := queue.NewMemory()
	w := NewWorker(q, repo)
	sess := session.New(session.Config{
		Host: "127.0.0.1", Port: pn, SystemID: "esp", Password: "secreto",
		SourceAddr: "shield", MsgPerSecond: 100, MaxConcurrency: 1,
	}, w)
	w.SetSession(sess) // el worker es el Handler de la sesión; se inyecta la sesión como Sender
	if err := sess.Dial(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := "stream:con:1:0"
	go q.Consume(ctx, key, "g", func(it queue.Item) error { return w.Handle(ctx, it) })

	p := pipeline.NewPipeline(repo, q, &fixedRouter{id: 1})
	msgID, _, err := p.Submit(ctx, pipeline.Outgoing{
		TenantID: "t1", SourceAddr: "shield", Msisdn: "569123", Text: "hola", Priority: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m, _ := repo.GetMessage(ctx, msgID)
		if m != nil && m.State == "delivered" {
			return // submit → resp → DLR → delivered
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("el mensaje no llego a delivered")
}

type fixedRouter struct{ id int }

func (f *fixedRouter) Route(ctx context.Context, tenantID, msisdn, tag string) (int, error) {
	return f.id, nil
}
