package worker

import (
	"context"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/session"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/smscsim"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

func noBackoff(int) time.Duration { return 0 }

func portOf2(addr string) int {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(portStr)
	return n
}

func dialSess(t *testing.T, h session.Handler, simAddr string) *session.Session {
	t.Helper()
	s := session.New(session.Config{
		Host: "127.0.0.1", Port: portOf2(simAddr), SystemID: "esp", Password: "secreto",
		SourceAddr: "shield", MsgPerSecond: 100, MaxConcurrency: 1,
	}, h)
	if err := s.Dial(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestWorkerDeliveredHappyPath(t *testing.T) {
	sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	defer sim.Close()

	repo := store.NewMemory()
	q := queue.NewMemory()
	w := NewWorker(q, repo, WithBackoff(noBackoff))
	sess := dialSess(t, w, sim.Addr())
	w.SetSession(sess)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go q.Consume(ctx, queue.Key(1, 0), "g", func(it queue.Item) error { return w.Handle(ctx, it) })

	p := pipeline.NewPipeline(repo, q, &fixedRouter{ids: []int{1}})
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
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no llego a delivered")
}

func TestWorkerFallbackOnReject(t *testing.T) {
	simA := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
		RespondSubmitStatus: smpp.ESME_RSYSERR})
	if err := simA.Start(); err != nil {
		t.Fatal(err)
	}
	defer simA.Close()
	simB := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
	if err := simB.Start(); err != nil {
		t.Fatal(err)
	}
	defer simB.Close()

	repo := store.NewMemory()
	q := queue.NewMemory()

	wA := NewWorker(q, repo, WithBackoff(noBackoff))
	wA.SetSession(dialSess(t, wA, simA.Addr()))
	wB := NewWorker(q, repo, WithBackoff(noBackoff))
	wB.SetSession(dialSess(t, wB, simB.Addr()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go q.Consume(ctx, queue.Key(1, 0), "g", func(it queue.Item) error { return wA.Handle(ctx, it) })
	go q.Consume(ctx, queue.Key(2, 0), "g", func(it queue.Item) error { return wB.Handle(ctx, it) })

	r := router.New(defaultStubRules, router.Config{Rand: rand.New(rand.NewSource(2))})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}

	p := pipeline.NewPipeline(repo, q, r)
	msgID, _, err := p.Submit(ctx, pipeline.Outgoing{
		TenantID: "t1", SourceAddr: "shield", Msisdn: "569123", Text: "hola", Priority: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m, err := repo.GetMessage(ctx, msgID)
		if err != nil {
			continue
		}
		if m.State == "delivered" {
			if m.ConnectorID != 2 {
				t.Fatalf("conector final=%d, esperaba 2", m.ConnectorID)
			}
			if m.TryCount != 2 {
				t.Fatalf("try_count=%d, esperaba 2 (reintento)", m.TryCount)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("fallback no entrego por B")
}

func TestWorkerExhaustRejected(t *testing.T) {
	simA := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
		RespondSubmitStatus: smpp.ESME_RSYSERR})
	if err := simA.Start(); err != nil {
		t.Fatal(err)
	}
	defer simA.Close()

	repo := store.NewMemory()
	q := queue.NewMemory()
	wA := NewWorker(q, repo, WithBackoff(noBackoff))
	wA.SetSession(dialSess(t, wA, simA.Addr()))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go q.Consume(ctx, queue.Key(1, 0), "g", func(it queue.Item) error { return wA.Handle(ctx, it) })

	p := pipeline.NewPipeline(repo, q, &fixedRouter{ids: []int{1}})
	msgID, _, err := p.Submit(ctx, pipeline.Outgoing{
		TenantID: "t1", SourceAddr: "shield", Msisdn: "569123", Text: "hola", Priority: 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m, _ := repo.GetMessage(ctx, msgID)
		if m != nil && m.State == "rejected" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no quedo rejected")
}

func TestWorkerFallbackOnTransport(t *testing.T) {
	simB := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
	if err := simB.Start(); err != nil {
		t.Fatal(err)
	}
	defer simB.Close()

	repo := store.NewMemory()
	q := queue.NewMemory()

	wA := NewWorker(q, repo, WithBackoff(noBackoff))
	sA := session.New(session.Config{Host: "127.0.0.1", Port: 1,
		SystemID: "esp", Password: "secreto", SourceAddr: "shield",
		MsgPerSecond: 100, MaxConcurrency: 1}, wA)
	sA.Close()
	wA.SetSession(sA)

	wB := NewWorker(q, repo, WithBackoff(noBackoff))
	wB.SetSession(dialSess(t, wB, simB.Addr()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go q.Consume(ctx, queue.Key(1, 0), "g", func(it queue.Item) error { return wA.Handle(ctx, it) })
	go q.Consume(ctx, queue.Key(2, 0), "g", func(it queue.Item) error { return wB.Handle(ctx, it) })

	r := router.New(defaultStubRules, router.Config{Rand: rand.New(rand.NewSource(2))})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}

	p := pipeline.NewPipeline(repo, q, r)
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
			if m.TryCount != 2 {
				t.Fatalf("try_count=%d", m.TryCount)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("transporte: fallback no entrego por B")
}

type fixedRouter struct{ ids []int }

func (f *fixedRouter) Route(_ context.Context, in router.RouteInput) (router.RouteResult, error) {
	return router.RouteResult{RuleID: 1, Connectors: f.ids}, nil
}

type stubRules struct {
	rules  []router.Rule
	groups []router.Group
}

var defaultStubRules = &stubRules{
	rules: []router.Rule{{ID: 1, Priority: 1, GroupID: 3}},
	groups: []router.Group{{
		ID: 3, Name: "ops",
		Members: []router.GroupMember{{ConnectorID: 1, Weight: 1}, {ConnectorID: 2, Weight: 1}},
	}},
}

func (s *stubRules) ListRoutingRules(context.Context) ([]router.Rule, error) { return s.rules, nil }
func (s *stubRules) ListGroups(context.Context) ([]router.Group, error)      { return s.groups, nil }
