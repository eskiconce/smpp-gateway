package worker

import (
	"context"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/dlr"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/session"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/smscsim"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

func noBackoff(int) time.Duration { return 0 }

type recNotifier struct {
	mu  sync.Mutex
	evs []dlr.Event
}

func (r *recNotifier) Notify(_ context.Context, ev dlr.Event) error {
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

func newProcWorker(t *testing.T, q queue.Queue, repo store.MessageRepo) (*Worker, *recNotifier, *dlr.Processor) {
	t.Helper()
	n := &recNotifier{}
	proc := dlr.NewProcessor(dlr.NewMemCache(), repo, n)
	return NewWorker(q, repo, WithBackoff(noBackoff), WithDLR(proc)), n, proc
}

func waitState(t *testing.T, repo store.MessageRepo, id, want string) *store.Message {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m, err := repo.GetMessage(context.Background(), id)
		if err == nil && m != nil && m.State == want {
			return m
		}
		time.Sleep(10 * time.Millisecond)
	}
	m, _ := repo.GetMessage(context.Background(), id)
	t.Fatalf("no llego a %s (state=%s)", want, m.State)
	return nil
}

func TestWorkerDeliveredHappyPath(t *testing.T) {
	sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	defer sim.Close()

	repo := store.NewMemory()
	q := queue.NewMemory()
	w, n, _ := newProcWorker(t, q, repo)
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

	m := waitState(t, repo, msgID, "delivered")
	if m.SmscMsgid == "" {
		t.Fatal("sin smsc_msgid persistido")
	}
	deadline := time.Now().Add(time.Second)
	for n.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.count() != 1 || n.evs[0].State != "delivered" {
		t.Fatalf("notificador: %d %+v", n.count(), n.evs)
	}
}

func TestWorkerDLRUndeliv(t *testing.T) {
	sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
		EnableDLR: true, DLRStatus: "UNDELIV"})
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	defer sim.Close()

	repo := store.NewMemory()
	q := queue.NewMemory()
	w, n, _ := newProcWorker(t, q, repo)
	w.SetSession(dialSess(t, w, sim.Addr()))

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

	waitState(t, repo, msgID, "undeliv")
	deadline := time.Now().Add(time.Second)
	for n.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n.count() != 1 || n.evs[0].State != "undeliv" {
		t.Fatalf("notificador: %d %+v", n.count(), n.evs)
	}
}

func TestWorkerDLRExpired(t *testing.T) {
	sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
		EnableDLR: true, DLRStatus: "EXPIRED"})
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	defer sim.Close()

	repo := store.NewMemory()
	q := queue.NewMemory()
	w, _, _ := newProcWorker(t, q, repo)
	w.SetSession(dialSess(t, w, sim.Addr()))

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
	waitState(t, repo, msgID, "expired")
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

	wA, _, _ := newProcWorker(t, q, repo)
	wA.SetSession(dialSess(t, wA, simA.Addr()))
	wB, _, _ := newProcWorker(t, q, repo)
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

	m := waitState(t, repo, msgID, "delivered")
	if m.ConnectorID != 2 || m.TryCount != 2 {
		t.Fatalf("conector=%d try=%d", m.ConnectorID, m.TryCount)
	}
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
	wA, _, _ := newProcWorker(t, q, repo)
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
	waitState(t, repo, msgID, "rejected")
}

func TestWorkerFallbackOnTransport(t *testing.T) {
	simB := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
	if err := simB.Start(); err != nil {
		t.Fatal(err)
	}
	defer simB.Close()

	repo := store.NewMemory()
	q := queue.NewMemory()

	wA, _, _ := newProcWorker(t, q, repo)
	sA := session.New(session.Config{Host: "127.0.0.1", Port: 1,
		SystemID: "esp", Password: "secreto", SourceAddr: "shield",
		MsgPerSecond: 100, MaxConcurrency: 1}, wA)
	sA.Close()
	wA.SetSession(sA)

	wB, _, _ := newProcWorker(t, q, repo)
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

	m := waitState(t, repo, msgID, "delivered")
	if m.TryCount != 2 {
		t.Fatalf("try=%d", m.TryCount)
	}
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

type billingStub struct{ price float64 }

func (f billingStub) Price(_ context.Context, _ string, _ int, _ string, _ int) (float64, error) {
	return f.price, nil
}
func (f billingStub) Reserve(context.Context, string, float64) error { return nil }

type spyBiller struct {
	mu     sync.Mutex
	debits map[string]float64
}

func (s *spyBiller) Debit(_ context.Context, _, messageID string, amount float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.debits == nil {
		s.debits = make(map[string]float64)
	}
	s.debits[messageID] = amount
	return nil
}

func (s *spyBiller) get(msgID string) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.debits[msgID]
	return v, ok
}

func TestWorkerDebitsOnAccept(t *testing.T) {
	sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	defer sim.Close()

	repo := store.NewMemory()
	q := queue.NewMemory()
	biller := &spyBiller{}
	w := NewWorker(q, repo, WithBackoff(noBackoff), WithDLR(dlr.NewProcessor(dlr.NewMemCache(), repo, nil)), WithBiller(biller))
	sess := dialSess(t, w, sim.Addr())
	w.SetSession(sess)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go q.Consume(ctx, queue.Key(1, 0), "g", func(it queue.Item) error { return w.Handle(ctx, it) })

	p := pipeline.NewPipeline(repo, q, &fixedRouter{ids: []int{1}}, pipeline.WithBiller(
		&billingStub{price: 0.25}))
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
			amt, ok := biller.get(msgID)
			if !ok || amt != 0.25 {
				t.Fatalf("debit no registrado: amt=%v ok=%v", amt, ok)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no llego a delivered")
}

func TestWorkerNoDebitOnReject(t *testing.T) {
	sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
		RespondSubmitStatus: smpp.ESME_RSYSERR})
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	defer sim.Close()

	repo := store.NewMemory()
	q := queue.NewMemory()
	biller := &spyBiller{}
	w := NewWorker(q, repo, WithBackoff(noBackoff), WithBiller(biller))
	sess := dialSess(t, w, sim.Addr())
	w.SetSession(sess)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go q.Consume(ctx, queue.Key(1, 0), "g", func(it queue.Item) error { return w.Handle(ctx, it) })

	p := pipeline.NewPipeline(repo, q, &fixedRouter{ids: []int{1}})
	msgID, _, err := p.Submit(ctx, pipeline.Outgoing{
		TenantID: "t1", Msisdn: "569123", Text: "hola",
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m, _ := repo.GetMessage(ctx, msgID)
		if m != nil && m.State == "rejected" {
			if _, ok := biller.get(msgID); ok {
				t.Fatal("mensaje rechazado no debia cobrarse")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no llego a rejected")
}
