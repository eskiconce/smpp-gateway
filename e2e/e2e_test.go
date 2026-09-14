package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/api"
	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/session"
	"github.com/eskiconce/smpp-gateway/internal/smscsim"
	"github.com/eskiconce/smpp-gateway/internal/store"
	"github.com/eskiconce/smpp-gateway/internal/worker"
)

func apiConfig() config.Config { return config.Config{} }

type e2eRules struct {
	groups []router.Group
}

func (e *e2eRules) ListRoutingRules(context.Context) ([]router.Rule, error) {
	return []router.Rule{{ID: 1, Priority: 1, Prefix: "569", GroupID: 3}}, nil
}

func (e *e2eRules) ListGroups(context.Context) ([]router.Group, error) {
	return e.groups, nil
}

func TestE2EHTTPSubmitToDelivered(t *testing.T) {
	sim := smscsim.New(smscsim.Config{
		Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true,
	})
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	defer sim.Close()

	_, portStr, _ := net.SplitHostPort(sim.Addr())
	port, _ := strconv.Atoi(portStr)

	repo := store.NewMemory()
	q := queue.NewMemory()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := worker.NewWorker(q, repo)
	sess := session.New(session.Config{
		Host: "127.0.0.1", Port: port, SystemID: "esp", Password: "secreto",
		SourceAddr: "shield", MsgPerSecond: 100, MaxConcurrency: 1,
	}, w)
	w.SetSession(sess)
	if err := sess.Dial(ctx); err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	go q.Consume(ctx, "stream:con:1:0", "e2e", func(it queue.Item) error {
		return w.Handle(ctx, it)
	})

	r := router.New(&e2eRules{groups: []router.Group{{
		ID: 3, Name: "ops",
		Members: []router.GroupMember{{ConnectorID: 1, Weight: 1}},
	}}}, router.Config{})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}
	p := pipeline.NewPipeline(repo, q, r)
	srv := api.New(apiConfig(), p, repo)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{"to": "569123", "text": "hola"})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/v1/messages", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", "t1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var out struct {
		MessageID string `json:"message_id"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		m, _ := repo.GetMessage(ctx, out.MessageID)
		if m != nil && m.State == "delivered" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("e2e: el mensaje no llego a delivered")
}
