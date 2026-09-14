package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/api"
	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/dlr"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/session"
	"github.com/eskiconce/smpp-gateway/internal/smscsim"
	"github.com/eskiconce/smpp-gateway/internal/store"
	"github.com/eskiconce/smpp-gateway/internal/worker"
)

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("SMG_TEST_INTEGRATION") != "1" {
		t.Skip("SMG_TEST_INTEGRATION != 1, saltando integracion")
	}
}

func TestIntegrationSubmitToDelivered(t *testing.T) {
	requireIntegration(t)

	dbURL := os.Getenv("SMG_DB_URL")
	redisURL := os.Getenv("SMG_REDIS_URL")
	if dbURL == "" || redisURL == "" {
		t.Fatal("SMG_DB_URL y SMG_REDIS_URL requeridos")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgRepo, err := store.NewPG(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pgRepo.Close()

	redisQ, err := queue.NewRedis(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	defer redisQ.Close()

	sim := smscsim.New(smscsim.Config{
		Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true,
	})
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	defer sim.Close()

	_, portStr, _ := net.SplitHostPort(sim.Addr())
	port, _ := strconv.Atoi(portStr)

	w := worker.NewWorker(redisQ, pgRepo, worker.WithDLR(
		dlr.NewProcessor(dlr.NewMemCache(), pgRepo, dlr.NewWebhookNotifier(pgRepo, 2*time.Second))))
	sess := session.New(session.Config{
		Host: "127.0.0.1", Port: port, SystemID: "esp", Password: "secreto",
		SourceAddr: "shield", MsgPerSecond: 100, MaxConcurrency: 1,
	}, w)
	w.SetSession(sess)
	if err := sess.Dial(ctx); err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	go redisQ.Consume(ctx, "stream:con:1:0", "integ", func(it queue.Item) error {
		return w.Handle(ctx, it)
	})

	r := router.New(pgRepo, router.Config{})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}
	p := pipeline.NewPipeline(pgRepo, redisQ, r)
	srv := api.New(config.Config{}, p, pgRepo)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{"to": "569123", "text": "hola integracion"})
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

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		m, _ := pgRepo.GetMessage(ctx, out.MessageID)
		if m != nil && m.State == "delivered" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("integracion: el mensaje no llego a delivered")
}

func TestIntegrationDLRPipeline(t *testing.T) {
	requireIntegration(t)
	dsn := os.Getenv("SMG_DB_URL")
	redisURL := os.Getenv("SMG_REDIS_URL")
	if dsn == "" || redisURL == "" {
		t.Skip("SMG_DB_URL/SMG_REDIS_URL vacio")
	}
	ctx := context.Background()

	repo, err := store.NewPG(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer repo.Close()

	var received map[string]any
	var mu sync.Mutex
	whsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		json.Unmarshal(b, &received)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer whsrv.Close()

	mid := "it-dlr-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := repo.CreateMessage(ctx, &store.Message{
		ID: mid, TenantID: "t1", Msisdn: "569123", Text: "hola", State: "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateWebhook(ctx, &store.Webhook{
		TenantID: "t1", URL: whsrv.URL, Events: []string{"delivered"}, Active: true,
	}); err != nil {
		t.Fatal(err)
	}

	cache, err := dlr.NewRedisCache(redisURL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	proc := dlr.NewProcessor(cache, repo, dlr.NewWebhookNotifier(repo, 2*time.Second))

	if err := proc.Register(ctx, "it-smsc-1", mid); err != nil {
		t.Fatal(err)
	}
	if err := proc.Handle(ctx, "it-smsc-1", "DELIVRD"); err != nil {
		t.Fatal(err)
	}

	m, err := repo.GetMessage(ctx, mid)
	if err != nil || m.State != "delivered" {
		t.Fatalf("state=%+v err=%v", m, err)
	}
	if v, _ := cache.Lookup(ctx, "it-smsc-1"); v != "" {
		t.Fatalf("cache deberia estar limpio, got %q", v)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := received != nil
		mu.Unlock()
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if received == nil || received["state"] != "delivered" {
		t.Fatalf("webhook=%+v", received)
	}
}
