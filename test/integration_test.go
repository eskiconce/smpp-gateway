package test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
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

	w := worker.NewWorker(redisQ, pgRepo)
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
