package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

func TestMessagesEndpoint(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	q := queue.NewMemory()
	key := "stream:con:5:0"
	go q.Consume(ctx, key, "g", func(queue.Item) error { return nil })

	r := router.New(stubRouterStore{}, router.Config{})
	_ = r.Load(ctx)
	p := pipeline.NewPipeline(repo, q, r)
	srv := New(config.Config{}, p, repo)

	body := `{"to": "569123", "text": "hola"}`
	req := httptest.NewRequest("POST", "/api/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("X-Tenant-ID", "t1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	var out struct {
		MessageID string `json:"message_id"`
		Segments  int    `json:"segments"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.MessageID == "" || out.Segments < 1 {
		t.Fatalf("out %+v", out)
	}
}

type stubRouterStore struct{}

func (stubRouterStore) ListRoutingRules(context.Context) ([]router.Rule, error) {
	return []router.Rule{{Priority: 1, ConnectorID: 5}}, nil
}

func (stubRouterStore) ListGroups(context.Context) ([]router.Group, error) {
	return []router.Group{}, nil
}

func TestGetMessageEndpoint(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	q := queue.NewMemory()
	go q.Consume(ctx, queue.Key(5, 0), "g", func(queue.Item) error { return nil })

	r := router.New(stubRouterStore{}, router.Config{})
	_ = r.Load(ctx)
	p := pipeline.NewPipeline(repo, q, r)
	srv := New(config.Config{}, p, repo)

	repo.CreateMessage(ctx, &store.Message{ID: "abc", TenantID: "t1", Msisdn: "569", State: "delivered"})

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/v1/messages/abc", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	var out struct {
		MessageID string `json:"message_id"`
		State     string `json:"state"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.MessageID != "abc" || out.State != "delivered" {
		t.Fatalf("out %+v", out)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/v1/messages/no-existe", nil)
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 404 {
		t.Fatalf("status %d", rr.Code)
	}
}

func TestWebhooksCRUD(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	q := queue.NewMemory()
	go q.Consume(ctx, queue.Key(5, 0), "g", func(queue.Item) error { return nil })

	r := router.New(stubRouterStore{}, router.Config{})
	_ = r.Load(ctx)
	p := pipeline.NewPipeline(repo, q, r)
	srv := New(config.Config{}, p, repo)
	h := srv.Handler()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/admin/webhooks", strings.NewReader(
		`{"url":"https://cli.example.com/h","auth_token":"tok","events":["delivered"],"active":true}`))
	req.Header.Set("X-Tenant-ID", "t1")
	h.ServeHTTP(rr, req)
	if rr.Code != 201 {
		t.Fatalf("create status %d: %s", rr.Code, rr.Body.String())
	}
	var created struct {
		ID int `json:"id"`
	}
	json.Unmarshal(rr.Body.Bytes(), &created)
	if created.ID == 0 {
		t.Fatal("sin id")
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("PUT", "/api/v1/admin/webhooks/"+strconv.Itoa(created.ID), strings.NewReader(
		`{"url":"https://cli.example.com/h","auth_token":"tok","events":["expired"],"active":false}`))
	req.Header.Set("X-Tenant-ID", "t1")
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("update status %d: %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/v1/admin/webhooks", nil)
	req.Header.Set("X-Tenant-ID", "t1")
	h.ServeHTTP(rr, req)
	var list []map[string]any
	json.Unmarshal(rr.Body.Bytes(), &list)
	if rr.Code != 200 || len(list) != 1 || list[0]["active"] != false {
		t.Fatalf("list status %d: %+v", rr.Code, list)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("DELETE", "/api/v1/admin/webhooks/"+strconv.Itoa(created.ID), nil)
	req.Header.Set("X-Tenant-ID", "t1")
	h.ServeHTTP(rr, req)
	if rr.Code != 204 {
		t.Fatalf("delete status %d", rr.Code)
	}
}
