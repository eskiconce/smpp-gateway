package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/eskiconce/smpp-gateway/internal/billing"
	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

func TestMessagesEndpoint(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	repo.CreateTenant(ctx, &store.Tenant{ID: "t1", Name: "kiki", ApiKey: "k1"})
	q := queue.NewMemory()
	key := "stream:con:5:0"
	go q.Consume(ctx, key, "g", func(queue.Item) error { return nil })

	r := router.New(stubRouterStore{}, router.Config{})
	_ = r.Load(ctx)
	p := pipeline.NewPipeline(repo, q, r)
	srv := New(config.Config{}, p, repo)

	body := `{"to": "569123", "text": "hola"}`
	req := httptest.NewRequest("POST", "/api/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer k1")
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
	repo.CreateTenant(ctx, &store.Tenant{ID: "t1", Name: "kiki", ApiKey: "k1"})
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
	repo.CreateTenant(ctx, &store.Tenant{ID: "t1", Name: "kiki", ApiKey: "k1"})
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

func TestSubmitRequiereAPIKey(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	repo.CreateTenant(ctx, &store.Tenant{ID: "t1", Name: "kiki", Balance: 10, ApiKey: "k1"})
	q := queue.NewMemory()
	go q.Consume(ctx, queue.Key(5, 0), "g", func(queue.Item) error { return nil })

	r := router.New(stubRouterStore{}, router.Config{})
	_ = r.Load(ctx)
	p := pipeline.NewPipeline(repo, q, r)
	srv := New(config.Config{}, p, repo)
	h := srv.Handler()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/messages", strings.NewReader(`{"to":"569123","text":"hola"}`))
	req.Header.Set("Authorization", "Bearer k1")
	h.ServeHTTP(rr, req)
	if rr.Code == 401 {
		t.Fatalf("con api key valida no debio dar 401: %d %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/v1/messages", strings.NewReader(`{"to":"569123","text":"hola"}`))
	req.Header.Set("Authorization", "Bearer inexistente")
	h.ServeHTTP(rr, req)
	if rr.Code != 401 {
		t.Fatalf("esperaba 401, got %d", rr.Code)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/v1/messages", strings.NewReader(`{"to":"569123","text":"hola"}`))
	h.ServeHTTP(rr, req)
	if rr.Code != 401 {
		t.Fatalf("sin header esperaba 401, got %d", rr.Code)
	}
}

func TestSubmitSaldoInsuficienteHTTP(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	repo.CreateTenant(ctx, &store.Tenant{ID: "t1", Name: "kiki", Balance: 0.1, ApiKey: "k1"})
	repo.CreateRateTable(ctx, &store.RateTable{TenantID: "t1", Name: "x", Active: true})
	q := queue.NewMemory()
	go q.Consume(ctx, queue.Key(5, 0), "g", func(queue.Item) error { return nil })

	r := router.New(stubRouterStore{}, router.Config{})
	_ = r.Load(ctx)
	bill := billing.New(repo, repo, repo)
	p := pipeline.NewPipeline(repo, q, r, pipeline.WithBiller(bill))
	srv := New(config.Config{}, p, repo)
	h := srv.Handler()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/messages", strings.NewReader(`{"to":"569123","text":"hola"}`))
	req.Header.Set("Authorization", "Bearer k1")
	h.ServeHTTP(rr, req)
	if rr.Code != 422 {
		t.Fatalf("sin tarifa esperaba 422, got %d %s", rr.Code, rr.Body.String())
	}
}

func TestAdminBillingEndpoints(t *testing.T) {
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
	req := httptest.NewRequest("POST", "/api/v1/admin/tenants",
		strings.NewReader(`{"id":"t1","name":"kiki","balance":10,"mode":"prepaid","api_key":"k1"}`))
	h.ServeHTTP(rr, req)
	if rr.Code != 201 {
		t.Fatalf("create tenant %d: %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/v1/admin/tenants/t1/credit",
		strings.NewReader(`{"amount":5}`))
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("credit %d: %s", rr.Code, rr.Body.String())
	}
	var cred struct {
		Balance float64 `json:"balance"`
	}
	json.Unmarshal(rr.Body.Bytes(), &cred)
	if cred.Balance != 15 {
		t.Fatalf("balance=%v", cred.Balance)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/v1/admin/rate-tables",
		strings.NewReader(`{"name":"std","active":true}`))
	req.Header.Set("X-Tenant-ID", "t1")
	h.ServeHTTP(rr, req)
	if rr.Code != 201 {
		t.Fatalf("rate-table %d: %s", rr.Code, rr.Body.String())
	}
	var tbl struct {
		ID int `json:"id"`
	}
	json.Unmarshal(rr.Body.Bytes(), &tbl)

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("POST",
		"/api/v1/admin/rate-tables/"+strconv.Itoa(tbl.ID)+"/entries",
		strings.NewReader(`{"prefix":"569","price":0.25}`))
	req.Header.Set("X-Tenant-ID", "t1")
	h.ServeHTTP(rr, req)
	if rr.Code != 201 {
		t.Fatalf("rate-entry %d: %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("GET",
		"/api/v1/admin/rate-tables/"+strconv.Itoa(tbl.ID)+"/entries", nil)
	req.Header.Set("X-Tenant-ID", "t1")
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("list entries %d", rr.Code)
	}
	var entries []map[string]any
	json.Unmarshal(rr.Body.Bytes(), &entries)
	if len(entries) != 1 || entries[0]["prefix"] != "569" {
		t.Fatalf("entries=%+v", entries)
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/v1/admin/tenants/t1/transactions", nil)
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("transactions %d", rr.Code)
	}
	var txns []map[string]any
	json.Unmarshal(rr.Body.Bytes(), &txns)
	if len(txns) != 1 || txns[0]["type"] != "credit" {
		t.Fatalf("txns=%+v", txns)
	}
}
