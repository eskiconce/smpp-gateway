package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/auth"
	"github.com/eskiconce/smpp-gateway/internal/billing"
	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

var testJWTSecret = []byte("test-secret-key-for-tests")

func testConfig() config.Config {
	return config.Config{JWTSecret: string(testJWTSecret), JWTTTL: time.Hour}
}

func mustHash(t *testing.T, pw string) string {
	t.Helper()
	h, err := auth.HashPassword(pw)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func loginToken(t *testing.T, srv *Server, user, pass string) string {
	t.Helper()
	body := `{"username":"` + user + `","password":"` + pass + `"}`
	req := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(body))
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("login %d: %s", rr.Code, rr.Body.String())
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out.Token
}

func apiFor(t *testing.T, repo *store.MemoryRepo) *Server {
	t.Helper()
	q := queue.NewMemory()
	ctx := context.Background()
	go q.Consume(ctx, queue.Key(5, 0), "g", func(queue.Item) error { return nil })
	r := router.New(stubRouterStore{}, router.Config{})
	_ = r.Load(ctx)
	p := pipeline.NewPipeline(repo, q, r)
	return New(testConfig(), p, repo)
}

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
	_ = repo.CreateUser(ctx, &store.User{
		Username: "admin", PasswordHash: mustHash(t, "a"), Role: "admin",
	})
	srv := apiFor(t, repo)
	tok := loginToken(t, srv, "admin", "a")
	h := srv.Handler()

	authReq := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("X-Tenant-ID", "t1")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}

	rr := authReq("POST", "/api/v1/admin/webhooks",
		`{"url":"https://cli.example.com/h","auth_token":"tok","events":["delivered"],"active":true}`)
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

	rr = authReq("PUT", "/api/v1/admin/webhooks/"+strconv.Itoa(created.ID),
		`{"url":"https://cli.example.com/h","auth_token":"tok","events":["expired"],"active":false}`)
	if rr.Code != 200 {
		t.Fatalf("update status %d: %s", rr.Code, rr.Body.String())
	}

	rr = authReq("GET", "/api/v1/admin/webhooks", "")
	var list []map[string]any
	json.Unmarshal(rr.Body.Bytes(), &list)
	if rr.Code != 200 || len(list) != 1 || list[0]["active"] != false {
		t.Fatalf("list status %d: %+v", rr.Code, list)
	}

	rr = authReq("DELETE", "/api/v1/admin/webhooks/"+strconv.Itoa(created.ID), "")
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
	_ = repo.CreateUser(ctx, &store.User{
		Username: "admin", PasswordHash: mustHash(t, "a"), Role: "admin",
	})
	srv := apiFor(t, repo)
	tok := loginToken(t, srv, "admin", "a")
	h := srv.Handler()

	authReq := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}

	rr := authReq("POST", "/api/v1/admin/tenants",
		`{"id":"t1","name":"kiki","balance":10,"mode":"prepaid","api_key":"k1"}`)
	if rr.Code != 201 {
		t.Fatalf("create tenant %d: %s", rr.Code, rr.Body.String())
	}

	rr = authReq("POST", "/api/v1/admin/tenants/t1/credit", `{"amount":5}`)
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

	rr = authReq("POST", "/api/v1/admin/rate-tables", `{"name":"std","active":true}`)
	if rr.Code != 201 {
		t.Fatalf("rate-table %d: %s", rr.Code, rr.Body.String())
	}
	var tbl struct {
		ID int `json:"id"`
	}
	json.Unmarshal(rr.Body.Bytes(), &tbl)

	rr = authReq("POST",
		"/api/v1/admin/rate-tables/"+strconv.Itoa(tbl.ID)+"/entries",
		`{"prefix":"569","price":0.25}`)
	if rr.Code != 201 {
		t.Fatalf("rate-entry %d: %s", rr.Code, rr.Body.String())
	}

	rr = authReq("GET",
		"/api/v1/admin/rate-tables/"+strconv.Itoa(tbl.ID)+"/entries", "")
	if rr.Code != 200 {
		t.Fatalf("list entries %d", rr.Code)
	}
	var entries []map[string]any
	json.Unmarshal(rr.Body.Bytes(), &entries)
	if len(entries) != 1 || entries[0]["prefix"] != "569" {
		t.Fatalf("entries=%+v", entries)
	}

	rr = authReq("GET", "/api/v1/admin/tenants/t1/transactions", "")
	if rr.Code != 200 {
		t.Fatalf("transactions %d", rr.Code)
	}
	var txns []map[string]any
	json.Unmarshal(rr.Body.Bytes(), &txns)
	if len(txns) != 1 || txns[0]["type"] != "credit" {
		t.Fatalf("txns=%+v", txns)
	}
}

func TestLogin(t *testing.T) {
	repo := store.NewMemory()
	_ = repo.CreateUser(context.Background(), &store.User{
		Username: "admin", PasswordHash: mustHash(t, "s3cret"), Role: "superadmin",
	})
	srv := apiFor(t, repo)
	tok := loginToken(t, srv, "admin", "s3cret")
	if tok == "" {
		t.Fatal("token vacio")
	}
	req := httptest.NewRequest("POST", "/api/v1/auth/login", strings.NewReader(`{"username":"admin","password":"mala"}`))
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 401 {
		t.Fatalf("login mal deberia dar 401, got %d", rr.Code)
	}
}

func TestRBACAdminEndpoints(t *testing.T) {
	repo := store.NewMemory()
	_ = repo.CreateUser(context.Background(), &store.User{
		Username: "viewer", PasswordHash: mustHash(t, "v"), Role: "viewer",
	})
	srv := apiFor(t, repo)

	req := httptest.NewRequest("GET", "/api/v1/admin/tenants", nil)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 401 {
		t.Fatalf("sin token deberia dar 401, got %d", rr.Code)
	}

	body := strings.NewReader(`{"id":"t1","name":"T1"}`)
	req = httptest.NewRequest("POST", "/api/v1/admin/tenants", body)
	req.Header.Set("Authorization", "Bearer "+loginToken(t, srv, "viewer", "v"))
	rr = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 403 {
		t.Fatalf("viewer escribiendo deberia dar 403, got %d", rr.Code)
	}

	req = httptest.NewRequest("GET", "/api/v1/admin/tenants", nil)
	req.Header.Set("Authorization", "Bearer "+loginToken(t, srv, "viewer", "v"))
	rr = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("viewer leyendo deberia dar 200, got %d", rr.Code)
	}
}

func TestConnectorsCRUD(t *testing.T) {
	repo := store.NewMemory()
	_ = repo.CreateUser(context.Background(), &store.User{
		Username: "admin", PasswordHash: mustHash(t, "a"), Role: "admin",
	})
	srv := apiFor(t, repo)
	tok := loginToken(t, srv, "admin", "a")

	post := func(path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		return rr
	}
	rr := post("/api/v1/admin/connectors", `{"name":"c1","type":"smpp","host":"h","port":2775}`)
	if rr.Code != 201 {
		t.Fatalf("create %d: %s", rr.Code, rr.Body.String())
	}
	req := httptest.NewRequest("GET", "/api/v1/admin/connectors", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rr = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"password":"`+maskPassword+`"`) {
		t.Fatalf("list %d: %s", rr.Code, rr.Body.String())
	}
	if rr := post("/api/v1/admin/connectors", `{"name":"x","type":"raro"}`); rr.Code != 400 {
		t.Fatalf("type invalido deberia dar 400, got %d", rr.Code)
	}
}

func TestRulesGroupsEndpoints(t *testing.T) {
	repo := store.NewMemory()
	_ = repo.CreateUser(context.Background(), &store.User{
		Username: "admin", PasswordHash: mustHash(t, "a"), Role: "admin",
	})
	srv := apiFor(t, repo)
	tok := loginToken(t, srv, "admin", "a")

	post := func(path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, req)
		return rr
	}
	rcg := post("/api/v1/admin/groups", `{"name":"g1"}`)
	if rcg.Code != 201 {
		t.Fatalf("group %d: %s", rcg.Code, rcg.Body.String())
	}
	var gid struct {
		ID int `json:"id"`
	}
	_ = json.Unmarshal(rcg.Body.Bytes(), &gid)

	cc := post("/api/v1/admin/connectors", `{"name":"c1","type":"smpp","host":"h","port":1}`)
	var cid struct {
		ID int `json:"id"`
	}
	_ = json.Unmarshal(cc.Body.Bytes(), &cid)

	req := httptest.NewRequest("PUT", "/api/v1/admin/groups/"+strconv.Itoa(gid.ID)+"/members",
		strings.NewReader(`{"members":[{"connector_id":`+strconv.Itoa(cid.ID)+`,"weight":100}]}`))
	req.Header.Set("Authorization", "Bearer "+tok)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("members %d: %s", rr.Code, rr.Body.String())
	}

	ruleBody := `{"priority":1,"prefix":"569","group_id":` + strconv.Itoa(gid.ID) + `}`
	rc := post("/api/v1/admin/routing-rules", ruleBody)
	if rc.Code != 201 {
		t.Fatalf("rule %d: %s", rc.Code, rc.Body.String())
	}
	req = httptest.NewRequest("GET", "/api/v1/admin/routing-rules", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rr = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"prefix":"569"`) {
		t.Fatalf("rules %d: %s", rr.Code, rr.Body.String())
	}
}

func TestMetricsAndMessagesList(t *testing.T) {
	repo := store.NewMemory()
	_ = repo.CreateUser(context.Background(), &store.User{
		Username: "v", PasswordHash: mustHash(t, "v"), Role: "viewer",
	})
	for i := 0; i < 3; i++ {
		_ = repo.CreateMessage(context.Background(), &store.Message{
			ID: "m" + strconv.Itoa(i), TenantID: "t1", Msisdn: "569x",
			State: "delivered", ConnectorID: 1, Segments: 1, Text: "hola",
		})
	}
	srv := apiFor(t, repo)
	tok := loginToken(t, srv, "v", "v")

	req := httptest.NewRequest("GET", "/api/v1/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"delivered":3`) {
		t.Fatalf("metrics %d: %s", rr.Code, rr.Body.String())
	}

	req = httptest.NewRequest("GET", "/api/v1/admin/messages?msisdn=569", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rr = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != 200 || !strings.Contains(rr.Body.String(), `"total":3`) {
		t.Fatalf("messages %d: %s", rr.Code, rr.Body.String())
	}
}
