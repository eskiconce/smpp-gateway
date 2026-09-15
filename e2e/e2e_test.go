package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/api"
	"github.com/eskiconce/smpp-gateway/internal/auth"
	"github.com/eskiconce/smpp-gateway/internal/billing"
	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/dlr"
	"github.com/eskiconce/smpp-gateway/internal/esme"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/session"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
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

type billingAdapter struct{ s *billing.Service }

func (a billingAdapter) Debit(ctx context.Context, tenantID, messageID string, amount float64) error {
	_, err := a.s.Debit(ctx, tenantID, messageID, amount)
	return err
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

	if err := repo.CreateTenant(ctx, &store.Tenant{ID: "t1", Name: "kiki", Balance: 10, Mode: "prepaid", ApiKey: "e2e-key"}); err != nil {
		t.Fatal(err)
	}
	tbl := &store.RateTable{TenantID: "t1", Name: "e2e", Active: true}
	if err := repo.CreateRateTable(ctx, tbl); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateRateEntry(ctx, &store.RateEntry{TableID: tbl.ID, Prefix: "", Price: 0.2500}); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	webhookHits := 0
	whsrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		webhookHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer whsrv.Close()

	if err := repo.CreateWebhook(ctx, &store.Webhook{
		TenantID: "t1", URL: whsrv.URL, Events: []string{"delivered"}, Active: true,
	}); err != nil {
		t.Fatal(err)
	}

	w := worker.NewWorker(q, repo, worker.WithDLR(
		dlr.NewProcessor(dlr.NewMemCache(), repo, dlr.NewWebhookNotifier(repo, 2*time.Second))),
		worker.WithBiller(billingAdapter{billing.New(repo, repo, repo)}))
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
	bill := billing.New(repo, repo, repo)
	p := pipeline.NewPipeline(repo, q, r, pipeline.WithBiller(bill))
	srv := api.New(apiConfig(), p, repo)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	body, _ := json.Marshal(map[string]string{"to": "569123", "text": "hola"})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/api/v1/messages", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer e2e-key")
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
			tnt, _ := repo.GetTenant(ctx, "t1")
			if tnt.Balance != 9.75 {
				t.Fatalf("tras delivered balance=%v (esperado 9.75)", tnt.Balance)
			}
			whDeadline := time.Now().Add(time.Second)
			for time.Now().Before(whDeadline) {
				mu.Lock()
				n := webhookHits
				mu.Unlock()
				if n > 0 {
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Fatal("webhook no fue llamado")
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("e2e: el mensaje no llego a delivered")
}

func readPDU(br *bufio.Reader) (*smpp.PDU, error) {
	head := make([]byte, 4)
	if _, err := io.ReadFull(br, head); err != nil {
		return nil, err
	}
	n := int(smpp.GetU32(head))
	if n < smpp.HeaderLen {
		return nil, fmt.Errorf("pdu corto: %d", n)
	}
	rest := make([]byte, n-4)
	if _, err := io.ReadFull(br, rest); err != nil {
		return nil, err
	}
	return smpp.Decode(append(head, rest...))
}

type dlrToESME struct {
	esme *esme.Server
}

func (d *dlrToESME) Notify(_ context.Context, ev dlr.Event) error {
	sessions := d.esme.SessionsByTenant(ev.TenantID)
	if len(sessions) == 0 {
		return nil
	}
	text := fmt.Sprintf("id:%s sub:001 dlvrd:001 submit date:2609121230 done date:2609121231 stat:%s err:000 text:", ev.SmscMsgid, ev.State)
	return sessions[0].Deliver(ev.Msisdn, "", text)
}

func TestE2ESMPPSubmitToDeliverSM(t *testing.T) {
	repo := store.NewMemory()
	q := queue.NewMemory()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := repo.CreateTenant(ctx, &store.Tenant{
		ID: "t1", Status: "active", Mode: "prepaid", Balance: 10,
		SmppSystemID: "esme-01", SmppPassword: "pass",
	}); err != nil {
		t.Fatal(err)
	}
	tbl := &store.RateTable{TenantID: "t1", Name: "nacional", Active: true}
	if err := repo.CreateRateTable(ctx, tbl); err != nil {
		t.Fatal(err)
	}
	if err := repo.CreateRateEntry(ctx, &store.RateEntry{TableID: tbl.ID, Prefix: "569", Price: 0.01}); err != nil {
		t.Fatal(err)
	}

	r := router.New(&e2eRules{groups: []router.Group{{
		ID: 3, Name: "ops",
		Members: []router.GroupMember{{ConnectorID: 1, Weight: 1}},
	}}}, router.Config{})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}
	bill := billing.New(repo, repo, repo)
	p := pipeline.NewPipeline(repo, q, r, pipeline.WithBiller(bill))

	esmeSrv := esme.New(esme.Config{Addr: "127.0.0.1:0"}, repo, p)
	go func() { _ = esmeSrv.Run(ctx) }()
	defer esmeSrv.Close()
	deadline := time.Now().Add(2 * time.Second)
	for esmeSrv.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
	sim.Start()
	defer sim.Close()

	_, portStr, _ := net.SplitHostPort(sim.Addr())
	port, _ := strconv.Atoi(portStr)

	dlrProc := dlr.NewProcessor(dlr.NewMemCache(), repo, &dlrToESME{esmeSrv})
	w := worker.NewWorker(q, repo, worker.WithBackoff(noBackoff), worker.WithDLR(dlrProc), worker.WithBiller(billingAdapter{bill}))
	sess := session.New(session.Config{
		Host: "127.0.0.1", Port: port, SystemID: "esp", Password: "secreto",
		SourceAddr: "shield", MsgPerSecond: 100, MaxConcurrency: 1,
	}, w)
	w.SetSession(sess)
	if err := sess.Dial(ctx); err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	go q.Consume(ctx, queue.Key(1, 0), "e2e", func(it queue.Item) error {
		return w.Handle(ctx, it)
	})

	esmeConn, _ := net.Dial("tcp", esmeSrv.Addr())
	defer esmeConn.Close()
	br := bufio.NewReader(esmeConn)

	bind := smpp.NewBindTransceiver(1, "esme-01", "pass", "", 0, 0, "")
	esmeConn.Write(smpp.Encode(bind))
	bindResp, _ := readPDU(br)
	if bindResp.Header.Status != smpp.ESME_ROK {
		t.Fatalf("bind status=%x", bindResp.Header.Status)
	}

	sub, _ := smpp.NewSubmitSM(2, "1234", "569123", "hola mundo", 0, 1)
	esmeConn.Write(smpp.Encode(sub))
	subResp, _ := readPDU(br)
	if subResp.Header.Status != smpp.ESME_ROK {
		t.Fatalf("submit status=%x", subResp.Header.Status)
	}
	msgID, err := smpp.ParseSubmitSMResp(subResp.Body)
	if err != nil || msgID == "" {
		t.Fatalf("submit_sm_resp sin msgid: %v", err)
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		dlrPkt, err := readPDU(br)
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if dlrPkt.Header.ID == smpp.DeliverSM {
			f, _ := smpp.ParseDeliverSM(dlrPkt.Body)
			if f.ShortMessage != "" {
				return
			}
		}
	}
	t.Fatal("e2e: deliver_sm no recibido")
}

type noBackoffType struct{}

func noBackoff(int) time.Duration { return 0 }

func TestAdminLoginYCRUD(t *testing.T) {
	repo := store.NewMemory()
	hash, err := auth.HashPassword("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	_ = repo.CreateUser(context.Background(), &store.User{
		Username: "admin", PasswordHash: hash, Role: "superadmin",
	})
	cfg := apiConfig()
	cfg.JWTSecret = "secreto-e2e"
	cfg.JWTTTL = time.Hour
	srv := api.New(cfg, nil, repo)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	loginBody := strings.NewReader(`{"username":"admin","password":"s3cret"}`)
	resp, err := http.Post(ts.URL+"/api/v1/auth/login", "application/json", loginBody)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("login %d", resp.StatusCode)
	}
	var tok struct{ Token string `json:"token"` }
	json.NewDecoder(resp.Body).Decode(&tok)
	resp.Body.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/admin/messages", nil)
	req.Header.Set("Authorization", "Bearer "+tok.Token)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 || !strings.Contains(string(b), `"total":`) {
		t.Fatalf("messages %d: %s", resp2.StatusCode, string(b))
	}
}
