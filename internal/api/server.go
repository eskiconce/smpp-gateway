package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/billing"
	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type repos interface {
	store.MessageRepo
	store.WebhookRepo
	store.TenantRepo
	store.RateRepo
	store.LedgerRepo
}

type Server struct {
	cfg     config.Config
	p       *pipeline.Pipeline
	repo    repos
	billing *billing.Service
}

func New(cfg config.Config, p *pipeline.Pipeline, repo repos) *Server {
	return &Server{cfg: cfg, p: p, repo: repo, billing: billing.New(repo, repo, repo)}
}

type submitReq struct {
	To         string `json:"to"`
	Text       string `json:"text"`
	RoutingTag string `json:"routing_tag"`
}

type submitResp struct {
	MessageID string `json:"message_id"`
	Segments  int    `json:"segments"`
}

func (s *Server) mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /api/v1/messages", s.handleSubmit)
	mux.HandleFunc("GET /api/v1/messages/{id}", s.handleGetMessage)

	mux.HandleFunc("GET /api/v1/admin/webhooks", s.handleListWebhooks)
	mux.HandleFunc("POST /api/v1/admin/webhooks", s.handleCreateWebhook)
	mux.HandleFunc("PUT /api/v1/admin/webhooks/{id}", s.handleUpdateWebhook)
	mux.HandleFunc("DELETE /api/v1/admin/webhooks/{id}", s.handleDeleteWebhook)

	mux.HandleFunc("GET /api/v1/admin/tenants", s.handleListTenants)
	mux.HandleFunc("POST /api/v1/admin/tenants", s.handleCreateTenant)
	mux.HandleFunc("GET /api/v1/admin/tenants/{id}", s.handleGetTenant)
	mux.HandleFunc("POST /api/v1/admin/tenants/{id}/credit", s.handleCreditTenant)
	mux.HandleFunc("GET /api/v1/admin/tenants/{id}/transactions", s.handleListTransactions)

	mux.HandleFunc("GET /api/v1/admin/rate-tables", s.handleListRateTables)
	mux.HandleFunc("POST /api/v1/admin/rate-tables", s.handleCreateRateTable)
	mux.HandleFunc("GET /api/v1/admin/rate-tables/{id}/entries", s.handleListRateEntries)
	mux.HandleFunc("POST /api/v1/admin/rate-tables/{id}/entries", s.handleCreateRateEntry)
	mux.HandleFunc("DELETE /api/v1/admin/rate-entries/{id}", s.handleDeleteRateEntry)
	return mux
}

func (s *Server) Handler() http.Handler { return s.mux() }

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if key == "" {
		http.Error(w, "api key requerida", http.StatusUnauthorized)
		return
	}
	tenant, err := s.repo.GetTenantByAPIKey(r.Context(), key)
	if err != nil {
		http.Error(w, "api key invalida", http.StatusUnauthorized)
		return
	}
	var req submitReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	if req.To == "" || req.Text == "" {
		http.Error(w, "to y text requeridos", http.StatusBadRequest)
		return
	}
	out := pipeline.Outgoing{
		TenantID:   tenant.ID,
		SourceAddr: "api",
		Msisdn:     req.To,
		Text:       req.Text,
		RoutingTag: req.RoutingTag,
		Priority:   0,
	}
	id, segments, err := s.p.Submit(r.Context(), out)
	if err != nil {
		switch {
		case errors.Is(err, billing.ErrInsufficientBalance):
			http.Error(w, "saldo insuficiente", http.StatusPaymentRequired)
		case errors.Is(err, billing.ErrNoRate):
			http.Error(w, "sin tarifa para el destino", http.StatusUnprocessableEntity)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(submitResp{MessageID: id, Segments: segments})
}

func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request) {
	m, err := s.repo.GetMessage(r.Context(), r.PathValue("id"))
	if err != nil || m == nil {
		http.Error(w, "mensaje no encontrado", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"message_id": m.ID, "tenant_id": m.TenantID, "msisdn": m.Msisdn,
		"state": m.State, "smsc_msgid": m.SmscMsgid, "try_count": m.TryCount,
		"segments": m.Segments, "amount": m.Amount,
		"created_at": m.CreatedAt, "updated_at": m.UpdatedAt,
	})
}

type webhookReq struct {
	URL       string   `json:"url"`
	AuthToken string   `json:"auth_token"`
	Events    []string `json:"events"`
	Active    bool     `json:"active"`
}

func (s *Server) tenantOf(r *http.Request) string {
	return r.Header.Get("X-Tenant-ID")
}

func (s *Server) handleListWebhooks(w http.ResponseWriter, r *http.Request) {
	ws, err := s.repo.ListWebhooks(r.Context(), s.tenantOf(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ws)
}

func (s *Server) decodeWebhook(w http.ResponseWriter, r *http.Request) (webhookReq, bool) {
	var req webhookReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		http.Error(w, "json invalido o url requerida", http.StatusBadRequest)
		return req, false
	}
	if req.Events == nil {
		req.Events = []string{}
	}
	return req, true
}

func (s *Server) handleCreateWebhook(w http.ResponseWriter, r *http.Request) {
	req, ok := s.decodeWebhook(w, r)
	if !ok {
		return
	}
	wb := &store.Webhook{TenantID: s.tenantOf(r), URL: req.URL, AuthToken: req.AuthToken,
		Events: req.Events, Active: req.Active}
	if err := s.repo.CreateWebhook(r.Context(), wb); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{"id": wb.ID})
}

func (s *Server) handleUpdateWebhook(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	req, ok := s.decodeWebhook(w, r)
	if !ok {
		return
	}
	err = s.repo.UpdateWebhook(r.Context(), store.Webhook{
		ID: id, TenantID: s.tenantOf(r), URL: req.URL, AuthToken: req.AuthToken,
		Events: req.Events, Active: req.Active,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteWebhook(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	if err := s.repo.DeleteWebhook(r.Context(), id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type tenantReq struct {
	ID      string  `json:"id"`
	Name    string  `json:"name"`
	Status  string  `json:"status"`
	Balance float64 `json:"balance"`
	Mode    string  `json:"mode"`
	ApiKey  string  `json:"api_key"`
}

func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	ts, err := s.repo.ListTenants(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ts)
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var req tenantReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ID == "" {
		http.Error(w, "json invalido o id requerido", http.StatusBadRequest)
		return
	}
	err := s.repo.CreateTenant(r.Context(), &store.Tenant{
		ID: req.ID, Name: req.Name, Status: req.Status,
		Balance: req.Balance, Mode: req.Mode, ApiKey: req.ApiKey,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{"id": req.ID})
}

func (s *Server) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	t, err := s.repo.GetTenant(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, "tenant no encontrado", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(t)
}

func (s *Server) handleCreditTenant(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Amount float64 `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Amount <= 0 {
		http.Error(w, "amount positivo requerido", http.StatusBadRequest)
		return
	}
	bal, err := s.billing.Credit(r.Context(), r.PathValue("id"), req.Amount)
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, "tenant no encontrado", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"balance": bal})
}

func (s *Server) handleListTransactions(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	txns, err := s.repo.ListTransactions(r.Context(), r.PathValue("id"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(txns)
}

type rateTableReq struct {
	Name   string `json:"name"`
	Active bool   `json:"active"`
}

func (s *Server) handleListRateTables(w http.ResponseWriter, r *http.Request) {
	ts, err := s.repo.ListRateTables(r.Context(), s.tenantOf(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ts)
}

func (s *Server) handleCreateRateTable(w http.ResponseWriter, r *http.Request) {
	var req rateTableReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		http.Error(w, "json invalido o name requerido", http.StatusBadRequest)
		return
	}
	tbl := &store.RateTable{TenantID: s.tenantOf(r), Name: req.Name, Active: req.Active}
	if err := s.repo.CreateRateTable(r.Context(), tbl); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{"id": tbl.ID})
}

func (s *Server) handleListRateEntries(w http.ResponseWriter, r *http.Request) {
	tableID, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	es, err := s.repo.ListRateEntries(r.Context(), tableID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(es)
}

type rateEntryReq struct {
	Prefix      string     `json:"prefix"`
	Price       float64    `json:"price"`
	ConnectorID *int       `json:"connector_id"`
	ValidFrom   *time.Time `json:"valid_from"`
	ValidTo     *time.Time `json:"valid_to"`
}

func (s *Server) handleCreateRateEntry(w http.ResponseWriter, r *http.Request) {
	tableID, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	var req rateEntryReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Price < 0 {
		http.Error(w, "json invalido o price negativo", http.StatusBadRequest)
		return
	}
	connID := 0
	if req.ConnectorID != nil {
		connID = *req.ConnectorID
	}
	e := &store.RateEntry{TableID: tableID, Prefix: req.Prefix, Price: req.Price,
		ConnectorID: connID, ValidFrom: req.ValidFrom, ValidTo: req.ValidTo}
	if err := s.repo.CreateRateEntry(r.Context(), e); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{"id": e.ID})
}

func (s *Server) handleDeleteRateEntry(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	if err := s.repo.DeleteRateEntry(r.Context(), id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) Run(ctx context.Context) error {
	addr := s.cfg.HTTPAddr
	if addr == "" {
		addr = ":8080"
	}
	srv := &http.Server{Addr: addr, Handler: s.mux()}
	slog.Info("api escuchando", "addr", addr)
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()
	return srv.ListenAndServe()
}
