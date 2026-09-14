package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type repos interface {
	store.MessageRepo
	store.WebhookRepo
}

type Server struct {
	cfg  config.Config
	p    *pipeline.Pipeline
	repo repos
}

func New(cfg config.Config, p *pipeline.Pipeline, repo repos) *Server {
	return &Server{cfg: cfg, p: p, repo: repo}
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
	return mux
}

func (s *Server) Handler() http.Handler { return s.mux() }

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req submitReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	if req.To == "" || req.Text == "" {
		http.Error(w, "to y text requeridos", http.StatusBadRequest)
		return
	}
	tenant := r.Header.Get("X-Tenant-ID")
	id, segments, err := s.p.Submit(r.Context(), pipeline.Outgoing{
		TenantID: tenant, SourceAddr: "api", Msisdn: req.To,
		Text: req.Text, RoutingTag: req.RoutingTag, Priority: 0,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
		"segments": m.Segments, "created_at": m.CreatedAt, "updated_at": m.UpdatedAt,
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
