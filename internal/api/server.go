package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type Server struct {
	cfg  config.Config
	p    *pipeline.Pipeline
	repo store.MessageRepo
}

func New(cfg config.Config, p *pipeline.Pipeline, repo store.MessageRepo) *Server {
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
