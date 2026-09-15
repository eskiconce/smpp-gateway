package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/store"
)

type metricsSnapshot struct {
	ByState     map[string]int           `json:"by_state"`
	ByConnector []store.ConnectorCount   `json:"by_connector"`
	Today       int                      `json:"today"`
	Last5Min    int                      `json:"last_5min"`
	GeneratedAt time.Time                `json:"generated_at"`
}

func (s *Server) metricsSnapshot(ctx context.Context) (metricsSnapshot, error) {
	dayStart := time.Now().Truncate(24 * time.Hour)
	fiveMin := time.Now().Add(-5 * time.Minute)
	byState, err := s.stats.CountByState(ctx, "", dayStart)
	if err != nil {
		return metricsSnapshot{}, err
	}
	byConn, err := s.stats.CountByConnector(ctx, "", dayStart)
	if err != nil {
		return metricsSnapshot{}, err
	}
	today, err := s.stats.CountMessages(ctx, "", dayStart)
	if err != nil {
		return metricsSnapshot{}, err
	}
	last5, err := s.stats.CountMessages(ctx, "", fiveMin)
	if err != nil {
		return metricsSnapshot{}, err
	}
	return metricsSnapshot{
		ByState: byState, ByConnector: byConn,
		Today: today, Last5Min: last5, GeneratedAt: time.Now(),
	}, nil
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap, err := s.metricsSnapshot(r.Context())
	if err != nil {
		http.Error(w, "no se pudieron calcular metricas", http.StatusInternalServerError)
		return
	}
	writeJSON(w, 200, snap)
}

func (s *Server) handleMetricsStream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming no soportado", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	ticker := time.NewTicker(s.metricsInterval)
	defer ticker.Stop()
	ctx := r.Context()
	for {
		snap, err := s.metricsSnapshot(ctx)
		if err != nil {
			fmt.Fprintf(w, "event: error\ndata: %v\n\n", err)
		} else {
			b, _ := json.Marshal(snap)
			fmt.Fprintf(w, "data: %s\n\n", b)
		}
		fl.Flush()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
