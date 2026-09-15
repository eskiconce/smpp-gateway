package api

import (
	"encoding/json"
	"net/http"

	"github.com/eskiconce/smpp-gateway/internal/router"
)

func (s *Server) handleListRules(w http.ResponseWriter, r *http.Request) {
	rules, err := s.rules.ListRoutingRules(r.Context())
	if err != nil {
		http.Error(w, "no se pudo listar", http.StatusInternalServerError)
		return
	}
	out := make([]map[string]any, 0, len(rules))
	for _, rule := range rules {
		out = append(out, ruleJSON(rule))
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleCreateRule(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Priority    int    `json:"priority"`
		TenantID    string `json:"tenant_id"`
		From        string `json:"from"`
		Prefix      string `json:"prefix"`
		Regex       string `json:"regex"`
		RoutingTag  string `json:"routing_tag"`
		ConnectorID int    `json:"connector_id"`
		GroupID     int    `json:"group_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	if req.ConnectorID == 0 && req.GroupID == 0 {
		http.Error(w, "se requiere connector_id o group_id", http.StatusBadRequest)
		return
	}
	id, err := s.rules.CreateRoutingRule(r.Context(), router.Rule{
		Priority: req.Priority, TenantID: req.TenantID, From: req.From,
		Prefix: req.Prefix, Regex: req.Regex, RoutingTag: req.RoutingTag,
		ConnectorID: req.ConnectorID, GroupID: req.GroupID,
	})
	if err != nil {
		http.Error(w, "no se pudo crear", http.StatusInternalServerError)
		return
	}
	writeJSON(w, 201, map[string]any{"id": id})
}

func (s *Server) handleDeleteRule(w http.ResponseWriter, r *http.Request) {
	id, err := readID(r)
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	if err := s.rules.DeleteRoutingRule(r.Context(), id); err != nil {
		http.Error(w, "regla no encontrada", http.StatusNotFound)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) handleUpdateRulePriority(w http.ResponseWriter, r *http.Request) {
	id, err := readID(r)
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	var req struct {
		Priority int `json:"priority"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	if err := s.rules.UpdateRoutingRulePriority(r.Context(), id, req.Priority); err != nil {
		http.Error(w, "regla no encontrada", http.StatusNotFound)
		return
	}
	writeJSON(w, 200, map[string]any{"id": id, "priority": req.Priority})
}

func ruleJSON(rule router.Rule) map[string]any {
	return map[string]any{
		"id": rule.ID, "priority": rule.Priority,
		"tenant_id": rule.TenantID, "from": rule.From, "prefix": rule.Prefix,
		"regex": rule.Regex, "routing_tag": rule.RoutingTag,
		"connector_id": rule.ConnectorID, "group_id": rule.GroupID,
	}
}
