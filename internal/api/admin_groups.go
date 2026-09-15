package api

import (
	"encoding/json"
	"net/http"

	"github.com/eskiconce/smpp-gateway/internal/router"
)

func (s *Server) handleListGroups(w http.ResponseWriter, r *http.Request) {
	groups, err := s.groups.ListGroups(r.Context())
	if err != nil {
		http.Error(w, "no se pudo listar", http.StatusInternalServerError)
		return
	}
	out := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		members := make([]map[string]any, 0, len(g.Members))
		for _, m := range g.Members {
			members = append(members, map[string]any{"connector_id": m.ConnectorID, "weight": m.Weight})
		}
		out = append(out, map[string]any{"id": g.ID, "name": g.Name, "members": members})
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleCreateGroup(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, "name requerido", http.StatusBadRequest)
		return
	}
	id, err := s.groups.CreateGroup(r.Context(), req.Name)
	if err != nil {
		http.Error(w, "no se pudo crear", http.StatusInternalServerError)
		return
	}
	writeJSON(w, 201, map[string]any{"id": id})
}

func (s *Server) handleDeleteGroup(w http.ResponseWriter, r *http.Request) {
	id, err := readID(r)
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	if err := s.groups.DeleteGroup(r.Context(), id); err != nil {
		http.Error(w, "grupo no encontrado", http.StatusNotFound)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) handleSetGroupMembers(w http.ResponseWriter, r *http.Request) {
	id, err := readID(r)
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	var req struct {
		Members []struct {
			ConnectorID int `json:"connector_id"`
			Weight      int `json:"weight"`
		} `json:"members"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	members := make([]router.GroupMember, 0, len(req.Members))
	for _, m := range req.Members {
		members = append(members, router.GroupMember{ConnectorID: m.ConnectorID, Weight: m.Weight})
	}
	if err := s.groups.SetGroupMembers(r.Context(), id, members); err != nil {
		http.Error(w, "grupo no encontrado", http.StatusNotFound)
		return
	}
	writeJSON(w, 200, map[string]any{"id": id})
}
