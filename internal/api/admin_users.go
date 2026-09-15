package api

import (
	"encoding/json"
	"net/http"

	"github.com/eskiconce/smpp-gateway/internal/auth"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

func (s *Server) handleListUsers(w http.ResponseWriter, r *http.Request) {
	list, err := s.users.ListUsers(r.Context())
	if err != nil {
		http.Error(w, "no se pudo listar", http.StatusInternalServerError)
		return
	}
	out := make([]map[string]any, 0, len(list))
	for _, u := range list {
		out = append(out, map[string]any{
			"id": u.ID, "username": u.Username, "role": u.Role,
			"tenant_id": u.TenantID, "created_at": u.CreatedAt,
		})
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Role     string `json:"role"`
		TenantID string `json:"tenant_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	if req.Username == "" || req.Password == "" {
		http.Error(w, "username y password requeridos", http.StatusBadRequest)
		return
	}
	if _, ok := auth.RoleRank[req.Role]; !ok {
		http.Error(w, "rol invalido (superadmin|admin|viewer)", http.StatusBadRequest)
		return
	}
	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		http.Error(w, "no se pudo hashear", http.StatusInternalServerError)
		return
	}
	u := &store.User{Username: req.Username, PasswordHash: hash, Role: req.Role, TenantID: req.TenantID}
	if err := s.users.CreateUser(r.Context(), u); err != nil {
		http.Error(w, "no se pudo crear (username puede existir)", http.StatusConflict)
		return
	}
	writeJSON(w, 201, map[string]any{"id": u.ID})
}

func (s *Server) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	id, err := readID(r)
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	if err := s.users.DeleteUser(r.Context(), id); err != nil {
		http.Error(w, "usuario no encontrado", http.StatusNotFound)
		return
	}
	w.WriteHeader(204)
}
