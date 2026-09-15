package api

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

const maskPassword = "********"

func (s *Server) handleListConnectors(w http.ResponseWriter, r *http.Request) {
	list, err := s.conns.ListConnectors(r.Context())
	if err != nil {
		http.Error(w, "no se pudo listar", http.StatusInternalServerError)
		return
	}
	out := make([]map[string]any, 0, len(list))
	for _, c := range list {
		c.Password = maskPassword
		out = append(out, connectorJSON(c))
	}
	writeJSON(w, 200, out)
}

func (s *Server) handleCreateConnector(w http.ResponseWriter, r *http.Request) {
	var c store.Connector
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	if c.Name == "" || (c.Type != "smpp" && c.Type != "http") || c.Host == "" {
		http.Error(w, "name, type y host requeridos", http.StatusBadRequest)
		return
	}
	id, err := s.conns.CreateConnector(r.Context(), &c)
	if err != nil {
		http.Error(w, "no se pudo crear", http.StatusInternalServerError)
		return
	}
	writeJSON(w, 201, map[string]any{"id": id})
}

func (s *Server) handleUpdateConnector(w http.ResponseWriter, r *http.Request) {
	id, err := readID(r)
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	current, err := s.conns.GetConnector(r.Context(), id)
	if err != nil {
		http.Error(w, "conector no encontrado", http.StatusNotFound)
		return
	}
	var c store.Connector
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		http.Error(w, "json invalido", http.StatusBadRequest)
		return
	}
	if c.Password == "" || c.Password == maskPassword {
		c.Password = current.Password
	}
	c.ID = id
	if err := s.conns.UpdateConnector(r.Context(), &c); err != nil {
		http.Error(w, "no se pudo actualizar", http.StatusInternalServerError)
		return
	}
	writeJSON(w, 200, map[string]any{"id": id})
}

func (s *Server) handleDeleteConnector(w http.ResponseWriter, r *http.Request) {
	id, err := readID(r)
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	if err := s.conns.DeleteConnector(r.Context(), id); err != nil {
		http.Error(w, "conector no encontrado", http.StatusNotFound)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) handleTestConnector(w http.ResponseWriter, r *http.Request) {
	id, err := readID(r)
	if err != nil {
		http.Error(w, "id invalido", http.StatusBadRequest)
		return
	}
	c, err := s.conns.GetConnector(r.Context(), id)
	if err != nil {
		http.Error(w, "conector no encontrado", http.StatusNotFound)
		return
	}
	if !c.Enabled {
		writeJSON(w, 200, map[string]any{"ok": false, "detail": "conector deshabilitado"})
		return
	}
	var ok bool
	var detail string
	if c.Type == "http" {
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get("http://" + c.Host + "/")
		if err != nil {
			detail = "GET fallido: " + err.Error()
		} else {
			resp.Body.Close()
			ok = resp.StatusCode >= 200 && resp.StatusCode < 500
			detail = "HTTP " + resp.Status
		}
	} else {
		addr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			detail = "dial fallido: " + err.Error()
		} else {
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			resp, err := testEnquire(conn)
			if err != nil {
				detail = "enquire_link fallido: " + err.Error()
			} else {
				ok = true
				detail = fmt.Sprintf("resp %d", resp)
			}
		}
	}
	writeJSON(w, 200, map[string]any{"ok": ok, "detail": detail})
}

func testEnquire(nc net.Conn) (int, error) {
	pdu := smpp.NewEnquireLink(1)
	if _, err := nc.Write(smpp.Encode(pdu)); err != nil {
		return 0, err
	}
	buf := make([]byte, 1024)
	n, err := nc.Read(buf)
	if err != nil {
		return 0, err
	}
	p, err := smpp.Decode(buf[:n])
	if err != nil {
		return 0, err
	}
	if p.Header.ID != smpp.EnquireLinkResp {
		return 0, fmt.Errorf("respuesta inesperada id=%d", p.Header.ID)
	}
	return int(p.Header.Status), nil
}

func connectorJSON(c store.Connector) map[string]any {
	return map[string]any{
		"id": c.ID, "name": c.Name, "type": c.Type, "host": c.Host, "port": c.Port,
		"system_id": c.SystemID, "password": c.Password, "bind_mode": c.BindMode,
		"source_addr": c.SourceAddr, "source_ton": c.SourceTON, "source_npi": c.SourceNPI,
		"dest_ton": c.DestTON, "dest_npi": c.DestNPI, "concurrency": c.Concurrency,
		"max_msg_per_sec": c.MaxMsgPerSec, "enquire_link_interval": c.EnquireLinkInterval,
		"tls": c.TLS, "enabled": c.Enabled,
	}
}
