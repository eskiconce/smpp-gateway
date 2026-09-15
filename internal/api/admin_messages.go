package api

import (
	"net/http"
	"strconv"

	"github.com/eskiconce/smpp-gateway/internal/store"
)

func (s *Server) handleListMessages(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	f := store.MessageFilter{
		TenantID:    q.Get("tenant_id"),
		Msisdn:      q.Get("msisdn"),
		State:       q.Get("state"),
		ConnectorID: atoiOrZero(q.Get("connector_id")),
		Limit:       limit,
		Offset:      offset,
	}
	items, total, err := s.repo.ListMessages(r.Context(), f)
	if err != nil {
		http.Error(w, "no se pudo listar", http.StatusInternalServerError)
		return
	}
	writeJSON(w, 200, map[string]any{"total": total, "items": mapMessages(items)})
}

func atoiOrZero(v string) int {
	n, _ := strconv.Atoi(v)
	return n
}

func mapMessages(items []store.Message) []map[string]any {
	out := make([]map[string]any, 0, len(items))
	for _, m := range items {
		out = append(out, map[string]any{
			"id": m.ID, "tenant_id": m.TenantID, "source_addr": m.SourceAddr,
			"msisdn": m.Msisdn, "text": m.Text, "segments": m.Segments,
			"connector_id": m.ConnectorID, "route_id": m.RouteID, "state": m.State,
			"try_count": m.TryCount, "smsc_msgid": m.SmscMsgid, "amount": m.Amount,
			"created_at": m.CreatedAt, "updated_at": m.UpdatedAt,
		})
	}
	return out
}
