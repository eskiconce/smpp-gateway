package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

func TestMessagesEndpoint(t *testing.T) {
	ctx := context.Background()
	repo := store.NewMemory()
	q := queue.NewMemory()
	key := "stream:con:5:0"
	go q.Consume(ctx, key, "g", func(queue.Item) error { return nil })

	r := router.New(stubRouterStore{}, router.Config{})
	_ = r.Load(ctx)
	p := pipeline.NewPipeline(repo, q, r)
	srv := New(config.Config{}, p, repo)

	body := `{"to": "569123", "text": "hola"}`
	req := httptest.NewRequest("POST", "/api/v1/messages", bytes.NewReader([]byte(body)))
	req.Header.Set("X-Tenant-ID", "t1")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)

	if rr.Code != 200 {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	var out struct {
		MessageID string `json:"message_id"`
		Segments  int    `json:"segments"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.MessageID == "" || out.Segments < 1 {
		t.Fatalf("out %+v", out)
	}
}

type stubRouterStore struct{}

func (stubRouterStore) ListRoutingRules(context.Context) ([]router.Rule, error) {
	return []router.Rule{{ID: 1, Priority: 1, ConnectorID: 5}}, nil
}
func (stubRouterStore) ListGroups(context.Context) ([]router.Group, error) { return nil, nil }
