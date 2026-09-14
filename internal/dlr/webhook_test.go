package dlr

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/store"
)

func TestWebhookNotifierPosts(t *testing.T) {
	var mu sync.Mutex
	var got map[string]any
	var gotAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		json.Unmarshal(b, &got)
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	repo := store.NewMemory()
	if err := repo.CreateWebhook(context.Background(), &store.Webhook{
		TenantID: "t1", URL: srv.URL, AuthToken: "tok1",
		Events: []string{"delivered", "undeliv"}, Active: true,
	}); err != nil {
		t.Fatal(err)
	}

	n := NewWebhookNotifier(repo, 2*time.Second)
	err := n.Notify(context.Background(), Event{
		TenantID: "t1", MessageID: "m1", SmscMsgid: "smsc-1",
		Msisdn: "569123", State: "delivered", Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := got != nil
		mu.Unlock()
		if ok {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatal("no llego el POST")
	}
	if got["event"] != "delivered" || got["message_id"] != "m1" || got["state"] != "delivered" {
		t.Fatalf("payload=%+v", got)
	}
	if gotAuth != "Bearer tok1" {
		t.Fatalf("auth=%q", gotAuth)
	}
}

func TestWebhookNotifierIgnoresDownWebhook(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	url := srv.URL
	srv.Close()

	repo := store.NewMemory()
	if err := repo.CreateWebhook(context.Background(), &store.Webhook{
		TenantID: "t1", URL: url, Events: []string{"delivered"}, Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	n := NewWebhookNotifier(repo, time.Second)
	if err := n.Notify(context.Background(), Event{
		TenantID: "t1", MessageID: "m1", State: "delivered",
	}); err != nil {
		t.Fatalf("no debia fallar por webhook caido: %v", err)
	}
}
