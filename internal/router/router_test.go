package router

import (
	"context"
	"testing"
)

type stubStore struct{ rules []Rule }

func (s stubStore) ListRoutingRules(context.Context) ([]Rule, error) { return s.rules, nil }

func TestRouteByPrefix(t *testing.T) {
	ctx := context.Background()
	r := New(stubStore{rules: []Rule{
		{Priority: 1, Prefix: "569", ConnectorID: 10},
		{Priority: 2, ConnectorID: 20}, // default
	}}, Config{})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := r.Route(ctx, "", "56912345678", "")
	if err != nil || id != 10 {
		t.Fatalf("id=%d err=%v", id, err)
	}
	id, err = r.Route(ctx, "", "59899", "")
	if err != nil || id != 20 {
		t.Fatalf("id=%d err=%v", id, err)
	}
}

func TestRouteTenantAndTag(t *testing.T) {
	ctx := context.Background()
	r := New(stubStore{rules: []Rule{
		{Priority: 1, TenantID: "t1", RoutingTag: "TAG", ConnectorID: 30},
		{Priority: 2, TenantID: "t2", ConnectorID: 40},
	}}, Config{})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if id, _ := r.Route(ctx, "t1", "59800", "TAG"); id != 30 {
		t.Fatalf("tag route id=%d", id)
	}
	if id, _ := r.Route(ctx, "t2", "59800", ""); id != 40 {
		t.Fatalf("tenant route id=%d", id)
	}
	if _, err := r.Route(ctx, "t2", "59800", "TAG"); err == nil {
		t.Fatal("esperaba error: t2 no tiene regla con TAG")
	}
}
