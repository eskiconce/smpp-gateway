package router

import (
	"context"
	"math/rand"
	"testing"
)

type stubStore struct {
	rules  []Rule
	groups []Group
}

func (s *stubStore) ListRoutingRules(context.Context) ([]Rule, error)  { return s.rules, nil }
func (s *stubStore) ListGroups(context.Context) ([]Group, error)       { return s.groups, nil }

func TestRouteDirectAndDefault(t *testing.T) {
	ctx := context.Background()
	r := New(&stubStore{rules: []Rule{
		{ID: 1, Priority: 1, Prefix: "569", ConnectorID: 10},
		{ID: 2, Priority: 2, ConnectorID: 20},
	}}, Config{})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := r.Route(ctx, RouteInput{Msisdn: "56912345678"})
	if err != nil || len(res.Connectors) != 1 || res.Connectors[0] != 10 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
	if res.RuleID != 1 {
		t.Fatalf("ruleID=%d", res.RuleID)
	}
	res, err = r.Route(ctx, RouteInput{Msisdn: "59899"})
	if err != nil || res.Connectors[0] != 20 {
		t.Fatalf("default res=%+v err=%v", res, err)
	}
}

func TestRouteFromFilter(t *testing.T) {
	ctx := context.Background()
	r := New(&stubStore{rules: []Rule{
		{ID: 1, Priority: 1, From: "ventas", ConnectorID: 11},
		{ID: 2, Priority: 2, ConnectorID: 12},
	}}, Config{})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if res, _ := r.Route(ctx, RouteInput{SourceAddr: "ventas", Msisdn: "5691"}); res.Connectors[0] != 11 {
		t.Fatalf("from+tenant res=%+v", res)
	}
	if res, _ := r.Route(ctx, RouteInput{SourceAddr: "otro", Msisdn: "5691"}); res.Connectors[0] != 12 {
		t.Fatalf("from no match res=%+v", res)
	}
}

func TestRouteByGroupWeighted(t *testing.T) {
	ctx := context.Background()
	r := New(&stubStore{
		rules:  []Rule{{ID: 9, Priority: 1, Prefix: "569", GroupID: 3}},
		groups: []Group{{ID: 3, Name: "operadores", Members: []GroupMember{{ConnectorID: 30, Weight: 5}, {ConnectorID: 31, Weight: 3}, {ConnectorID: 32, Weight: 2}}}},
	}, Config{Rand: rand.New(rand.NewSource(42))})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}

	hits := map[int]int{}
	for i := 0; i < 1000; i++ {
		res, err := r.Route(ctx, RouteInput{Msisdn: "5691"})
		if err != nil {
			t.Fatal(err)
		}
		if res.RuleID != 9 {
			t.Fatalf("ruleID=%d", res.RuleID)
		}
		hits[res.Connectors[0]]++
		if len(res.Connectors) != 3 {
			t.Fatalf("candidatos=%v (deben ser 3)", res.Connectors)
		}
	}

	p30 := 100 * hits[30] / 1000
	p31 := 100 * hits[31] / 1000
	if p30 < 40 || p30 > 60 || p31 < 20 || p31 > 40 {
		t.Fatalf("distribucion fuera de rango: %d/%d/%d", hits[30], hits[31], hits[32])
	}
}

func TestRouteNoRoute(t *testing.T) {
	ctx := context.Background()
	r := New(&stubStore{}, Config{})
	if err := r.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Route(ctx, RouteInput{Msisdn: "599999"}); err != ErrNoRoute {
		t.Fatalf("err=%v, esperaba ErrNoRoute", err)
	}
}
