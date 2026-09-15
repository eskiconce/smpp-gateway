package store

import (
	"context"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/router"
)

func TestUserRepoMemory(t *testing.T) {
	ctx := context.Background()
	repo := NewMemory()
	if err := repo.CreateUser(ctx, &User{Username: "admin", PasswordHash: "h", Role: "superadmin"}); err != nil {
		t.Fatal(err)
	}
	u, err := repo.GetUserByUsername(ctx, "admin")
	if err != nil || u.Role != "superadmin" {
		t.Fatalf("got=%+v err=%v", u, err)
	}
	if _, err := repo.GetUserByUsername(ctx, "nadie"); err != ErrNotFound {
		t.Fatalf("esperaba ErrNotFound, got %v", err)
	}
	users, _ := repo.ListUsers(ctx)
	if len(users) != 1 {
		t.Fatalf("users=%d", len(users))
	}
	if err := repo.DeleteUser(ctx, u.ID); err != nil {
		t.Fatal(err)
	}
}

func TestConnectorRepoMemory(t *testing.T) {
	ctx := context.Background()
	repo := NewMemory()
	id, err := repo.CreateConnector(ctx, &Connector{Name: "c1", Type: "smpp", Host: "h", Port: 2775})
	if err != nil || id == 0 {
		t.Fatalf("id=%d err=%v", id, err)
	}
	g, err := repo.GetConnector(ctx, id)
	if err != nil || g.Name != "c1" {
		t.Fatalf("got=%+v err=%v", g, err)
	}
	g.Port = 4000
	if err := repo.UpdateConnector(ctx, g); err != nil {
		t.Fatal(err)
	}
	got, _ := repo.GetConnector(ctx, id)
	if got.Port != 4000 {
		t.Fatalf("port=%d", got.Port)
	}
	list, _ := repo.ListConnectors(ctx)
	if len(list) != 1 {
		t.Fatalf("list=%d", len(list))
	}
	if err := repo.DeleteConnector(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetConnector(ctx, id); err != ErrNotFound {
		t.Fatalf("esperaba ErrNotFound, got %v", err)
	}
}

func TestGroupRuleRepoMemory(t *testing.T) {
	ctx := context.Background()
	repo := NewMemory()
	cid, _ := repo.CreateConnector(ctx, &Connector{Name: "c1", Type: "smpp", Host: "h", Port: 2775})
	gid, err := repo.CreateGroup(ctx, "grupo-a")
	if err != nil || gid == 0 {
		t.Fatalf("gid=%d err=%v", gid, err)
	}
	if err := repo.SetGroupMembers(ctx, gid, []router.GroupMember{{ConnectorID: cid, Weight: 100}}); err != nil {
		t.Fatal(err)
	}
	groups, _ := repo.ListGroups(ctx)
	if len(groups) != 1 || len(groups[0].Members) != 1 || groups[0].Members[0].Weight != 100 {
		t.Fatalf("groups=%+v", groups)
	}
	rid, err := repo.CreateRoutingRule(ctx, router.Rule{Priority: 1, Prefix: "569", GroupID: gid})
	if err != nil || rid == 0 {
		t.Fatalf("rid=%d err=%v", rid, err)
	}
	rules, _ := repo.ListRoutingRules(ctx)
	if len(rules) != 1 || rules[0].Prefix != "569" {
		t.Fatalf("rules=%+v", rules)
	}
	if err := repo.UpdateRoutingRulePriority(ctx, rid, 5); err != nil {
		t.Fatal(err)
	}
	rules, _ = repo.ListRoutingRules(ctx)
	if rules[0].Priority != 5 {
		t.Fatalf("priority=%d", rules[0].Priority)
	}
	if err := repo.DeleteRoutingRule(ctx, rid); err != nil {
		t.Fatal(err)
	}
	if err := repo.DeleteGroup(ctx, gid); err != nil {
		t.Fatal(err)
	}
}

func TestListMessagesAndStatsMemory(t *testing.T) {
	ctx := context.Background()
	repo := NewMemory()
	for i := 0; i < 3; i++ {
		if err := repo.CreateMessage(ctx, &Message{
			ID: "m" + itoa(i), TenantID: "t1", Msisdn: "569" + itoa(i),
			State: "delivered", ConnectorID: 1, Segments: 1, Text: "hola",
		}); err != nil {
			t.Fatal(err)
		}
	}
	items, total, err := repo.ListMessages(ctx, MessageFilter{Msisdn: "56", State: "delivered", Limit: 2})
	if err != nil || total != 3 || len(items) != 2 {
		t.Fatalf("items=%d total=%d err=%v", len(items), total, err)
	}
	byState, err := repo.CountByState(ctx, "t1", time.Time{})
	if err != nil || byState["delivered"] != 3 {
		t.Fatalf("byState=%v err=%v", byState, err)
	}
	byConn, err := repo.CountByConnector(ctx, "", time.Time{})
	if err != nil || len(byConn) != 1 || byConn[0].Count != 3 {
		t.Fatalf("byConn=%+v err=%v", byConn, err)
	}
	n, err := repo.CountMessages(ctx, "", time.Time{})
	if err != nil || n != 3 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}
