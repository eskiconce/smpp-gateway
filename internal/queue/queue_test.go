package queue

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestMemoryQueueRoundTrip(t *testing.T) {
	ctx := context.Background()
	q := NewMemory()
	key := "stream:con:1:0"
	it := Item{ID: "m1", TenantID: "t1", Msisdn: "569xxx", Text: "hi", ConnectorID: 1, Priority: 0}

	if err := q.Enqueue(ctx, key, it); err != nil {
		t.Fatal(err)
	}
	got := make(chan Item, 1)
	go func() { q.Consume(ctx, key, "g", func(i Item) error { got <- i; return nil }) }()
	select {
	case i := <-got:
		if i.ID != "m1" || i.Text != "hi" {
			t.Fatalf("got %+v", i)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout esperando item")
	}
}

func TestRedisQueueWithMiniredis(t *testing.T) {
	mr := miniredis.RunT(t)
	ctx := context.Background()
	q, err := NewRedis("redis://" + mr.Addr() + "/0")
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	key := "stream:con:2:0"
	it := Item{ID: "r1", ConnectorID: 2, Text: "via redis"}
	if err := q.Enqueue(ctx, key, it); err != nil {
		t.Fatal(err)
	}

	done := make(chan Item, 1)
	go func() {
		q.Consume(ctx, key, "conn-2", func(i Item) error { done <- i; return nil })
	}()
	select {
	case i := <-done:
		if i.ID != "r1" || strings.TrimSpace(i.Text) != "via redis" {
			t.Fatalf("got %+v", i)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout esperando item de redis")
	}
}
