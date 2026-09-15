package queue

import (
	"context"
	"testing"
)

func TestKey(t *testing.T) {
	if got := Key(3, 1); got != "stream:con:3:1" {
		t.Fatalf("key=%q", got)
	}
}

func TestItemAmountRoundtrip(t *testing.T) {
	q := NewMemory()
	ctx := context.Background()
	it := Item{ID: "m1", TenantID: "t1", ConnectorID: 1, Amount: 1.25}
	if err := q.Enqueue(ctx, Key(1, 0), it); err != nil {
		t.Fatal(err)
	}
	var got Item
	done := make(chan struct{})
	go q.Consume(ctx, Key(1, 0), "g", func(x Item) error { got = x; close(done); return nil })
	<-done
	if got.Amount != 1.25 {
		t.Fatalf("amount=%v", got.Amount)
	}
}
