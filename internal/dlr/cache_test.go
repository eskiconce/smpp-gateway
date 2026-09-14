package dlr

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestMemCacheRoundTrip(t *testing.T) {
	c := NewMemCache()
	ctx := context.Background()
	if err := c.Register(ctx, "smsc-1", "m1"); err != nil {
		t.Fatal(err)
	}
	v, err := c.Lookup(ctx, "smsc-1")
	if err != nil || v != "m1" {
		t.Fatalf("lookup=%q err=%v", v, err)
	}
	if err := c.Delete(ctx, "smsc-1"); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.Lookup(ctx, "smsc-1"); v != "" {
		t.Fatalf("no borro: %q", v)
	}
}

func TestRedisCacheRoundTrip(t *testing.T) {
	mr := miniredis.RunT(t)
	c, err := NewRedisCache("redis://"+mr.Addr(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	if err := c.Register(ctx, "x", "v"); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.Lookup(ctx, "x"); v != "v" {
		t.Fatalf("lookup=%q", v)
	}
	if got := mr.Keys(); len(got) != 1 || got[0] != "dlr:pend:x" {
		t.Fatalf("keys=%v", got)
	}
	if v, _ := c.Lookup(ctx, "no-existe"); v != "" {
		t.Fatalf("esperaba vacio, got %q", v)
	}
}
