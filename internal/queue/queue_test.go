package queue

import "testing"

func TestKey(t *testing.T) {
	if got := Key(3, 1); got != "stream:con:3:1" {
		t.Fatalf("key=%q", got)
	}
}
