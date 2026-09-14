package smpp

import (
	"strings"
	"testing"
)

func TestSplitTextShort(t *testing.T) {
	segs, n, err := SplitText("hello")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(segs) != 1 || segs[0] != "hello" {
		t.Fatalf("short text: n=%d segs=%v", n, segs)
	}
}

func TestSplitTextExactly160(t *testing.T) {
	text := strings.Repeat("a", 160)
	segs, n, err := SplitText(text)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(segs) != 1 {
		t.Fatalf("exactly 160: n=%d len=%d", n, len(segs))
	}
}

func TestSplitText161(t *testing.T) {
	text := strings.Repeat("b", 161)
	segs, n, err := SplitText(text)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || len(segs) != 2 {
		t.Fatalf("161 chars: n=%d len=%d", n, len(segs))
	}
	if len(segs[0]) != 153 || len(segs[1]) != 8 {
		t.Fatalf("parts len: %d, %d", len(segs[0]), len(segs[1]))
	}
}

func TestSplitText400(t *testing.T) {
	segs, n, err := SplitText(strings.Repeat("x", 400))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || len(segs) != 3 {
		t.Fatalf("400 chars: n=%d len=%d", n, len(segs))
	}
}

func TestConcatUDH(t *testing.T) {
	_udh := ConcatUDH(1, 3, 1)
	if len(_udh) != 6 {
		t.Fatalf("udh len=%d", len(_udh))
	}
}
