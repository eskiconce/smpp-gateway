package smpp

import "testing"

func TestESME0512(t *testing.T) {
	if ESME_0512 != 0x0512 {
		t.Fatalf("ESME_0512=%x, esperaba 0x0512", ESME_0512)
	}
	p := NewSubmitSMResp(1, ESME_0512, "msg-01")
	b := Encode(p)
	p2, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if p2.Header.Status != ESME_0512 {
		t.Fatalf("status=%x", p2.Header.Status)
	}
}
