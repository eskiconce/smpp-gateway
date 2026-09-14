package smpp

import (
	"strings"
	"testing"
)

func TestSubmitSMRoundTrip(t *testing.T) {
	p, err := NewSubmitSM(1, "shield", "56912345678", "hola mundo", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	raw := Encode(p)
	got, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	f, err := ParseSubmitSM(got.Body)
	if err != nil {
		t.Fatal(err)
	}
	if f.SourceAddr != "shield" || f.DestAddr != "56912345678" || f.ShortMessage != "hola mundo" {
		t.Fatalf("fields %+v", f)
	}
	if f.RegisteredDelivery != 1 {
		t.Fatalf("registered_delivery=%d", f.RegisteredDelivery)
	}
}

func TestSubmitSMRespRoundTrip(t *testing.T) {
	p := NewSubmitSMResp(2, ESME_ROK, "smsc-123")
	got, err := Decode(Encode(p))
	if err != nil {
		t.Fatal(err)
	}
	msgid, err := ParseSubmitSMResp(got.Body)
	if err != nil || msgid != "smsc-123" {
		t.Fatalf("msgid=%q err=%v", msgid, err)
	}
}

func TestSubmitSMLongUsesMessagePayload(t *testing.T) {
	long := strings.Repeat("á", 300)
	p, err := NewSubmitSM(3, "shield", "569", long, 8, 1)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := Decode(Encode(p))
	if err != nil {
		t.Fatal(err)
	}
	f, err := ParseSubmitSM(dec.Body)
	if err != nil {
		t.Fatal(err)
	}
	if f.ShortMessage != "" || string(f.MessagePayload) != long {
		t.Fatalf("short=%q payload=%d", f.ShortMessage, len(f.MessagePayload))
	}
}

func TestBindTransceiverRoundTrip(t *testing.T) {
	p := NewBindTransceiver(5, "esp", "secreto", "", 0, 0, "")
	dec, err := Decode(Encode(p))
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseBindTransceiver(dec.Body)
	if err != nil {
		t.Fatal(err)
	}
	if b.SystemID != "esp" || b.Password != "secreto" {
		t.Fatalf("bind %+v", b)
	}
}

func TestSplitText(t *testing.T) {
	segs, n, err := SplitText(strings.Repeat("x", 400))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || len(segs) != 3 {
		t.Fatalf("n=%d len=%d", n, len(segs))
	}
}
