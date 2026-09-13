package smpp

import (
	"bytes"
	"testing"
)

func TestEncodeHeader(t *testing.T) {
	p := &PDU{Header: Header{Length: 16, ID: EnquireLink, Status: ESME_ROK, Seq: 7}}
	got := Encode(p)
	want := []byte{0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07}
	if !bytes.Equal(got, want) {
		t.Fatalf("encode header\ngot  %x\nwant %x", got, want)
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	p := &PDU{
		Header: Header{ID: SubmitSM, Status: ESME_ROK, Seq: 9},
		Body:   []byte{0x00, 0x41, 0x62, 0x63},
	}
	raw := Encode(p)
	got, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.ID != SubmitSM || got.Header.Seq != 9 {
		t.Fatalf("header %+v", got.Header)
	}
	if !bytes.Equal(got.Body, p.Body) {
		t.Fatalf("body %x", got.Body)
	}
}

func TestDecodeError(t *testing.T) {
	if _, err := Decode([]byte{0, 0, 0}); err == nil {
		t.Fatal("esperaba error por longitud < 16")
	}
}

func TestEncodeEmitsTLV(t *testing.T) {
	p := &PDU{
		Header: Head(9, SubmitSM),
		Body:   []byte{0x00, 0x41, 0x62, 0x63},
	}
	p.SetTLV(0x0424, []byte("payload"))
	raw := Encode(p)
	want := []byte{0x04, 0x24, 0x00, 0x07, 'p', 'a', 'y', 'l', 'o', 'a', 'd'}
	tail := raw[len(raw)-len(want):]
	if !bytes.Equal(tail, want) {
		t.Fatalf("tlv tail %x, want %x", tail, want)
	}
}
