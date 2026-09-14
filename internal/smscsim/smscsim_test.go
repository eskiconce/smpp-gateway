package smscsim

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/smpp"
)

func TestSimHandshakeAndSubmit(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	// bind transceiver
	conn.Write(smpp.Encode(smpp.NewBindTransceiver(1, "esp", "secreto", "", 0, 0, "")))
	p, err := readPDU(br)
	if err != nil {
		t.Fatal(err)
	}
	if p.Header.ID != smpp.BindTransceiverResp || p.Header.Status != smpp.ESME_ROK {
		t.Fatalf("bind resp %+v", p.Header)
	}

	// submit
	conn.Write(smpp.Encode(mustSubmit(t, "test", "569123", "hola")))
	p, err = readPDU(br)
	if err != nil {
		t.Fatal(err)
	}
	msgid, err := smpp.ParseSubmitSMResp(p.Body)
	if err != nil {
		t.Fatal(err)
	}
	if msgid != "smsc-2" {
		t.Fatalf("msgid=%q", msgid)
	}

	// DLR deliver_sm
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p, err = readPDU(br)
		if err == nil && p.Header.ID == smpp.DeliverSM {
			f, _ := smpp.ParseDeliverSM(p.Body)
			stat, _ := smpp.ParseDLR(f.ShortMessage)
			if stat == "DELIVRD" {
				return
			}
		}
	}
	t.Fatal("no recibio DLR")
}

func mustSubmit(t *testing.T, src, dst, text string) *smpp.PDU {
	t.Helper()
	p, err := smpp.NewSubmitSM(2, src, dst, text, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSimSubmitRejected(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
		RespondSubmitStatus: smpp.ESME_RSYSERR})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	conn.Write(smpp.Encode(smpp.NewBindTransceiver(1, "esp", "secreto", "", 0, 0, "")))
	p, _ := readPDU(br)
	if p.Header.ID != smpp.BindTransceiverResp || p.Header.Status != smpp.ESME_ROK {
		t.Fatalf("bind resp %+v", p.Header)
	}

	conn.Write(smpp.Encode(mustSubmit(t, "test", "569123", "hola")))
	p, err = readPDU(br)
	if err != nil {
		t.Fatal(err)
	}
	if p.Header.Status != smpp.ESME_RSYSERR {
		t.Fatalf("status=%d", p.Header.Status)
	}
}

func TestSimDropOnSubmit(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", DropOnSubmit: true})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	conn.Write(smpp.Encode(smpp.NewBindTransceiver(1, "esp", "secreto", "", 0, 0, "")))
	// read bind response first — server responds to bind before dropping on submit
	p, err := readPDU(br)
	if err != nil {
		t.Fatal(err)
	}
	if p.Header.Status != smpp.ESME_ROK {
		t.Fatalf("bind status=%d", p.Header.Status)
	}
	// now send submit — server will drop connection
	conn.Write(smpp.Encode(mustSubmit(t, "test", "569123", "hola")))
	p, err = readPDU(br)
	conn.Close()
	if err == nil {
		t.Fatalf("esperaba cierre de conexion, se recibio %+v", p)
	}
}

func TestSimDLRStatus(t *testing.T) {
	srv := New(Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto",
		EnableDLR: true, DLRStatus: "UNDELIV"})
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	conn.Write(smpp.Encode(smpp.NewBindTransceiver(1, "esp", "secreto", "", 0, 0, "")))
	p, err := readPDU(br)
	if err != nil || p.Header.ID != smpp.BindTransceiverResp {
		t.Fatalf("bind=%+v err=%v", p.Header, err)
	}

	conn.Write(smpp.Encode(mustSubmit(t, "test", "569123", "hola")))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p, err = readPDU(br)
		if err != nil {
			t.Fatal(err)
		}
		if p.Header.ID == smpp.DeliverSM {
			f, err := smpp.ParseDeliverSM(p.Body)
			if err != nil {
				t.Fatal(err)
			}
			stat, _ := smpp.ParseDLR(f.ShortMessage)
			if stat != "UNDELIV" {
				t.Fatalf("stat=%q", stat)
			}
			return
		}
	}
	t.Fatal("no recibio DLR con UNDELIV")
}
