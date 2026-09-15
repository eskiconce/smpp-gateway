package esme

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/billing"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type fakePipeline struct {
	msgID string
	segs  int
	err   error
}

func (f *fakePipeline) Submit(_ context.Context, _ pipeline.Outgoing) (string, int, error) {
	return f.msgID, f.segs, f.err
}

func start(t *testing.T, srv *Server) {
	t.Helper()
	go func() { _ = srv.Run(context.Background()) }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.Addr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if srv.Addr() == "" {
		t.Fatal("esme server no arranco")
	}
}

func TestESMEBindOK(t *testing.T) {
	repo := store.NewMemory()
	_ = repo.CreateTenant(context.Background(), &store.Tenant{
		ID: "t1", Status: "active",
		SmppSystemID: "esme-01", SmppPassword: "pass", Mode: "prepaid", Balance: 10,
	})
	fp := &fakePipeline{msgID: "gw-001", segs: 1}
	srv := New(Config{Addr: "127.0.0.1:0", EnquireLinkInterval: 30 * time.Second}, repo, fp)
	start(t, srv)
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)

	bind := smpp.NewBindTransceiver(1, "esme-01", "pass", "", 0, 0, "")
	conn.Write(smpp.Encode(bind))
	p, _ := readPDU(br)
	if p.Header.Status != smpp.ESME_ROK {
		t.Fatalf("bind status=%x", p.Header.Status)
	}

	sub, _ := smpp.NewSubmitSM(2, "1234", "569123", "hola", 0, 1)
	conn.Write(smpp.Encode(sub))
	p, _ = readPDU(br)
	if p.Header.Status != smpp.ESME_ROK {
		t.Fatalf("submit status=%x", p.Header.Status)
	}
	msgid, _ := smpp.ParseSubmitSMResp(p.Body)
	if msgid != "gw-001" {
		t.Fatalf("msgid=%q", msgid)
	}
}

func TestESMEBindFail(t *testing.T) {
	repo := store.NewMemory()
	_ = repo.CreateTenant(context.Background(), &store.Tenant{
		ID: "t1", Status: "active",
		SmppSystemID: "esme-01", SmppPassword: "pass",
	})
	fp := &fakePipeline{msgID: "gw-001", segs: 1}
	srv := New(Config{Addr: "127.0.0.1:0"}, repo, fp)
	start(t, srv)
	defer srv.Close()

	conn, _ := net.Dial("tcp", srv.Addr())
	defer conn.Close()
	br := bufio.NewReader(conn)

	bind := smpp.NewBindTransceiver(1, "esme-01", "wrong", "", 0, 0, "")
	conn.Write(smpp.Encode(bind))
	p, _ := readPDU(br)
	if p.Header.Status != smpp.ESME_RINVPASWD {
		t.Fatalf("esperaba RINVPASWD, got %x", p.Header.Status)
	}
}

func TestESMESaldoInsuficiente(t *testing.T) {
	repo := store.NewMemory()
	_ = repo.CreateTenant(context.Background(), &store.Tenant{
		ID: "t1", Status: "active", SmppSystemID: "esme-01", SmppPassword: "pass",
		Mode: "prepaid", Balance: 0,
	})
	fp := &fakePipeline{err: billing.ErrInsufficientBalance}
	srv := New(Config{Addr: "127.0.0.1:0"}, repo, fp)
	start(t, srv)
	defer srv.Close()

	conn, _ := net.Dial("tcp", srv.Addr())
	defer conn.Close()
	br := bufio.NewReader(conn)

	bind := smpp.NewBindTransceiver(1, "esme-01", "pass", "", 0, 0, "")
	conn.Write(smpp.Encode(bind))
	p, _ := readPDU(br)
	if p.Header.Status != smpp.ESME_ROK {
		t.Fatalf("bind status=%x", p.Header.Status)
	}

	sub, _ := smpp.NewSubmitSM(2, "1234", "569123", "hola", 0, 1)
	conn.Write(smpp.Encode(sub))
	p, _ = readPDU(br)
	if p.Header.Status != smpp.ESME_0512 {
		t.Fatalf("esperaba 0512, got %x", p.Header.Status)
	}
}

func TestDeliverSMToSession(t *testing.T) {
	repo := store.NewMemory()
	_ = repo.CreateTenant(context.Background(), &store.Tenant{
		ID: "t1", Status: "active",
		SmppSystemID: "esme-01", SmppPassword: "pass",
	})
	fp := &fakePipeline{msgID: "gw-001", segs: 1}
	srv := New(Config{Addr: "127.0.0.1:0"}, repo, fp)
	start(t, srv)
	defer srv.Close()

	conn, _ := net.Dial("tcp", srv.Addr())
	defer conn.Close()
	br := bufio.NewReader(conn)

	bind := smpp.NewBindTransceiver(1, "esme-01", "pass", "", 0, 0, "")
	conn.Write(smpp.Encode(bind))
	p, _ := readPDU(br)
	if p.Header.Status != smpp.ESME_ROK {
		t.Fatalf("bind status=%x", p.Header.Status)
	}

	sessions := srv.SessionsByTenant("t1")
	if len(sessions) != 1 {
		t.Fatalf("sesiones=%d", len(sessions))
	}
	if err := sessions[0].Deliver("1234", "569123", "dlr text"); err != nil {
		t.Fatal(err)
	}
	p, _ = readPDU(br)
	if p.Header.ID != smpp.DeliverSM {
		t.Fatalf("esperaba DeliverSM, got %x", p.Header.ID)
	}
	f, _ := smpp.ParseDeliverSM(p.Body)
	if f.ShortMessage != "dlr text" {
		t.Fatalf("dlr text=%q", f.ShortMessage)
	}
}
