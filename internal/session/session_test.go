package session

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/smscsim"
)

type spyHandler struct {
	mu    sync.Mutex
	resps []resp
	dlrs  []string
}

type resp struct {
	status smpp.CommandStatus
	msgid  string
}

func (s *spyHandler) OnSubmitResp(seq uint32, status smpp.CommandStatus, msgid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resps = append(s.resps, resp{status, msgid})
}

func (s *spyHandler) OnDLR(msgid, stat string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dlrs = append(s.dlrs, msgid+":"+stat)
}

func (s *spyHandler) countResps() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.resps) }
func (s *spyHandler) lastResp() resp {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resps[len(s.resps)-1]
}
func (s *spyHandler) dlrCount() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.dlrs) }

func TestSessionSubmitAndDLR(t *testing.T) {
	sim := smscsim.New(smscsim.Config{Addr: "127.0.0.1:0", SystemID: "esp", Password: "secreto", EnableDLR: true})
	if err := sim.Start(); err != nil {
		t.Fatal(err)
	}
	defer sim.Close()

	h := &spyHandler{}
	s := New(Config{
		Host: "127.0.0.1", Port: portOf(sim.Addr()), SystemID: "esp", Password: "secreto",
		SourceAddr: "shield", MsgPerSecond: 100, MaxConcurrency: 2,
	}, h)
	if err := s.Dial(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.Submit("569123", "hola", 1); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for h.countResps() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if h.countResps() == 0 {
		t.Fatal("sin submit_sm_resp")
	}
	if r := h.lastResp(); r.status != smpp.ESME_ROK || r.msgid == "" {
		t.Fatalf("resp %+v", r)
	}
	if h.dlrCount() == 0 {
		t.Fatal("sin DLR")
	}
}

func portOf(addr string) int {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(portStr)
	return n
}
