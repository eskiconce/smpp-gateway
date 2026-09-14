package smscsim

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/eskiconce/smpp-gateway/internal/smpp"
)

type Config struct {
	Addr      string
	SystemID  string
	Password  string
	EnableDLR bool
}

type Server struct {
	cfg   Config
	ln    net.Listener
	conns sync.Map
	seq   uint32
	mu    sync.Mutex
}

func New(cfg Config) *Server {
	return &Server{cfg: cfg}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.ln = ln
	go s.acceptLoop()
	return nil
}

func (s *Server) Addr() string { return s.ln.Addr().String() }

func (s *Server) Close() error {
	if s.ln != nil {
		s.ln.Close()
	}
	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	for {
		p, err := readPDU(br)
		if err != nil {
			return
		}
		switch p.Header.ID {
		case smpp.BindTransceiver:
			bf, err := smpp.ParseBindTransceiver(p.Body)
			status := smpp.ESME_ROK
			if err != nil || bf.SystemID != s.cfg.SystemID || bf.Password != s.cfg.Password {
				status = smpp.ESME_RINVPASWD
			}
			conn.Write(smpp.Encode(smpp.NewBindTransceiverResp(p.Header.Seq, status, s.cfg.SystemID)))
			s.conns.Store(conn, true)
		case smpp.SubmitSM:
			msgid := fmt.Sprintf("smsc-%d", p.Header.Seq)
			conn.Write(smpp.Encode(smpp.NewSubmitSMResp(p.Header.Seq, smpp.ESME_ROK, msgid)))
			if s.cfg.EnableDLR {
				dlr := "id:" + msgid + " sub:001 dlvrd:001 submit date:2609121230 done date:2609121231 stat:DELIVRD err:000 text:"
				conn.Write(smpp.Encode(smpp.NewDeliverSM(s.nextSeq(), s.cfg.SystemID, "", dlr)))
			}
		case smpp.EnquireLink:
			conn.Write(smpp.Encode(smpp.NewEnquireLinkResp(p.Header.Seq)))
		default:
			resp := &smpp.PDU{Header: smpp.Head(p.Header.Seq, smpp.GenericNack), Body: make([]byte, 4)}
			smpp.PutU32(resp.Body, uint32(smpp.ESME_RUNKNOWNERR))
			conn.Write(smpp.Encode(resp))
		}
	}
}

func (s *Server) nextSeq() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	return s.seq
}

func readPDU(br *bufio.Reader) (*smpp.PDU, error) {
	head := make([]byte, 4)
	if _, err := io.ReadFull(br, head); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(head))
	if n < smpp.HeaderLen {
		return nil, fmt.Errorf("pdu demasiado corto: %d", n)
	}
	rest := make([]byte, n-4)
	if _, err := io.ReadFull(br, rest); err != nil {
		return nil, err
	}
	return smpp.Decode(append(head, rest...))
}
