package esme

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/billing"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type Config struct {
	Addr                string
	EnquireLinkInterval time.Duration
}

type Submitter interface {
	Submit(ctx context.Context, out pipeline.Outgoing) (msgID string, segments int, err error)
}

type session struct {
	tenantID string
	systemID string
	conn     net.Conn
	br       *bufio.Reader
	seq      uint32
	mu       sync.Mutex
	done     chan struct{}
}

func (se *session) Deliver(source, dest, dlrText string) error {
	se.mu.Lock()
	defer se.mu.Unlock()
	se.seq++
	_, err := se.conn.Write(smpp.Encode(smpp.NewDeliverSM(se.seq, source, dest, dlrText)))
	return err
}

type Server struct {
	cfg      Config
	repo     store.TenantRepo
	p        Submitter
	ln       net.Listener
	lnMu     sync.Mutex
	sessions sync.Map
	wg       sync.WaitGroup
}

func New(cfg Config, repo store.TenantRepo, p Submitter) *Server {
	if cfg.EnquireLinkInterval <= 0 {
		cfg.EnquireLinkInterval = 30 * time.Second
	}
	return &Server{cfg: cfg, repo: repo, p: p}
}

func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	s.lnMu.Lock()
	s.ln = ln
	s.lnMu.Unlock()
	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return nil
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(ctx, conn)
		}()
	}
}

func (s *Server) Addr() string {
	s.lnMu.Lock()
	defer s.lnMu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

func (s *Server) Close() error {
	s.lnMu.Lock()
	ln := s.ln
	s.lnMu.Unlock()
	if ln != nil {
		ln.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Server) SessionsByTenant(tenantID string) []*session {
	var out []*session
	s.sessions.Range(func(_, v any) bool {
		se := v.(*session)
		if se.tenantID == tenantID {
			out = append(out, se)
		}
		return true
	})
	return out
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	se := &session{conn: conn, br: br, done: make(chan struct{})}

	p, err := readPDU(br)
	if err != nil {
		return
	}
	switch p.Header.ID {
	case smpp.BindTransceiver, smpp.BindReceiver, smpp.BindTransmitter:
		bf, err := smpp.ParseBindTransceiver(p.Body)
		if err != nil {
			conn.Write(smpp.Encode(smpp.NewBindTransceiverResp(p.Header.Seq, smpp.ESME_RBINDFAIL, "")))
			return
		}
		tenant, err := s.repo.GetTenantBySMPPSystemID(ctx, bf.SystemID)
		if err != nil || tenant.SmppPassword != bf.Password {
			conn.Write(smpp.Encode(smpp.NewBindTransceiverResp(p.Header.Seq, smpp.ESME_RINVPASWD, "")))
			return
		}
		se.tenantID = tenant.ID
		se.systemID = bf.SystemID
		conn.Write(smpp.Encode(smpp.NewBindTransceiverResp(p.Header.Seq, smpp.ESME_ROK, se.systemID)))
	default:
		h := smpp.Head(p.Header.Seq, smpp.GenericNack)
		h.Status = smpp.ESME_RBINDFAIL
		conn.Write(smpp.Encode(&smpp.PDU{Header: h}))
		return
	}

	s.sessions.Store(se.systemID, se)
	defer s.sessions.Delete(se.systemID)
	slog.Info("esme bind", "system_id", se.systemID, "tenant", se.tenantID)

	for {
		p, err := readPDU(br)
		if err != nil {
			return
		}
		switch p.Header.ID {
		case smpp.SubmitSM:
			s.handleSubmit(ctx, se, p)
		case smpp.EnquireLink:
			conn.Write(smpp.Encode(smpp.NewEnquireLinkResp(p.Header.Seq)))
		case smpp.Unbind:
			conn.Write(smpp.Encode(&smpp.PDU{Header: smpp.Head(p.Header.Seq, smpp.UnbindResp)}))
			return
		default:
			h := smpp.Head(p.Header.Seq, smpp.GenericNack)
			h.Status = smpp.ESME_RUNKNOWNERR
			conn.Write(smpp.Encode(&smpp.PDU{Header: h}))
		}
	}
}

func (s *Server) handleSubmit(ctx context.Context, se *session, p *smpp.PDU) {
	f, err := smpp.ParseSubmitSM(p.Body)
	if err != nil {
		h := smpp.Head(p.Header.Seq, smpp.SubmitSMResp)
		h.Status = smpp.ESME_RUNKNOWNERR
		se.conn.Write(smpp.Encode(&smpp.PDU{Header: h}))
		return
	}

	id, _, err := s.p.Submit(ctx, pipeline.Outgoing{
		TenantID:      se.tenantID,
		SourceAddr:    f.SourceAddr,
		Msisdn:        f.DestAddr,
		Text:          f.ShortMessage,
		DataCoding:    int(f.DataCoding),
		SourceChannel: "smpp",
	})
	if err != nil {
		status := smpp.ESME_RUNKNOWNERR
		if errors.Is(err, billing.ErrInsufficientBalance) {
			status = smpp.ESME_0512
		}
		h := smpp.Head(p.Header.Seq, smpp.SubmitSMResp)
		h.Status = status
		se.conn.Write(smpp.Encode(&smpp.PDU{Header: h}))
		return
	}

	se.conn.Write(smpp.Encode(smpp.NewSubmitSMResp(p.Header.Seq, smpp.ESME_ROK, id)))
}

func readPDU(br *bufio.Reader) (*smpp.PDU, error) {
	head := make([]byte, 4)
	if _, err := io.ReadFull(br, head); err != nil {
		return nil, err
	}
	n := int(smpp.GetU32(head))
	if n < smpp.HeaderLen {
		return nil, fmt.Errorf("pdu corto: %d", n)
	}
	rest := make([]byte, n-4)
	if _, err := io.ReadFull(br, rest); err != nil {
		return nil, err
	}
	return smpp.Decode(append(head, rest...))
}
