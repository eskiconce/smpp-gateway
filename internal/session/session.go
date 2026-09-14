package session

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"regexp"
	"sync"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"golang.org/x/time/rate"
)

type Config struct {
	ConnectorID          int
	Host                 string
	Port                 int
	SystemID             string
	Password             string
	SourceAddr           string
	SourceTON, SourceNPI uint8
	DestTON, DestNPI     uint8
	EnquireLinkInterval  time.Duration
	MaxConcurrency       int
	MsgPerSecond         float64
	TLS                  bool
}

type Handler interface {
	OnSubmitResp(seq uint32, status smpp.CommandStatus, msgid string)
	OnDLR(msgid, stat string)
}

type Session struct {
	cfg Config
	h   Handler

	mu     sync.Mutex
	conn   net.Conn
	seq    uint32
	closed bool
	once   sync.Once

	wmu sync.Mutex

	lim  *rate.Limiter
	sem  chan struct{}
	done chan struct{}
}

func New(cfg Config, h Handler) *Session {
	lim := rate.NewLimiter(rate.Every(time.Second/time.Duration(cfg.MsgPerSecond)), int(cfg.MsgPerSecond))
	if cfg.MsgPerSecond <= 0 {
		lim = rate.NewLimiter(rate.Inf, 1)
	}
	if cfg.MaxConcurrency <= 0 {
		cfg.MaxConcurrency = 1
	}
	return &Session{
		cfg: cfg, h: h,
		lim: lim, sem: make(chan struct{}, cfg.MaxConcurrency),
		done: make(chan struct{}),
	}
}

func (s *Session) Dial(ctx context.Context) error {
	delay := 300 * time.Millisecond
	for {
		err := s.tryDial()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > 5*time.Second {
			delay = 5 * time.Second
		}
	}
}

func (s *Session) tryDial() error {
	addr := net.JoinHostPort(s.cfg.Host, fmt.Sprintf("%d", s.cfg.Port))
	var conn net.Conn
	var err error
	if s.cfg.TLS {
		conn, err = tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return err
	}
	s.conn = conn
	if err := s.bind(conn); err != nil {
		conn.Close()
		return err
	}
	go s.readerLoop(conn)
	go s.enquireLoop(conn)
	return nil
}

func (s *Session) bind(conn net.Conn) error {
	s.seq = 1
	pdu := smpp.NewBindTransceiver(s.seq, s.cfg.SystemID, s.cfg.Password, "", s.cfg.SourceTON, s.cfg.SourceNPI, "")
	if _, err := conn.Write(smpp.Encode(pdu)); err != nil {
		return err
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	p, err := readPDU(conn)
	conn.SetReadDeadline(time.Time{})
	if err != nil {
		return err
	}
	if p.Header.ID != smpp.BindTransceiverResp {
		return fmt.Errorf("bind: id inesperado %x", p.Header.ID)
	}
	if p.Header.Status != smpp.ESME_ROK {
		return fmt.Errorf("bind: status %x", uint32(p.Header.Status))
	}
	return nil
}

func readPDU(conn net.Conn) (*smpp.PDU, error) {
	head := make([]byte, 4)
	if _, err := io.ReadFull(conn, head); err != nil {
		return nil, err
	}
	n := int(smpp.GetU32(head))
	if n < smpp.HeaderLen {
		return nil, fmt.Errorf("pdu corto: %d", n)
	}
	rest := make([]byte, n-4)
	if _, err := io.ReadFull(conn, rest); err != nil {
		return nil, err
	}
	return smpp.Decode(append(head, rest...))
}

func (s *Session) readerLoop(conn net.Conn) {
	for {
		p, err := readPDU(conn)
		if err != nil {
			s.markDead()
			return
		}
		switch p.Header.ID {
		case smpp.SubmitSMResp:
			msgid, _ := smpp.ParseSubmitSMResp(p.Body)
			s.h.OnSubmitResp(p.Header.Seq, p.Header.Status, msgid)
		case smpp.DeliverSM:
			f, err := smpp.ParseDeliverSM(p.Body)
			var stat string
			if err == nil && f.DataCoding == 0 && len(f.ShortMessage) > 0 {
				stat, _ = smpp.ParseDLR(f.ShortMessage)
			}
			id := dlrID(f.ShortMessage, p.Header.Seq)
			s.h.OnDLR(id, stat)
			ack := &smpp.PDU{Header: smpp.Head(p.Header.Seq, smpp.DeliverSMResp)}
			s.write(smpp.Encode(ack))
		case smpp.EnquireLinkResp:
			// ok
		}
	}
}

func (s *Session) enquireLoop(conn net.Conn) {
	iv := s.cfg.EnquireLinkInterval
	if iv <= 0 {
		iv = 30 * time.Second
	}
	ticker := time.NewTicker(iv)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.write(smpp.Encode(smpp.NewEnquireLink(s.nextSeq())))
		}
	}
}

func (s *Session) Submit(dest, text string, regDelivery uint8) (uint32, error) {
	if err := s.lim.Wait(context.Background()); err != nil {
		return 0, err
	}
	select {
	case s.sem <- struct{}{}:
	case <-s.done:
		return 0, fmt.Errorf("sesion cerrada")
	}
	defer func() { <-s.sem }()

	seq := s.nextSeq()
	pdu, err := smpp.NewSubmitSM(seq, s.cfg.SourceAddr, dest, text, 0, regDelivery)
	if err != nil {
		return 0, err
	}
	return seq, s.write(smpp.Encode(pdu))
}

func (s *Session) write(b []byte) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.mu.Lock()
	conn := s.conn
	closed := s.closed
	s.mu.Unlock()
	if closed || conn == nil {
		return fmt.Errorf("sin conexion")
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := conn.Write(b)
	conn.SetWriteDeadline(time.Time{})
	return err
}

func (s *Session) nextSeq() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	return s.seq
}

func (s *Session) markDead() {
	s.once.Do(func() {
		s.mu.Lock()
		if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
		s.closed = true
		s.mu.Unlock()
		close(s.done)
	})
}

func (s *Session) Close() error {
	s.markDead()
	return nil
}

var dlrIDRe = regexp.MustCompile(`\bid:(\S+)\b`)

func dlrID(dlrText string, fallback uint32) string {
	m := dlrIDRe.FindStringSubmatch(dlrText)
	if m == nil {
		return fmt.Sprintf("smsc-%d", fallback)
	}
	return m[1]
}
