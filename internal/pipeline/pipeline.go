package pipeline

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/store"
	"github.com/google/uuid"
)

type Router interface {
	Route(ctx context.Context, in router.RouteInput) (router.RouteResult, error)
}

type Outgoing struct {
	TenantID   string
	SourceAddr string
	Msisdn     string
	Text       string
	RoutingTag string
	Priority   int
	DataCoding int
}

type Biller interface {
	Price(ctx context.Context, tenantID string, connectorID int, msisdn string, segments int) (float64, error)
	Reserve(ctx context.Context, tenantID string, amount float64) error
}

type nilBiller struct{}

func (nilBiller) Price(context.Context, string, int, string, int) (float64, error) { return 0, nil }
func (nilBiller) Reserve(context.Context, string, float64) error                   { return nil }

var NilBiller Biller = nilBiller{}

type Option func(*Pipeline)

func WithBiller(b Biller) Option {
	return func(p *Pipeline) { p.biller = b }
}

type Pipeline struct {
	repo   store.MessageRepo
	q      queue.Queue
	r      Router
	biller Biller
}

func NewPipeline(repo store.MessageRepo, q queue.Queue, r Router, opts ...Option) *Pipeline {
	p := &Pipeline{repo: repo, q: q, r: r, biller: NilBiller}
	for _, o := range opts {
		o(p)
	}
	return p
}

var tagRe = regexp.MustCompile(`\[[A-Za-z0-9_]+\]`)

func splitTag(text string) (clean, tag string) {
	loc := tagRe.FindStringIndex(text)
	if loc == nil {
		return text, ""
	}
	return text[:loc[0]] + text[loc[1]:], text[loc[0]+1 : loc[1]-1]
}

func (p *Pipeline) Submit(ctx context.Context, out Outgoing) (string, int, error) {
	if out.Msisdn == "" || out.Text == "" {
		return "", 0, errors.New("msisdn y text requeridos")
	}
	text, tag := out.Text, out.RoutingTag
	if tag == "" {
		text, tag = splitTag(out.Text)
	}

	_, segments, err := smpp.SplitText(text)
	if err != nil {
		return "", 0, err
	}

	res, err := p.r.Route(ctx, router.RouteInput{
		TenantID:   out.TenantID,
		SourceAddr: out.SourceAddr,
		Msisdn:     out.Msisdn,
		RoutingTag: tag,
	})
	if err != nil {
		return "", 0, err
	}
	if len(res.Connectors) == 0 {
		return "", 0, router.ErrNoRoute
	}

	amount := 0.0
	if _, ok := p.biller.(nilBiller); !ok {
		amount, err = p.biller.Price(ctx, out.TenantID, res.Connectors[0], out.Msisdn, segments)
		if err != nil {
			return "", 0, err
		}
		if err := p.biller.Reserve(ctx, out.TenantID, amount); err != nil {
			return "", 0, err
		}
	}

	msgID := uuid.NewString()
	msg := &store.Message{
		ID: msgID, TenantID: out.TenantID, SourceAddr: out.SourceAddr,
		Msisdn: out.Msisdn, Text: text, Segments: segments,
		ConnectorID: res.Connectors[0], RouteID: res.RuleID, State: "buffered", CreatedAt: time.Now(),
		Amount: amount,
	}
	if err := p.repo.CreateMessage(ctx, msg); err != nil {
		return "", 0, err
	}

	item := queue.Item{
		ID: msgID, TenantID: out.TenantID, ConnectorID: res.Connectors[0],
		Priority: out.Priority, DataCoding: out.DataCoding,
		Msisdn: out.Msisdn, SourceAddr: out.SourceAddr, Text: text,
		Candidates: res.Connectors, Try: 0, Amount: amount,
	}
	if err := p.q.Enqueue(ctx, queue.Key(res.Connectors[0], out.Priority), item); err != nil {
		return "", 0, err
	}
	if err := p.repo.UpdateState(ctx, msgID, "enqueued"); err != nil {
		return "", 0, err
	}
	return msgID, segments, nil
}
