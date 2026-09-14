package pipeline

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/store"
	"github.com/google/uuid"
)

type Outgoing struct {
	TenantID   string
	SourceAddr string
	Msisdn     string
	Text       string
	RoutingTag string
	Priority   int
	DataCoding int
}

type Router interface {
	Route(ctx context.Context, tenantID, msisdn, routingTag string) (int, error)
}

type Pipeline struct {
	repo store.MessageRepo
	q    queue.Queue
	r    Router
}

func NewPipeline(repo store.MessageRepo, q queue.Queue, r Router) *Pipeline {
	return &Pipeline{repo: repo, q: q, r: r}
}

var ErrEmpty = errors.New("pipeline: mensaje vacio")

func (p *Pipeline) Submit(ctx context.Context, out Outgoing) (string, int, error) {
	if out.Text == "" || out.Msisdn == "" {
		return "", 0, ErrEmpty
	}
	_, segments, err := smpp.SplitText(out.Text)
	if err != nil {
		return "", 0, err
	}

	connectorID, err := p.r.Route(ctx, out.TenantID, out.Msisdn, out.RoutingTag)
	if err != nil {
		return "", 0, err
	}

	msgID := uuid.NewString()
	msg := &store.Message{
		ID: msgID, TenantID: out.TenantID, SourceAddr: out.SourceAddr,
		Msisdn: out.Msisdn, Text: out.Text, Segments: segments,
		ConnectorID: connectorID, State: "buffered", CreatedAt: time.Now(),
	}
	if err := p.repo.CreateMessage(ctx, msg); err != nil {
		return "", 0, err
	}

	item := queue.Item{
		ID: msgID, TenantID: out.TenantID, ConnectorID: connectorID,
		Priority: out.Priority, DataCoding: out.DataCoding,
		Msisdn: out.Msisdn, SourceAddr: out.SourceAddr, Text: out.Text,
	}
	if err := p.q.Enqueue(ctx, queueKey(connectorID, out.Priority), item); err != nil {
		return "", 0, err
	}
	if err := p.repo.UpdateState(ctx, msgID, "enqueued"); err != nil {
		return "", 0, err
	}
	return msgID, segments, nil
}

func queueKey(connectorID, priority int) string {
	return "stream:con:" + strconv.Itoa(connectorID) + ":" + strconv.Itoa(priority)
}
