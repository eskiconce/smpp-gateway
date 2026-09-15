package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/dlr"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type Sender interface {
	Submit(dest, text string, regDelivery uint8) (uint32, error)
}

type Biller interface {
	Debit(ctx context.Context, tenantID, messageID string, amount float64) error
}

type Worker struct {
	q       queue.Queue
	repo    store.MessageRepo
	sess    Sender
	pending map[uint32]queue.Item
	dlrProc *dlr.Processor
	backoff func(try int) time.Duration
	biller  Biller
	log     *slog.Logger
}

type Option func(*Worker)

func WithBackoff(f func(try int) time.Duration) Option {
	return func(w *Worker) { w.backoff = f }
}

func WithDLR(p *dlr.Processor) Option {
	return func(w *Worker) { w.dlrProc = p }
}

func WithBiller(b Biller) Option {
	return func(w *Worker) { w.biller = b }
}

func NewWorker(q queue.Queue, repo store.MessageRepo, opts ...Option) *Worker {
	w := &Worker{
		q: q, repo: repo,
		pending: map[uint32]queue.Item{},
		backoff: defaultBackoff,
		log:     slog.Default(),
	}
	for _, o := range opts {
		o(w)
	}
	return w
}

func defaultBackoff(try int) time.Duration {
	d := 500 * time.Millisecond << uint(try)
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

func (w *Worker) SetSession(s Sender) { w.sess = s }

func (w *Worker) Handle(ctx context.Context, it queue.Item) error {
	w.repo.IncrementTry(ctx, it.ID)
	w.repo.SetConnector(ctx, it.ID, it.ConnectorID)
	seq, err := w.sess.Submit(it.Msisdn, it.Text, 1)
	if err != nil {
		w.fallback(it, true)
		return nil
	}
	w.pending[seq] = it
	return nil
}

func (w *Worker) OnSubmitResp(seq uint32, status smpp.CommandStatus, msgid string) {
	it, ok := w.pending[seq]
	if !ok {
		return
	}
	delete(w.pending, seq)
	ctx := context.Background()
	if status != smpp.ESME_ROK {
		w.repo.SetSmscMsgid(ctx, it.ID, msgid)
		w.fallback(it, false)
		return
	}
	w.repo.SetSmscMsgid(ctx, it.ID, msgid)
	if w.dlrProc != nil {
		_ = w.dlrProc.Register(ctx, msgid, it.ID)
	}
	if w.biller != nil && it.Amount > 0 {
		if err := w.biller.Debit(ctx, it.TenantID, it.ID, it.Amount); err != nil {
			w.log.Warn("debit fallido", "message_id", it.ID, "err", err)
		}
	}
}

func (w *Worker) OnDLR(msgid, stat string) {
	if w.dlrProc == nil {
		return
	}
	w.dlrProc.Handle(context.Background(), msgid, stat)
}

func (w *Worker) fallback(it queue.Item, transportErr bool) {
	next := it.Try + 1
	if next >= len(it.Candidates) {
		state := "undeliv"
		if !transportErr {
			state = "rejected"
		}
		w.repo.UpdateState(context.Background(), it.ID, state)
		return
	}
	nxt := it
	nxt.ConnectorID = it.Candidates[next]
	nxt.Try = next
	delay := w.backoff(it.Try)
	go func() {
		if delay > 0 {
			time.Sleep(delay)
		}
		w.q.Enqueue(context.Background(), queue.Key(nxt.ConnectorID, nxt.Priority), nxt)
	}()
}
