package worker

import (
	"context"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type Sender interface {
	Submit(dest, text string, regDelivery uint8) (uint32, error)
}

type Worker struct {
	q       queue.Queue
	repo    store.MessageRepo
	sess    Sender
	pending map[uint32]queue.Item
	dlr     map[string]string
	backoff func(try int) time.Duration
}

type Option func(*Worker)

func WithBackoff(f func(try int) time.Duration) Option {
	return func(w *Worker) { w.backoff = f }
}

func NewWorker(q queue.Queue, repo store.MessageRepo, opts ...Option) *Worker {
	w := &Worker{
		q: q, repo: repo,
		pending: map[uint32]queue.Item{},
		dlr:     map[string]string{},
		backoff: defaultBackoff,
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
	w.dlr[msgid] = it.ID
}

func (w *Worker) OnDLR(msgid, stat string) {
	id, ok := w.dlr[msgid]
	if !ok {
		return
	}
	state := "delivered"
	if stat != "DELIVRD" {
		state = "undeliv"
	}
	w.repo.UpdateState(context.Background(), id, state)
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
