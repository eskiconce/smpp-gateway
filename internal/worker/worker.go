package worker

import (
	"context"

	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/smpp"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

type Sender interface {
	Submit(dest, text string, regDelivery uint8) (uint32, error)
}

type Worker struct {
	q    queue.Queue
	repo store.MessageRepo
	sess Sender

	pending map[uint32]string // seq -> messageID
	dlr     map[string]string // smscMsgid -> messageID
}

func NewWorker(q queue.Queue, repo store.MessageRepo) *Worker {
	return &Worker{q: q, repo: repo, pending: map[uint32]string{}, dlr: map[string]string{}}
}

func (w *Worker) SetSession(s Sender) { w.sess = s }

func (w *Worker) Handle(ctx context.Context, it queue.Item) error {
	seq, err := w.sess.Submit(it.Msisdn, it.Text, 1)
	if err != nil {
		w.repo.UpdateState(ctx, it.ID, "failed")
		return nil
	}
	w.pending[seq] = it.ID
	return nil
}

// OnSubmitResp implementa session.Handler.
func (w *Worker) OnSubmitResp(seq uint32, status smpp.CommandStatus, msgid string) {
	id, ok := w.pending[seq]
	if !ok {
		return
	}
	delete(w.pending, seq)
	ctx := context.Background()
	if status != smpp.ESME_ROK {
		w.repo.UpdateState(ctx, id, "rejected")
		return
	}
	w.repo.SetSmscMsgid(ctx, id, msgid)
	w.dlr[msgid] = id // cache en caliente; la persistencia real llega en M3
}

// OnDLR implementa session.Handler.
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
