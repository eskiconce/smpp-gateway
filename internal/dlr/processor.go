package dlr

import (
	"context"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/store"
)

type Event struct {
	TenantID, MessageID, SmscMsgid, Msisdn, State string
	Timestamp                                     time.Time
}

type Notifier interface {
	Notify(ctx context.Context, ev Event) error
}

type Processor struct {
	cache    Cache
	repo     store.MessageRepo
	notifier Notifier
}

func NewProcessor(cache Cache, repo store.MessageRepo, notifier Notifier) *Processor {
	return &Processor{cache: cache, repo: repo, notifier: notifier}
}

func (p *Processor) Register(ctx context.Context, smscMsgid, messageID string) error {
	return p.cache.Register(ctx, smscMsgid, messageID)
}

func (p *Processor) Handle(ctx context.Context, smscMsgid, stat string) error {
	id, err := p.cache.Lookup(ctx, smscMsgid)
	if err != nil || id == "" {
		return err
	}
	state := MapStat(stat)
	if state == "" {
		return nil
	}
	m, err := p.repo.GetMessage(ctx, id)
	if err != nil {
		return err
	}
	if err := p.repo.UpdateState(ctx, id, state); err != nil {
		return err
	}
	_ = p.cache.Delete(ctx, smscMsgid)
	if p.notifier != nil {
		go p.notifier.Notify(context.Background(), Event{
			TenantID: m.TenantID, MessageID: m.ID, SmscMsgid: smscMsgid,
			Msisdn: m.Msisdn, State: state, Timestamp: time.Now(),
		})
	}
	return nil
}
