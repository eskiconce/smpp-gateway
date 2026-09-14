package dlr

import (
	"context"
	"log/slog"
	"time"

	"github.com/eskiconce/smpp-gateway/internal/store"
)

type Reconciler struct {
	repo     store.MessageRepo
	notifier Notifier
	timeout  time.Duration
	interval time.Duration
}

func NewReconciler(repo store.MessageRepo, notifier Notifier, timeout, interval time.Duration) *Reconciler {
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	if interval <= 0 {
		interval = time.Minute
	}
	return &Reconciler{repo: repo, notifier: notifier, timeout: timeout, interval: interval}
}

func (r *Reconciler) ReconcileOnce(ctx context.Context) (int, error) {
	stale, err := r.repo.ListStaleAccepted(ctx, time.Now().Add(-r.timeout))
	if err != nil {
		return 0, err
	}
	for _, m := range stale {
		if err := r.repo.UpdateState(ctx, m.ID, "expired"); err != nil {
			return 0, err
		}
		if r.notifier != nil {
			go r.notifier.Notify(context.Background(), Event{
				TenantID: m.TenantID, MessageID: m.ID, SmscMsgid: m.SmscMsgid,
				Msisdn: m.Msisdn, State: "expired", Timestamp: time.Now(),
			})
		}
	}
	return len(stale), nil
}

func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			n, err := r.ReconcileOnce(ctx)
			if err != nil {
				slog.Error("reconciliacion dlr", "err", err)
				continue
			}
			if n > 0 {
				slog.Info("expired por falta de dlr", "n", n)
			}
		}
	}
}
