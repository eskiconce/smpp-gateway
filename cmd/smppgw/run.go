package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/eskiconce/smpp-gateway/internal/api"
	"github.com/eskiconce/smpp-gateway/internal/billing"
	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/dlr"
	"github.com/eskiconce/smpp-gateway/internal/logger"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/session"
	"github.com/eskiconce/smpp-gateway/internal/store"
	"github.com/eskiconce/smpp-gateway/internal/worker"
)

type billingAdapter struct{ s *billing.Service }

func (a billingAdapter) Debit(ctx context.Context, tenantID, messageID string, amount float64) error {
	_, err := a.s.Debit(ctx, tenantID, messageID, amount)
	return err
}

func signalCtx() context.Context {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	_ = stop
	return ctx
}

func runServer(parent context.Context, cfg config.Config) {
	log := logger.New("server")
	ctx, cancel := context.WithCancel(signalCtx())
	defer cancel()

	pg, err := store.NewPG(ctx, cfg.DBURL)
	if err != nil {
		log.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pg.Close()

	rq, err := queue.NewRedis(cfg.RedisURL)
	if err != nil {
		log.Error("redis", "err", err)
		os.Exit(1)
	}
	defer rq.Close()

	rt := router.New(pg, router.Config{})
	if err := rt.Load(ctx); err != nil {
		log.Error("router", "err", err)
		os.Exit(1)
	}

	bill := billing.New(pg, pg, pg)
	pl := pipeline.NewPipeline(pg, rq, rt, pipeline.WithBiller(bill))
	srv := api.New(cfg, pl, pg)
	if cfg.ReconcileInterval > 0 {
		rec := dlr.NewReconciler(pg, dlr.NewWebhookNotifier(pg, cfg.WebhookTimeout),
			cfg.ReconcileTimeout, cfg.ReconcileInterval)
		go func() {
			_ = rec.Run(ctx)
		}()
	}
	log.Info("server iniciado")
	if err := srv.Run(ctx); err != nil {
		log.Error("http", "err", err)
	}
}

func runConnector(parent context.Context, cfg config.Config) {
	log := logger.New("connector")
	ctx, cancel := context.WithCancel(signalCtx())
	defer cancel()

	pg, err := store.NewPG(ctx, cfg.DBURL)
	if err != nil {
		log.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pg.Close()

	rq, err := queue.NewRedis(cfg.RedisURL)
	if err != nil {
		log.Error("redis", "err", err)
		os.Exit(1)
	}
	defer rq.Close()

	connectorID := cfg.ConnectorID
	cache, err := dlr.NewRedisCache(cfg.RedisURL, cfg.DLRTTL)
	if err != nil {
		log.Error("dlr cache", "err", err)
		os.Exit(1)
	}
	defer cache.Close()
	dlrProc := dlr.NewProcessor(cache, pg, dlr.NewWebhookNotifier(pg, cfg.WebhookTimeout))
	bill := billing.New(pg, pg, pg)

	w := worker.NewWorker(rq, pg, worker.WithDLR(dlrProc), worker.WithBiller(billingAdapter{bill}))
	sess := session.New(session.Config{
		Host: "127.0.0.1", Port: 2775, SystemID: "esp", Password: "secreto",
		SourceAddr: "shield", MsgPerSecond: 100, MaxConcurrency: 10,
	}, w)
	w.SetSession(sess)
	if err := sess.Dial(ctx); err != nil {
		log.Error("dial", "err", err)
		os.Exit(1)
	}
	defer sess.Close()

	for prio := 0; prio <= 2; prio++ {
		key := queue.Key(connectorID, prio)
		go func(p int) {
			_ = rq.Consume(ctx, key, fmt.Sprintf("cn-%d-%d", connectorID, p), func(it queue.Item) error {
				return w.Handle(ctx, it)
			})
		}(prio)
	}

	log.Info("connector iniciado", "id", connectorID)
	<-ctx.Done()
}
