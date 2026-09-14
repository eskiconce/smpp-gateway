package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/eskiconce/smpp-gateway/internal/api"
	"github.com/eskiconce/smpp-gateway/internal/config"
	"github.com/eskiconce/smpp-gateway/internal/logger"
	"github.com/eskiconce/smpp-gateway/internal/pipeline"
	"github.com/eskiconce/smpp-gateway/internal/queue"
	"github.com/eskiconce/smpp-gateway/internal/router"
	"github.com/eskiconce/smpp-gateway/internal/store"
)

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

	rt := router.New(stubRuleStore{}, router.Config{})
	if err := rt.Load(ctx); err != nil {
		log.Error("router", "err", err)
		os.Exit(1)
	}

	pl := pipeline.NewPipeline(pg, rq, rt)
	srv := api.New(cfg, pl, pg)
	log.Info("server iniciado")
	if err := srv.Run(ctx); err != nil {
		log.Error("http", "err", err)
	}
}

func runConnector(parent context.Context, cfg config.Config) {
	log := logger.New("connector")
	_ = log
	// M1: worker wiring completo en Task 12
}

// stubRuleStore adapts store.PGRepo to router.Store.
// TODO(Task 12): replace with real pgRuleStore that queries routing_rules table.
type stubRuleStore struct{}

func (stubRuleStore) ListRoutingRules(_ context.Context) ([]router.Rule, error) {
	return nil, nil
}
func (stubRuleStore) ListGroups(_ context.Context) ([]router.Group, error) {
	return nil, nil
}
