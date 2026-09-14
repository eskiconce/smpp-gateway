package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Role              string
	ConnectorID       int
	HTTPAddr          string
	SMPPAddr          string
	DBURL             string
	RedisURL          string
	DLRTTL            time.Duration
	ReconcileTimeout  time.Duration
	ReconcileInterval time.Duration
	WebhookTimeout    time.Duration
}

func Load() (Config, error) {
	var err error
	c := Config{
		HTTPAddr: envOr("SMG_HTTP_ADDR", ":8080"),
		SMPPAddr: envOr("SMG_SMPP_ADDR", ":2775"),
		DBURL:    envOr("SMG_DB_URL", "postgres://smpp:smpp@localhost:5432/smpp?sslmode=disable"),
		RedisURL: envOr("SMG_REDIS_URL", "redis://localhost:6379/0"),
	}
	c.Role = os.Getenv("SMG_ROLE")
	if c.Role == "" {
		return Config{}, fmt.Errorf("SMG_ROLE es requerido (server|connector)")
	}
	if c.Role == "connector" {
		id, err := strconv.Atoi(os.Getenv("SMG_CONNECTOR_ID"))
		if err != nil {
			return Config{}, fmt.Errorf("SMG_CONNECTOR_ID requerido para rol connector: %w", err)
		}
		c.ConnectorID = id
	}
	c.DLRTTL, err = time.ParseDuration(envOr("SMG_DLR_TTL", "168h"))
	if err != nil {
		return Config{}, fmt.Errorf("SMG_DLR_TTL invalido: %w", err)
	}
	c.ReconcileTimeout, err = time.ParseDuration(envOr("SMG_RECONCILE_TIMEOUT", "10m"))
	if err != nil {
		return Config{}, fmt.Errorf("SMG_RECONCILE_TIMEOUT invalido: %w", err)
	}
	c.ReconcileInterval, err = time.ParseDuration(envOr("SMG_RECONCILE_INTERVAL", "1m"))
	if err != nil {
		return Config{}, fmt.Errorf("SMG_RECONCILE_INTERVAL invalido: %w", err)
	}
	c.WebhookTimeout, err = time.ParseDuration(envOr("SMG_WEBHOOK_TIMEOUT", "5s"))
	if err != nil {
		return Config{}, fmt.Errorf("SMG_WEBHOOK_TIMEOUT invalido: %w", err)
	}
	return c, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
