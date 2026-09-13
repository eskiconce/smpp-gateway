package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Role        string
	ConnectorID int
	HTTPAddr    string
	SMPPAddr    string
	DBURL       string
	RedisURL    string
}

func Load() (Config, error) {
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
	return c, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
