package main

import (
	"context"
	"fmt"
	"os"

	"github.com/eskiconce/smpp-gateway/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	ctx := context.Background()
	switch cfg.Role {
	case "server":
		runServer(ctx, cfg)
	case "connector":
		runConnector(ctx, cfg)
	default:
		fmt.Fprintln(os.Stderr, "rol desconocido:", cfg.Role)
		os.Exit(1)
	}
}
