package config

import (
	"os"
	"testing"
)

func TestLoad(t *testing.T) {
	os.Setenv("SMG_ROLE", "server")
	os.Setenv("SMG_HTTP_ADDR", ":9090")
	defer os.Unsetenv("SMG_ROLE")
	defer os.Unsetenv("SMG_HTTP_ADDR")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Role != "server" || c.HTTPAddr != ":9090" {
		t.Fatalf("unexpected config: %+v", c)
	}
}
