package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	os.Setenv("SMG_ROLE", "server")
	os.Setenv("SMG_HTTP_ADDR", ":9090")
	os.Setenv("SMG_JWT_SECRET", "test-secret")
	defer os.Unsetenv("SMG_ROLE")
	defer os.Unsetenv("SMG_HTTP_ADDR")
	defer os.Unsetenv("SMG_JWT_SECRET")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Role != "server" || c.HTTPAddr != ":9090" {
		t.Fatalf("unexpected config: %+v", c)
	}
}

func TestLoadDurations(t *testing.T) {
	os.Setenv("SMG_ROLE", "server")
	os.Setenv("SMG_JWT_SECRET", "test-secret")
	defer os.Unsetenv("SMG_ROLE")
	defer os.Unsetenv("SMG_JWT_SECRET")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.DLRTTL != 168*time.Hour || c.ReconcileTimeout != 10*time.Minute ||
		c.ReconcileInterval != time.Minute || c.WebhookTimeout != 5*time.Second {
		t.Fatalf("durations=%+v", c)
	}
}

func TestServerRequieresJWTSecret(t *testing.T) {
	os.Setenv("SMG_ROLE", "server")
	os.Setenv("SMG_JWT_SECRET", "")
	defer os.Unsetenv("SMG_ROLE")
	defer os.Unsetenv("SMG_JWT_SECRET")
	if _, err := Load(); err == nil {
		t.Fatal("server sin SMG_JWT_SECRET deberia fallar")
	}
}
