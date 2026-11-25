package test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"socket_server/internal/config"
)

func TestAppConfigDefaultsStorageToRedis(t *testing.T) {
	cfg := config.AppConfig{}

	cfg.ApplyDefaults()

	if cfg.Storage != "redis" {
		t.Fatalf("expected default storage redis, got %q", cfg.Storage)
	}
}

func TestAppConfigRejectsUnsupportedStorage(t *testing.T) {
	cfg := config.AppConfig{Storage: "unknown"}

	cfg.ApplyDefaults()
	err := cfg.Validate()

	if err == nil || !strings.Contains(err.Error(), "unsupported storage mode") {
		t.Fatalf("expected unsupported storage mode error, got %v", err)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.json")
	if err := os.WriteFile(path, []byte(`{"unknown_field":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(path); err == nil || !strings.Contains(err.Error(), "unknown_field") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestSelectEndpoint(t *testing.T) {
	cfg := config.AppConfig{Gateways: []config.GatewayConfig{{EndpointID: "gateway-1"}, {EndpointID: "gateway-2"}}}

	t.Setenv("ENDPOINT_ID", "")
	first, err := config.SelectEndpoint("gateway", cfg.Gateways, cfg.FindGateway)
	if err != nil || first != &cfg.Gateways[0] {
		t.Fatalf("default endpoint = %v, error = %v", first, err)
	}

	t.Setenv("ENDPOINT_ID", "gateway-2")
	selected, err := config.SelectEndpoint("gateway", cfg.Gateways, cfg.FindGateway)
	if err != nil || selected != &cfg.Gateways[1] {
		t.Fatalf("configured endpoint = %v, error = %v", selected, err)
	}

	t.Setenv("ENDPOINT_ID", "unknown")
	if _, err := config.SelectEndpoint("gateway", cfg.Gateways, cfg.FindGateway); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unknown endpoint error = %v, want %v", err, os.ErrNotExist)
	}
}
