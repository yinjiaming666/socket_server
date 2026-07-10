package config

import (
	"strings"
	"testing"
)

func TestAppConfigDefaultsStorageToMemory(t *testing.T) {
	cfg := AppConfig{}

	cfg.ApplyDefaults()

	if cfg.Storage != "memory" {
		t.Fatalf("expected default storage memory, got %q", cfg.Storage)
	}
}

func TestAppConfigRejectsUnsupportedStorage(t *testing.T) {
	cfg := AppConfig{Storage: "unknown"}

	cfg.ApplyDefaults()
	err := cfg.Validate()

	if err == nil || !strings.Contains(err.Error(), "unsupported storage mode") {
		t.Fatalf("expected unsupported storage mode error, got %v", err)
	}
}
