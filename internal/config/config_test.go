package config

import (
	"os"
	"path/filepath"
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

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.json")
	if err := os.WriteFile(path, []byte(`{"unknown_field":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "unknown_field") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}
