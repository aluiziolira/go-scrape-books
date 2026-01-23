package config

import (
	"strings"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name: "negative parallelism",
			mutate: func(cfg *Config) {
				cfg.Parallelism = -1
			},
			wantErr: "parallelism",
		},
		{
			name: "zero max pages",
			mutate: func(cfg *Config) {
				cfg.MaxPages = 0
			},
			wantErr: "max pages",
		},
		{
			name: "empty base url",
			mutate: func(cfg *Config) {
				cfg.BaseURL = ""
			},
			wantErr: "base URL",
		},
		{
			name: "invalid url format",
			mutate: func(cfg *Config) {
				cfg.BaseURL = "http://"
			},
			wantErr: "base URL",
		},
		{
			name: "negative timeout",
			mutate: func(cfg *Config) {
				cfg.Timeout = -1 * time.Second
			},
			wantErr: "timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestDefaultConfigValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should validate, got %v", err)
	}
}
