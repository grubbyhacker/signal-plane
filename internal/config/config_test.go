package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAppliesDefaultsAndEnvOverrides(t *testing.T) {
	t.Setenv("SIGNAL_GATEWAY_ADDR", ":19090")
	t.Setenv("NATS_URL", "nats://example.test:4222")

	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(`
routes:
  - id: manual-local
    path: /manual
    source: manual
    publish_subject: signals.manual.local.test
`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Gateway.Addr != ":19090" {
		t.Fatalf("gateway addr = %q", cfg.Gateway.Addr)
	}
	if cfg.NATS.URL != "nats://example.test:4222" {
		t.Fatalf("nats url = %q", cfg.NATS.URL)
	}
	if cfg.NATS.Stream != DefaultStreamName {
		t.Fatalf("stream = %q", cfg.NATS.Stream)
	}
	if cfg.Routes[0].MaxBodyBytes != DefaultMaxBody {
		t.Fatalf("max body = %d", cfg.Routes[0].MaxBodyBytes)
	}
}

func TestValidateDispatcherRequiresFixedProfileEndpoint(t *testing.T) {
	base := Config{
		Gateway: GatewayConfig{Addr: ":8080"},
		NATS:    NATSConfig{URL: DefaultNATSURL, Stream: DefaultStreamName, Subjects: []string{DefaultSubject}},
		Dispatcher: DispatcherConfig{
			Enabled: true, Subject: "signals.github.>", Durable: "dispatcher", DatabasePath: "jobs.db",
			BrokerURL: "https://broker.internal" + BrokerProfilePath, BrokerTokenEnv: "BROKER_TOKEN", Workers: 1,
		},
		Routes: []Route{{ID: "manual", Path: "/manual", Source: "manual", MaxBodyBytes: 1, PublishSubject: "signals.manual"}},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid fixed endpoint rejected: %v", err)
	}
	multipleWorkers := base
	multipleWorkers.Dispatcher.Workers = 2
	if err := multipleWorkers.Validate(); err == nil || !strings.Contains(err.Error(), "exactly one worker") {
		t.Fatalf("multiple worker error=%v", err)
	}
	for _, invalid := range []string{
		"https://broker.internal/v1/jobs",
		"https://broker.internal" + BrokerProfilePath + "/",
		"https://broker.internal" + BrokerProfilePath + "?profile=other",
	} {
		cfg := base
		cfg.Dispatcher.BrokerURL = invalid
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), BrokerProfilePath) {
			t.Fatalf("broker_url %q error=%v", invalid, err)
		}
	}
}

func TestValidateRejectsDuplicateRoutePaths(t *testing.T) {
	cfg := Config{
		Gateway: GatewayConfig{Addr: ":8080"},
		NATS:    NATSConfig{URL: DefaultNATSURL, Stream: DefaultStreamName, Subjects: []string{DefaultSubject}},
		Routes: []Route{
			{ID: "one", Path: "/manual", Source: "manual", MaxBodyBytes: 1, PublishSubject: "signals.one"},
			{ID: "two", Path: "/manual", Source: "manual", MaxBodyBytes: 1, PublishSubject: "signals.two"},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected duplicate path error")
	}
}

func TestValidateAdmissionTuples(t *testing.T) {
	base := Config{
		Gateway: GatewayConfig{Addr: ":8080"},
		NATS:    NATSConfig{URL: DefaultNATSURL, Stream: DefaultStreamName, Subjects: []string{DefaultSubject}},
		Routes: []Route{{
			ID: "github", Path: "/github", Source: "github", MaxBodyBytes: 1024, PublishSubject: "signals.github",
			Admission: AdmissionSet{Tuples: []AdmissionTuple{{Repository: "owner/repo", Event: "issues", Actions: []string{"labeled"}}}},
		}},
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid tuples rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"mixed legacy", func(c *Config) { c.Routes[0].Admission.Events = []string{"issues"} }},
		{"empty repository", func(c *Config) { c.Routes[0].Admission.Tuples[0].Repository = "" }},
		{"empty event", func(c *Config) { c.Routes[0].Admission.Tuples[0].Event = "" }},
		{"no actions", func(c *Config) { c.Routes[0].Admission.Tuples[0].Actions = nil }},
		{"empty action", func(c *Config) { c.Routes[0].Admission.Tuples[0].Actions = []string{""} }},
		{"duplicate action", func(c *Config) { c.Routes[0].Admission.Tuples[0].Actions = []string{"labeled", "labeled"} }},
		{"duplicate tuple", func(c *Config) {
			c.Routes[0].Admission.Tuples = append(c.Routes[0].Admission.Tuples, c.Routes[0].Admission.Tuples[0])
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			cfg.Routes = append([]Route(nil), base.Routes...)
			cfg.Routes[0].Admission.Tuples = append([]AdmissionTuple(nil), base.Routes[0].Admission.Tuples...)
			tt.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected tuple validation error")
			}
		})
	}
}
