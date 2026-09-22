package config

import (
	"strings"
	"testing"
	"time"
)

func validLauncherConfig() Config {
	cfg := Config{
		Gateway: GatewayConfig{Addr: ":8080"},
		NATS:    NATSConfig{URL: DefaultNATSURL, Stream: DefaultStreamName, Subjects: []string{DefaultSubject}},
		Dispatcher: DispatcherConfig{
			Enabled: true, Subject: "signals.github.>", Durable: "dispatcher", DatabasePath: "jobs.db",
			BrokerURL: "http://sandbox-broker:8091", BrokerTokenEnv: "BROKER_TOKEN", Workers: 1,
			RepairDeadline: 2 * time.Hour, RepairMaxAttempts: 2,
			RepositoryTaskRoutes: []RepositoryTaskRoute{{
				ID: "fixture", Repository: "grubbyhacker/repository-agent-fixture", Event: "issues",
				Action: "labeled", Label: "automation:requested", Profile: "terra-medium-v1",
			}},
		},
		WorkRouter: WorkRouterConfig{
			Enabled: true, Subject: "signals.github.webhook", Durable: "resume-release-router", DatabasePath: "jobs.db",
			YKMURL: "https://mcp.fleiglabs.cc/mcp", YKMAuthMode: "cloudflare_access",
			GitHubPrivateKeyPath: "/run/secrets/app.pem", YKMClientIDEnv: "CF_ID", YKMClientSecretEnv: "CF_SECRET",
		},
		Routes: []Route{{ID: "manual", Path: "/manual", Source: "manual", MaxBodyBytes: 1, PublishSubject: "signals.manual"}},
	}
	cfg.ShadowAdmission = ShadowAdmissionConfig{
		Enabled: true,
		RouteActivations: []RouteActivation{{
			RouteDefinitionPath: "/etc/signal-plane/routes/intake.json", ExecutorID: "youknowme.curator",
			ExecutorKind: "deterministic_tool", ExecutorVersion: "v1",
		}},
		Launcher: WorkItemLauncherConfig{
			Enabled: true, AgentType: "youknowme-curator", ExecutorID: "youknowme.curator",
			ExecutorKind: "deterministic_tool", ExecutorVersion: "v1",
			Profiles: map[string]string{"process_intake": "ykm-curator-workitem-intake"},
		},
	}
	return cfg
}

func TestValidateWorkItemLauncherContract(t *testing.T) {
	base := validLauncherConfig()
	if err := base.Validate(); err != nil {
		t.Fatalf("valid launcher rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"dispatcher disabled":  func(cfg *Config) { cfg.Dispatcher.Enabled = false },
		"work router disabled": func(cfg *Config) { cfg.WorkRouter.Enabled = false },
		"missing profile":      func(cfg *Config) { cfg.ShadowAdmission.Launcher.Profiles = nil },
		"unknown executor":     func(cfg *Config) { cfg.ShadowAdmission.Launcher.ExecutorID = "other" },
		"version mismatch":     func(cfg *Config) { cfg.ShadowAdmission.Launcher.ExecutorVersion = "v2" },
		"invalid mode": func(cfg *Config) {
			cfg.ShadowAdmission.Launcher.Profiles = map[string]string{"Process Intake": "profile"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := validLauncherConfig()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "launcher") {
				t.Fatalf("invalid launcher error = %v", err)
			}
		})
	}
}
