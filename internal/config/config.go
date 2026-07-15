package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultGatewayAddr = ":8080"
	DefaultNATSURL     = "nats://127.0.0.1:4222"
	DefaultStreamName  = "SIGNALS"
	DefaultSubject     = "signals.>"
	DefaultMaxBody     = int64(1 << 20)
	BrokerProfilePath  = "/v1/launch-profiles/codex-issue-implement/launch"
)

type Config struct {
	Gateway    GatewayConfig    `yaml:"gateway"`
	NATS       NATSConfig       `yaml:"nats"`
	Dispatcher DispatcherConfig `yaml:"dispatcher"`
	WorkRouter WorkRouterConfig `yaml:"work_router"`
	Routes     []Route          `yaml:"routes"`
}

type WorkRouterConfig struct {
	Enabled              bool   `yaml:"enabled"`
	Addr                 string `yaml:"addr"`
	Subject              string `yaml:"subject"`
	Durable              string `yaml:"durable"`
	DatabasePath         string `yaml:"database_path"`
	YKMURL               string `yaml:"ykm_url"`
	YKMAuthMode          string `yaml:"ykm_auth_mode"`
	GitHubPrivateKeyPath string `yaml:"github_private_key_path"`
	YKMClientIDEnv       string `yaml:"ykm_client_id_env"`
	YKMClientSecretEnv   string `yaml:"ykm_client_secret_env"`
	YKMLocalSecretEnv    string `yaml:"ykm_local_secret_env"`
}

type DispatcherConfig struct {
	Enabled               bool   `yaml:"enabled"`
	Addr                  string `yaml:"addr"`
	Subject               string `yaml:"subject"`
	Durable               string `yaml:"durable"`
	DatabasePath          string `yaml:"database_path"`
	BrokerURL             string `yaml:"broker_url"`
	BrokerTokenEnv        string `yaml:"broker_token_env"`
	Workers               int    `yaml:"workers"`
	RecoveryStartSequence uint64 `yaml:"recovery_start_sequence"`
}

type GatewayConfig struct {
	Addr string `yaml:"addr"`
}

type NATSConfig struct {
	URL      string   `yaml:"url"`
	Stream   string   `yaml:"stream"`
	Subjects []string `yaml:"subjects"`
}

type Route struct {
	ID                     string       `yaml:"id"`
	Path                   string       `yaml:"path"`
	Source                 string       `yaml:"source"`
	MaxBodyBytes           int64        `yaml:"max_body_bytes"`
	PublishSubject         string       `yaml:"publish_subject"`
	PublishSubjectTemplate string       `yaml:"publish_subject_template"`
	ManualAuthTokenEnv     string       `yaml:"manual_auth_token_env"`
	GitHub                 GitHubConfig `yaml:"github"`
	Admission              AdmissionSet `yaml:"admission"`
}

type GitHubConfig struct {
	WebhookSecretEnv string `yaml:"webhook_secret_env"`
	PublishPing      bool   `yaml:"publish_ping"`
}

type AdmissionSet struct {
	Repositories []string         `yaml:"repositories"`
	Events       []string         `yaml:"events"`
	Actions      []string         `yaml:"actions"`
	Tuples       []AdmissionTuple `yaml:"tuples"`
}

type AdmissionTuple struct {
	Repository string   `yaml:"repository"`
	Event      string   `yaml:"event"`
	Actions    []string `yaml:"actions"`
}

func Load(path string) (Config, error) {
	if path == "" {
		path = "configs/example.yaml"
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	applyEnv(&cfg)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func applyEnv(cfg *Config) {
	if cfg.Gateway.Addr == "" {
		cfg.Gateway.Addr = DefaultGatewayAddr
	}
	if value := os.Getenv("SIGNAL_GATEWAY_ADDR"); value != "" {
		cfg.Gateway.Addr = value
	}

	if cfg.NATS.URL == "" {
		cfg.NATS.URL = DefaultNATSURL
	}
	if value := os.Getenv("NATS_URL"); value != "" {
		cfg.NATS.URL = value
	}
	if cfg.NATS.Stream == "" {
		cfg.NATS.Stream = DefaultStreamName
	}
	if len(cfg.NATS.Subjects) == 0 {
		cfg.NATS.Subjects = []string{DefaultSubject}
	}
	if cfg.Dispatcher.Addr == "" {
		cfg.Dispatcher.Addr = ":8082"
	}
	if cfg.Dispatcher.Subject == "" {
		cfg.Dispatcher.Subject = "signals.github.>"
	}
	if cfg.Dispatcher.Durable == "" {
		cfg.Dispatcher.Durable = "github-task-dispatcher"
	}
	if cfg.Dispatcher.DatabasePath == "" {
		cfg.Dispatcher.DatabasePath = "github-task-dispatcher.db"
	}
	if cfg.Dispatcher.Workers == 0 {
		cfg.Dispatcher.Workers = 1
	}
	if cfg.WorkRouter.Addr == "" {
		cfg.WorkRouter.Addr = ":8083"
	}
	if cfg.WorkRouter.Subject == "" {
		cfg.WorkRouter.Subject = "signals.github.webhook"
	}
	if cfg.WorkRouter.Durable == "" {
		cfg.WorkRouter.Durable = "resume-release-router"
	}
	if cfg.WorkRouter.DatabasePath == "" {
		cfg.WorkRouter.DatabasePath = "github-task-dispatcher.db"
	}

	for i := range cfg.Routes {
		if cfg.Routes[i].MaxBodyBytes == 0 {
			cfg.Routes[i].MaxBodyBytes = DefaultMaxBody
		}
	}
}

func (cfg Config) Validate() error {
	if strings.TrimSpace(cfg.Gateway.Addr) == "" {
		return errors.New("gateway addr is required")
	}
	if strings.TrimSpace(cfg.NATS.URL) == "" {
		return errors.New("nats url is required")
	}
	if strings.TrimSpace(cfg.NATS.Stream) == "" {
		return errors.New("nats stream is required")
	}
	if len(cfg.NATS.Subjects) == 0 {
		return errors.New("nats subjects are required")
	}
	if len(cfg.Routes) == 0 {
		return errors.New("at least one route is required")
	}
	if cfg.Dispatcher.Enabled {
		if strings.TrimSpace(cfg.Dispatcher.Subject) == "" || strings.TrimSpace(cfg.Dispatcher.Durable) == "" || strings.TrimSpace(cfg.Dispatcher.DatabasePath) == "" || strings.TrimSpace(cfg.Dispatcher.BrokerURL) == "" || strings.TrimSpace(cfg.Dispatcher.BrokerTokenEnv) == "" {
			return errors.New("enabled dispatcher requires subject, durable, database_path, broker_url, and broker_token_env")
		}
		if cfg.Dispatcher.Workers != 1 {
			return errors.New("enabled dispatcher requires exactly one worker")
		}
		brokerURL, err := url.Parse(cfg.Dispatcher.BrokerURL)
		if err != nil || (brokerURL.Scheme != "http" && brokerURL.Scheme != "https") || brokerURL.Host == "" || brokerURL.User != nil || brokerURL.EscapedPath() != BrokerProfilePath || brokerURL.RawQuery != "" || brokerURL.Fragment != "" {
			return fmt.Errorf("enabled dispatcher broker_url must be the exact codex issue profile endpoint ending in %s", BrokerProfilePath)
		}
	}
	if cfg.WorkRouter.Enabled {
		wr := cfg.WorkRouter
		if wr.Subject == "" || wr.Durable == "" || wr.DatabasePath == "" || wr.YKMURL == "" || wr.GitHubPrivateKeyPath == "" {
			return errors.New("enabled work_router requires subject, durable, database_path, ykm_url, and github_private_key_path")
		}
		parsed, err := url.Parse(wr.YKMURL)
		if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/mcp" || parsed.Host == "" {
			return errors.New("work_router ykm_url must be a fixed MCP endpoint")
		}
		switch wr.YKMAuthMode {
		case "cloudflare_access":
			if wr.YKMURL != "https://mcp.fleiglabs.cc/mcp" || wr.YKMClientIDEnv == "" || wr.YKMClientSecretEnv == "" || wr.YKMLocalSecretEnv != "" {
				return errors.New("cloudflare_access work_router requires HTTPS client ID/secret env names only")
			}
		case "local_secret":
			if wr.YKMURL != "http://youknowme-mcp:8765/mcp" || wr.YKMLocalSecretEnv == "" || wr.YKMClientIDEnv != "" || wr.YKMClientSecretEnv != "" {
				return errors.New("local_secret work_router requires a private local URL and local secret env name only")
			}
		default:
			return errors.New("enabled work_router has unsupported ykm_auth_mode")
		}
	}

	seen := map[string]string{}
	for _, route := range cfg.Routes {
		if err := route.Validate(); err != nil {
			return err
		}
		if previous := seen[route.Path]; previous != "" {
			return fmt.Errorf("route path %q is used by both %q and %q", route.Path, previous, route.ID)
		}
		seen[route.Path] = route.ID
	}
	return nil
}

func (route Route) Validate() error {
	if strings.TrimSpace(route.ID) == "" {
		return errors.New("route id is required")
	}
	if !strings.HasPrefix(route.Path, "/") {
		return fmt.Errorf("route %q path must start with /", route.ID)
	}
	if strings.TrimSpace(route.Source) == "" {
		return fmt.Errorf("route %q source is required", route.ID)
	}
	if route.MaxBodyBytes <= 0 {
		return fmt.Errorf("route %q max_body_bytes must be positive", route.ID)
	}
	if strings.TrimSpace(route.Subject()) == "" {
		return fmt.Errorf("route %q publish_subject is required", route.ID)
	}
	if len(route.Admission.Tuples) > 0 {
		if len(route.Admission.Repositories) > 0 || len(route.Admission.Events) > 0 || len(route.Admission.Actions) > 0 {
			return fmt.Errorf("route %q admission cannot combine tuples with repositories/events/actions", route.ID)
		}
		seen := make(map[string]struct{}, len(route.Admission.Tuples))
		for i, tuple := range route.Admission.Tuples {
			if strings.TrimSpace(tuple.Repository) == "" || strings.TrimSpace(tuple.Event) == "" {
				return fmt.Errorf("route %q admission tuple %d requires repository and event", route.ID, i)
			}
			if len(tuple.Actions) == 0 {
				return fmt.Errorf("route %q admission tuple %d requires at least one action", route.ID, i)
			}
			key := tuple.Repository + "\x00" + tuple.Event
			if _, ok := seen[key]; ok {
				return fmt.Errorf("route %q admission has duplicate tuple for %q and %q", route.ID, tuple.Repository, tuple.Event)
			}
			seen[key] = struct{}{}
			actions := make(map[string]struct{}, len(tuple.Actions))
			for _, action := range tuple.Actions {
				if strings.TrimSpace(action) == "" {
					return fmt.Errorf("route %q admission tuple %d contains an empty action", route.ID, i)
				}
				if _, ok := actions[action]; ok {
					return fmt.Errorf("route %q admission tuple %d contains duplicate action %q", route.ID, i, action)
				}
				actions[action] = struct{}{}
			}
		}
	}
	return nil
}

func (route Route) Subject() string {
	if route.PublishSubject != "" {
		return route.PublishSubject
	}
	return route.PublishSubjectTemplate
}

func ContainsAllowed(values []string, value string) bool {
	if len(values) == 0 {
		return true
	}
	for _, allowed := range values {
		if allowed == value {
			return true
		}
	}
	return false
}
