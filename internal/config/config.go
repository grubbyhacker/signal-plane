package config

import (
	"errors"
	"fmt"
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
)

type Config struct {
	Gateway GatewayConfig `yaml:"gateway"`
	NATS    NATSConfig    `yaml:"nats"`
	Routes  []Route       `yaml:"routes"`
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
	Repositories []string `yaml:"repositories"`
	Events       []string `yaml:"events"`
	Actions      []string `yaml:"actions"`
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
