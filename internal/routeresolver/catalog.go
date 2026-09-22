package routeresolver

import (
	"bytes"
	"fmt"

	"gopkg.in/yaml.v3"
)

// StaticCatalog is a deployment-owned ModeCatalog: a fixed map of AgentType to
// the modes it declares. It stands in for a real AgentType contract registry
// and lets the resolver validate selected modes without importing any AgentType
// implementation. The YouKnowMe-curator type declares process_intake and
// reconcile (agent-platform-coupling Taxonomy → Modes).
type StaticCatalog struct {
	modes map[string]map[string]struct{}
}

// NewStaticCatalog builds a catalog from agentType → declared modes.
func NewStaticCatalog(declared map[string][]string) (*StaticCatalog, error) {
	catalog := &StaticCatalog{modes: make(map[string]map[string]struct{}, len(declared))}
	for agentType, modes := range declared {
		if !kebabPattern.MatchString(agentType) {
			return nil, fmt.Errorf("catalog agent_type %q must be a bounded kebab-case identifier", agentType)
		}
		set := make(map[string]struct{}, len(modes))
		for _, mode := range modes {
			if !snakePattern.MatchString(mode) {
				return nil, fmt.Errorf("catalog %q mode %q must be a bounded snake_case identifier", agentType, mode)
			}
			set[mode] = struct{}{}
		}
		catalog.modes[agentType] = set
	}
	return catalog, nil
}

// Declares reports whether agentType is known and enumerates mode.
func (catalog *StaticCatalog) Declares(agentType, mode string) bool {
	set, ok := catalog.modes[agentType]
	if !ok {
		return false
	}
	_, ok = set[mode]
	return ok
}

// YouKnowMeCuratorCatalog is the deployment-owned catalog for the first route
// set: the youknowme-curator type declares process_intake and reconcile.
func YouKnowMeCuratorCatalog() *StaticCatalog {
	catalog, err := NewStaticCatalog(map[string][]string{
		"youknowme-curator": {"process_intake", "reconcile"},
	})
	if err != nil {
		// The literal above is valid; a build/test would catch a regression.
		panic(err)
	}
	return catalog
}

// ParseConfig decodes a deployment-owned route config from YAML, rejecting
// unknown fields so a caller cannot smuggle a selection key, and validating the
// result. It reads bytes only — no file or network access.
func ParseConfig(raw []byte) (Config, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode route config: %w", err)
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}
