// Package routeresolver is the first deployment-owned RouteResolver used by the
// Stage-5 shadow ingress.
//
// Per the agent-platform-coupling design ("Selection authority"): emitters name
// a DOMAIN FACT only (upload.completed, youknowme.reconciliation_due, …); a
// deployment-owned route maps that fact to (agent_type, mode); the broker later
// resolves the active generation. This package implements the middle layer for
// two YouKnowMe-curator facts, wired to shadow admission ONLY. It never selects
// an image, release, or generation, never launches, and never touches the
// legacy dispatcher.
//
// The route config is deployment-owned data: a versioned mapping from a fact
// string to (agent_type, mode) plus the ledger route snapshot that admits it.
// A stable revision is computed from that config so a change is attributable.
// Modes are validated against a narrow injected ModeCatalog (the AgentType
// contract shape) without importing any AgentType implementation.
package routeresolver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/grubbyhacker/signal-plane/internal/shadowingress"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// ModeCatalog is the narrow contract the resolver validates selected modes
// against. It is the AgentType contract SHAPE — "does this type declare this
// mode" — injected as an interface so the resolver depends on no AgentType
// implementation code. A deployment-owned catalog (see StaticCatalog) satisfies
// it today; a real AgentType registry can satisfy it later unchanged.
type ModeCatalog interface {
	// Declares reports whether agentType is a known type that enumerates mode.
	Declares(agentType, mode string) bool
}

// Fact is the source-neutral domain fact an emitter states. It is the
// event_kind plus an optional action, joined with '.', exactly as the design
// names facts (upload.completed, youknowme.reconciliation_due). The resolver
// matches on this and nothing else — never on agent, image, release, or
// generation, which emitters never name.
func Fact(event workledger.Event) string {
	if event.Action == "" {
		return event.EventKind
	}
	return event.EventKind + "." + event.Action
}

// RouteEntry maps one domain fact to a deployment-owned (agent_type, mode) and
// the ledger route snapshot that admits it. It expresses no image, release, or
// generation.
type RouteEntry struct {
	Fact            string `json:"fact" yaml:"fact"`
	AgentType       string `json:"agent_type" yaml:"agent_type"`
	Mode            string `json:"mode" yaml:"mode"`
	RouteSnapshotID string `json:"route_snapshot_id" yaml:"route_snapshot_id"`
	// ContractRevision pins the AgentType/route contract revision this route
	// resolved against. Optional; bounded when present.
	ContractRevision string `json:"contract_revision" yaml:"contract_revision"`
}

// Config is the deployment-owned, versioned route table. It is DATA the
// resolver reads, never code, and it maps facts only.
type Config struct {
	Version int          `json:"version" yaml:"version"`
	Routes  []RouteEntry `json:"routes" yaml:"routes"`
}

var (
	factPattern  = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)?$`)
	kebabPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,127}$`)
	snakePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,127}$`)
	// forbiddenFields are keys a route config must never carry: routing chooses
	// (agent_type, mode) only, never a selectable image/release/generation.
	forbiddenFields = []string{"image", "release", "generation", "digest", "broker_run", "broker_run_id", "resolved_release", "resolved_release_generation", "resolved_release_digest"}
)

// Validate bounds every field, requires a positive version, forbids duplicate
// facts, and validates each route's shape. It consults no live system.
func (config Config) Validate() error {
	if config.Version <= 0 {
		return errors.New("route config version must be positive")
	}
	if len(config.Routes) == 0 {
		return errors.New("route config must declare at least one route")
	}
	seen := make(map[string]struct{}, len(config.Routes))
	for i, route := range config.Routes {
		if !factPattern.MatchString(route.Fact) {
			return fmt.Errorf("route %d fact %q is not a bounded domain-fact identifier", i, route.Fact)
		}
		if _, dup := seen[route.Fact]; dup {
			return fmt.Errorf("route config declares duplicate fact %q", route.Fact)
		}
		seen[route.Fact] = struct{}{}
		if !kebabPattern.MatchString(route.AgentType) {
			return fmt.Errorf("route %q agent_type must be a bounded kebab-case identifier", route.Fact)
		}
		if !snakePattern.MatchString(route.Mode) {
			return fmt.Errorf("route %q mode must be a bounded snake_case identifier", route.Fact)
		}
		if route.RouteSnapshotID == "" || len(route.RouteSnapshotID) > 256 {
			return fmt.Errorf("route %q requires a bounded route_snapshot_id", route.Fact)
		}
		if len(route.ContractRevision) > 256 {
			return fmt.Errorf("route %q contract_revision is oversized", route.Fact)
		}
	}
	return nil
}

// Revision computes a stable, deployment-owned snapshot revision over the
// canonicalized config: version plus fact-sorted routes. The same config yields
// the same revision on every host and process; any change (a new fact, a
// different mode, a repointed snapshot) changes it, so a routing change is
// attributable.
func (config Config) Revision() (string, error) {
	if err := config.Validate(); err != nil {
		return "", err
	}
	routes := make([]RouteEntry, len(config.Routes))
	copy(routes, config.Routes)
	sort.Slice(routes, func(a, b int) bool { return routes[a].Fact < routes[b].Fact })
	canonical := Config{Version: config.Version, Routes: routes}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return "routecfg:v" + fmt.Sprint(config.Version) + ":" + hex.EncodeToString(sum[:]), nil
}

// Resolver is the deployment-owned RouteResolver. It matches a domain fact to a
// route and returns the route snapshot and admission-safe binding the route
// selects. Unknown facts return no match. It satisfies
// shadowingress.RouteResolver.
type Resolver struct {
	revision string
	byFact   map[string]RouteEntry
}

// New builds a Resolver from a deployment-owned config, validating every
// selected (agent_type, mode) against the injected catalog. It records the
// config's stable revision. The catalog is REQUIRED: a mode the AgentType
// contract does not declare is a misconfiguration, refused here.
func New(config Config, catalog ModeCatalog) (*Resolver, error) {
	if catalog == nil {
		return nil, errors.New("route resolver requires a mode catalog")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	// Guard against a config that smuggled a selection field past the struct
	// (e.g. via a hand-authored map). Serialize and scan.
	encoded, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	lowered := strings.ToLower(string(encoded))
	for _, field := range forbiddenFields {
		if strings.Contains(lowered, `"`+field+`"`) {
			return nil, fmt.Errorf("route config must not carry selection field %q: routing chooses agent_type and mode only", field)
		}
	}
	revision, err := config.Revision()
	if err != nil {
		return nil, err
	}
	byFact := make(map[string]RouteEntry, len(config.Routes))
	for _, route := range config.Routes {
		if !catalog.Declares(route.AgentType, route.Mode) {
			return nil, fmt.Errorf("route %q selects mode %q not declared by AgentType %q", route.Fact, route.Mode, route.AgentType)
		}
		byFact[route.Fact] = route
	}
	return &Resolver{revision: revision, byFact: byFact}, nil
}

// Revision returns the stable route-config snapshot revision.
func (resolver *Resolver) Revision() string { return resolver.revision }

// RouteSnapshotIDs returns the distinct ledger route snapshot IDs this config
// references, sorted. A caller uses it to verify every referenced snapshot is
// activated in the ledger before serving — fail closed on a missing snapshot.
func (resolver *Resolver) RouteSnapshotIDs() []string {
	seen := make(map[string]struct{}, len(resolver.byFact))
	for _, route := range resolver.byFact {
		seen[route.RouteSnapshotID] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Resolve maps a validated domain-fact event to a deployment-owned routing
// decision. An unknown fact returns Matched=false — no dispatch, no guessing.
// The returned binding carries only (agent_type, mode, contract revision); it
// never names an image, release, or generation.
func (resolver *Resolver) Resolve(_ context.Context, event workledger.Event) (shadowingress.Resolution, error) {
	route, ok := resolver.byFact[Fact(event)]
	if !ok {
		return shadowingress.Resolution{Matched: false}, nil
	}
	return shadowingress.Resolution{
		Matched:         true,
		RouteSnapshotID: route.RouteSnapshotID,
		Binding: workledger.AgentBinding{
			AgentType:            route.AgentType,
			Mode:                 route.Mode,
			TypeContractRevision: route.ContractRevision,
		},
	}, nil
}
