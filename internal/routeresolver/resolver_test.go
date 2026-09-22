package routeresolver

import (
	"context"
	"strings"
	"testing"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func ykmConfig() Config {
	return Config{
		Version: 1,
		Routes: []RouteEntry{
			{Fact: "upload.completed", AgentType: "youknowme-curator", Mode: "process_intake", RouteSnapshotID: "route-intake", ContractRevision: "contract-v1"},
			{Fact: "youknowme.reconciliation_due", AgentType: "youknowme-curator", Mode: "reconcile", RouteSnapshotID: "route-reconcile", ContractRevision: "contract-v1"},
		},
	}
}

func TestParseConfigRejectsUnknownAndSelectionFields(t *testing.T) {
	good := `
version: 1
routes:
  - fact: upload.completed
    agent_type: youknowme-curator
    mode: process_intake
    route_snapshot_id: route-intake
`
	cfg, err := ParseConfig([]byte(good))
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if cfg.Version != 1 || len(cfg.Routes) != 1 || cfg.Routes[0].Fact != "upload.completed" {
		t.Fatalf("parsed config = %#v", cfg)
	}
	// An unknown key (a selection field an emitter must never supply) is refused
	// by strict decoding.
	for _, bad := range []string{
		"version: 1\nroutes:\n  - fact: upload.completed\n    agent_type: youknowme-curator\n    mode: process_intake\n    route_snapshot_id: r\n    image: ghcr.io/x@sha256:d\n",
		"version: 1\nroutes:\n  - fact: upload.completed\n    agent_type: youknowme-curator\n    mode: process_intake\n    route_snapshot_id: r\n    generation: 3\n",
		"version: 1\nroutes:\n  - fact: upload.completed\n    agent_type: youknowme-curator\n    mode: process_intake\n    route_snapshot_id: r\n    resolved_release_digest: sha256:x\n",
	} {
		if _, err := ParseConfig([]byte(bad)); err == nil {
			t.Fatalf("config with a selection field was accepted:\n%s", bad)
		}
	}
	// Version and non-empty routes are required.
	if _, err := ParseConfig([]byte("version: 0\nroutes: []\n")); err == nil {
		t.Fatal("zero version accepted")
	}
}

func TestRevisionIsDeterministicAndOrderIndependent(t *testing.T) {
	a := ykmConfig()
	b := ykmConfig()
	// Reverse route order in b; the revision must be identical.
	b.Routes[0], b.Routes[1] = b.Routes[1], b.Routes[0]
	ra, err := a.Revision()
	if err != nil {
		t.Fatal(err)
	}
	rb, err := b.Revision()
	if err != nil {
		t.Fatal(err)
	}
	if ra != rb {
		t.Fatalf("revision not order-independent: %q != %q", ra, rb)
	}
	if !strings.HasPrefix(ra, "routecfg:v1:") {
		t.Fatalf("revision lacks versioned prefix: %q", ra)
	}
	// A material change (different mode) changes the revision.
	c := ykmConfig()
	c.Routes[0].Mode = "reconcile"
	rc, err := c.Revision()
	if err != nil {
		t.Fatal(err)
	}
	if rc == ra {
		t.Fatal("revision did not change when a route's mode changed")
	}
}

func TestResolverMatchesFactsAndDropsUnknown(t *testing.T) {
	resolver, err := New(ykmConfig(), YouKnowMeCuratorCatalog())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(resolver.Revision(), "routecfg:v1:") {
		t.Fatalf("resolver revision = %q", resolver.Revision())
	}
	ctx := context.Background()

	// upload.completed -> (youknowme-curator, process_intake)
	res, err := resolver.Resolve(ctx, workledger.Event{EventKind: "upload", Action: "completed"})
	if err != nil || !res.Matched || res.RouteSnapshotID != "route-intake" || res.Binding.AgentType != "youknowme-curator" || res.Binding.Mode != "process_intake" {
		t.Fatalf("upload.completed = %#v err=%v", res, err)
	}
	// youknowme.reconciliation_due -> (youknowme-curator, reconcile)
	res, err = resolver.Resolve(ctx, workledger.Event{EventKind: "youknowme", Action: "reconciliation_due"})
	if err != nil || !res.Matched || res.RouteSnapshotID != "route-reconcile" || res.Binding.Mode != "reconcile" {
		t.Fatalf("reconciliation_due = %#v err=%v", res, err)
	}
	// The resolver output never carries an image/release/generation.
	if res.Binding.ResolvedReleaseGeneration != 0 || res.Binding.ResolvedReleaseDigest != "" || res.Binding.BrokerRunID != "" {
		t.Fatalf("resolver leaked a broker-resolved field: %#v", res.Binding)
	}

	// An unknown fact yields no match — no dispatch, no guessing.
	for _, unknown := range []workledger.Event{
		{EventKind: "upload", Action: "started"},
		{EventKind: "pull_request", Action: "synchronize"},
		{EventKind: "unknown"},
	} {
		res, err := resolver.Resolve(ctx, unknown)
		if err != nil || res.Matched {
			t.Fatalf("unknown fact %s.%s resolved: %#v err=%v", unknown.EventKind, unknown.Action, res, err)
		}
	}
}

func TestNewRejectsModeNotInCatalog(t *testing.T) {
	cfg := ykmConfig()
	cfg.Routes[0].Mode = "address_review_feedback" // not declared for curator here
	if _, err := New(cfg, YouKnowMeCuratorCatalog()); err == nil {
		t.Fatal("resolver accepted a mode the catalog does not declare")
	}
	// Unknown agent type is also refused.
	cfg = ykmConfig()
	cfg.Routes[0].AgentType = "code-reviewer"
	if _, err := New(cfg, YouKnowMeCuratorCatalog()); err == nil {
		t.Fatal("resolver accepted an agent type the catalog does not know")
	}
	// A nil catalog is refused.
	if _, err := New(ykmConfig(), nil); err == nil {
		t.Fatal("resolver accepted a nil catalog")
	}
}

func TestFactComposition(t *testing.T) {
	if got := Fact(workledger.Event{EventKind: "upload", Action: "completed"}); got != "upload.completed" {
		t.Fatalf("fact = %q", got)
	}
	if got := Fact(workledger.Event{EventKind: "youknowme"}); got != "youknowme" {
		t.Fatalf("actionless fact = %q", got)
	}
}

func TestBindSnapshotsUsesDispatcherActivationOutcomes(t *testing.T) {
	cfg := Config{
		Version: 1,
		Routes: []RouteEntry{{
			Fact: "upload.completed", AgentType: "youknowme-curator", Mode: "process_intake",
			RouteID: "ykm-upload-intake", ContractRevision: "contract-v1",
		}},
	}
	if _, err := New(cfg, YouKnowMeCuratorCatalog()); err == nil {
		t.Fatal("resolver accepted an unbound stable route id")
	}
	bound, err := cfg.BindSnapshots(map[string]string{"ykm-upload-intake": "route-0123456789abcdef0123456789abcdef"})
	if err != nil {
		t.Fatal(err)
	}
	if bound.Routes[0].RouteID != "" || bound.Routes[0].RouteSnapshotID != "route-0123456789abcdef0123456789abcdef" {
		t.Fatalf("bound route = %#v", bound.Routes[0])
	}
	resolver, err := New(bound, YouKnowMeCuratorCatalog())
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.Resolve(context.Background(), workledger.Event{EventKind: "upload", Action: "completed"})
	if err != nil || !resolved.Matched || resolved.RouteSnapshotID != bound.Routes[0].RouteSnapshotID {
		t.Fatalf("resolved = %#v err=%v", resolved, err)
	}
	if _, err := cfg.BindSnapshots(nil); err == nil {
		t.Fatal("missing dispatcher activation binding was accepted")
	}
}

func TestRouteEntryRequiresExactlyOneRouteSelector(t *testing.T) {
	base := RouteEntry{Fact: "upload.completed", AgentType: "youknowme-curator", Mode: "process_intake"}
	for name, route := range map[string]RouteEntry{
		"neither": base,
		"both": func() RouteEntry {
			route := base
			route.RouteID = "ykm-upload-intake"
			route.RouteSnapshotID = "route-existing"
			return route
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if err := (Config{Version: 1, Routes: []RouteEntry{route}}).Validate(); err == nil {
				t.Fatalf("%s route selector shape was accepted", name)
			}
		})
	}
}
