// Package shadowingresscmd is the executable wiring for the Stage-5 shadow
// stack: it loads the deployment-owned route config, opens the work ledger,
// constructs shadowadmit + routeresolver + shadowingress, and serves the Unix
// socket. It is the process glue only — it holds no launcher, broker, or
// dispatcher, opens no network listener, and never mutates legacy tables.
//
// Disabled by default: with shadow_admission.ingress.enabled=false the Run
// entrypoint is a clean no-op. When enabled it FAILS CLOSED on a missing route
// snapshot, a catalog mismatch, or bad socket permissions — it refuses to
// serve rather than admitting into an inconsistent state.
package shadowingresscmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/routeresolver"
	"github.com/grubbyhacker/signal-plane/internal/shadowadmit"
	"github.com/grubbyhacker/signal-plane/internal/shadowingress"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// Service is a constructed, ready-to-serve shadow ingress plus the resources it
// owns. Close releases the ledger.
type Service struct {
	server   *shadowingress.Server
	store    *workledger.Store
	revision string
}

// Serve runs the ingress until ctx is cancelled, then returns.
func (s *Service) Serve(ctx context.Context) error { return s.server.Serve(ctx) }

// Revision returns the deployment-owned route-config revision that is serving.
func (s *Service) Revision() string { return s.revision }

// Close releases the ledger the service opened.
func (s *Service) Close() error {
	if s.store != nil {
		return s.store.Close()
	}
	return nil
}

// Build constructs the shadow stack from config, failing closed on any
// inconsistency. It does NOT serve. The caller owns the returned Service and
// must Close it. catalog is the deployment-owned mode catalog to validate
// selected modes against.
//
// Build opens its OWN work-ledger handle from cfg.DatabasePath. That is correct
// only for tooling that owns the database exclusively; the production
// single-writer path is BuildWithStore, where the dispatcher passes the handle
// it already owns so no second process opens the operational database.
//
// Build returns (nil, nil) when the ingress is disabled: there is nothing to
// run, which is the default posture.
func Build(ctx context.Context, cfg config.ShadowAdmissionConfig, catalog routeresolver.ModeCatalog, logger *slog.Logger) (*Service, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if !cfg.Enabled || !cfg.Ingress.Enabled {
		logger.Info("shadow ingress disabled; nothing to serve",
			"admission_enabled", cfg.Enabled, "ingress_enabled", cfg.Ingress.Enabled)
		return nil, nil
	}
	if cfg.DatabasePath == "" {
		return nil, errors.New("shadow ingress requires shadow_admission.database_path when enabled")
	}
	store, err := workledger.Open(cfg.DatabasePath)
	if err != nil {
		return nil, fmt.Errorf("open work ledger: %w", err)
	}
	service, err := BuildWithStore(ctx, store, cfg, catalog, logger)
	if err != nil {
		store.Close()
		return nil, err
	}
	// The service now owns the handle Build opened, so Close releases it. Mark
	// it owned by handing the store to the service (BuildWithStore left store
	// nil because the caller there owns it).
	service.store = store
	return service, nil
}

// BuildWithStore constructs the shadow stack against a CALLER-OWNED work-ledger
// store, opening nothing. It is the single-writer ingress seam: the dispatcher,
// the sole owner of the operational database, passes its own attached handle so
// the ingress admits through the one connection instead of opening a second.
// The returned Service does NOT own the store (its Close is a no-op on the
// ledger); the dispatcher closes its handle exactly once.
//
// It fails closed identically to Build: missing route config, catalog
// mismatch, a route snapshot not active in the ledger, or bad socket
// permissions all refuse to serve. Returns (nil, nil) when disabled.
func BuildWithStore(ctx context.Context, store *workledger.Store, cfg config.ShadowAdmissionConfig, catalog routeresolver.ModeCatalog, logger *slog.Logger) (*Service, error) {
	return BuildWithStoreAndSnapshots(ctx, store, cfg, nil, catalog, logger)
}

// BuildWithStoreAndSnapshots is the production single-writer constructor. The
// snapshot map comes directly from dispatcher startup activation and binds any
// stable route_id entries in the deployment-owned route table before the
// resolver is constructed. No snapshot id is authored by deployment config.
func BuildWithStoreAndSnapshots(ctx context.Context, store *workledger.Store, cfg config.ShadowAdmissionConfig, snapshotByRouteID map[string]string, catalog routeresolver.ModeCatalog, logger *slog.Logger) (*Service, error) {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if !cfg.Enabled || !cfg.Ingress.Enabled {
		logger.Info("shadow ingress disabled; nothing to serve",
			"admission_enabled", cfg.Enabled, "ingress_enabled", cfg.Ingress.Enabled)
		return nil, nil
	}
	if store == nil {
		return nil, errors.New("shadow ingress requires a work-ledger store")
	}
	if cfg.Ingress.RouteConfigPath == "" {
		return nil, errors.New("shadow ingress requires shadow_admission.ingress.route_config_path when enabled")
	}

	// Parse the deployment-owned route config (fact -> agent_type/mode/snapshot).
	raw, err := os.ReadFile(cfg.Ingress.RouteConfigPath)
	if err != nil {
		return nil, fmt.Errorf("read route config: %w", err)
	}
	routeCfg, err := routeresolver.ParseConfig(raw)
	if err != nil {
		return nil, fmt.Errorf("parse route config: %w", err)
	}
	if snapshotByRouteID != nil {
		routeCfg, err = routeCfg.BindSnapshots(snapshotByRouteID)
		if err != nil {
			return nil, fmt.Errorf("bind dispatcher-activated route snapshots: %w", err)
		}
	}
	// New fails closed on a catalog mismatch (a mode the AgentType does not
	// declare) and on a config that smuggled a selection field.
	resolver, err := routeresolver.New(routeCfg, catalog)
	if err != nil {
		return nil, fmt.Errorf("build route resolver: %w", err)
	}

	// Fail closed on a missing route snapshot: every snapshot the route config
	// references must be an active snapshot in the ledger, or admission would
	// route facts to a snapshot that cannot admit them.
	for _, id := range resolver.RouteSnapshotIDs() {
		exists, existErr := store.RouteSnapshotExists(ctx, id)
		if existErr != nil {
			return nil, fmt.Errorf("verify route snapshot %q: %w", id, existErr)
		}
		if !exists {
			return nil, fmt.Errorf("route config references route snapshot %q that is not active in the ledger", id)
		}
	}

	shadow, err := shadowadmit.New(store, true)
	if err != nil {
		return nil, fmt.Errorf("build shadow admitter: %w", err)
	}

	// NewServer fails closed on bad socket permissions (non-absolute path or a
	// group/world-writable parent directory) when enabled.
	server, err := shadowingress.NewServer(shadowingress.Config{
		Enabled:     true,
		SocketPath:  cfg.Ingress.SocketPath,
		AllowedUIDs: cfg.Ingress.AllowedUIDs,
	}, shadow, resolver, logger)
	if err != nil {
		return nil, fmt.Errorf("build shadow ingress: %w", err)
	}

	logger.Info("shadow ingress ready",
		"socket", cfg.Ingress.SocketPath,
		"route_revision", resolver.Revision(),
		"routes", len(resolver.RouteSnapshotIDs()))
	// store is nil here: the caller owns the handle. Build (above) sets it when
	// it opened the handle itself.
	return &Service{server: server, store: nil, revision: resolver.Revision()}, nil
}

// Run is the process entrypoint: it builds the stack from the loaded config and
// serves until ctx is cancelled (a signal handler cancels ctx). When the
// ingress is disabled it returns nil immediately.
func Run(ctx context.Context, cfg config.ShadowAdmissionConfig, catalog routeresolver.ModeCatalog, logger *slog.Logger) error {
	service, err := Build(ctx, cfg, catalog, logger)
	if err != nil {
		return err
	}
	if service == nil {
		return nil // disabled
	}
	defer service.Close()
	return service.Serve(ctx)
}
