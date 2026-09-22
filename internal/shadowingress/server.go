package shadowingress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/shadowadmit"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// Config configures the host intake. Disabled by default: an empty SocketPath
// or Enabled=false makes Serve a no-op that returns immediately.
type Config struct {
	// Enabled turns the ingress on. Default false keeps it inert.
	Enabled bool
	// SocketPath is the absolute path of the Unix-domain socket to listen on.
	SocketPath string
	// AllowedUIDs, when non-empty, restricts the peer to these process UIDs
	// (SO_PEERCRED). Empty means "same UID as this process".
	AllowedUIDs []uint32
}

// Resolution is the deployment-owned routing decision for one domain fact. It
// is produced by a RouteResolver, never by the caller. When Matched is false
// the fact does not route to any agent and is deterministically dropped (no
// admission): an unknown fact yields no dispatch — silence is correct, guessing
// is the flaw.
type Resolution struct {
	Matched         bool
	RouteSnapshotID string
	Binding         workledger.AgentBinding
}

// RouteResolver maps a validated source-neutral domain-fact Event to a
// deployment-owned routing decision: which route snapshot admits it and the
// (agent_type, mode) binding that route selects. It is the seam through which a
// later deployment-owned route table is injected; this package depends on the
// interface but does not implement the table. The resolver must not select an
// image, release, or generation — only the admission-safe binding.
type RouteResolver interface {
	Resolve(ctx context.Context, event workledger.Event) (Resolution, error)
}

// Server is the shadow-admission host ingress. It admits through the
// shadowadmit service ONLY; it holds no launcher, broker, or dispatcher handle.
type Server struct {
	cfg      Config
	admit    admitService
	resolver RouteResolver
	logger   *slog.Logger
	now      func() time.Time
	authz    peerAuthorizer
}

// peerAuthorizer authenticates the connecting local process. Its Linux
// implementation reads SO_PEERCRED; other platforms refuse (see the
// build-tagged files).
type peerAuthorizer interface {
	authorize(conn net.Conn, allowed []uint32) error
}

// NewServer validates config and constructs a Server. The admit service must be
// a *shadowadmit.Shadow (or a compatible admission-only service). When the
// ingress is enabled a deployment-owned RouteResolver is REQUIRED: the caller
// never selects routing, so an enabled ingress with no resolver is refused.
func NewServer(cfg Config, admit *shadowadmit.Shadow, resolver RouteResolver, logger *slog.Logger) (*Server, error) {
	if admit == nil {
		return nil, errors.New("shadow ingress requires a shadowadmit service")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Enabled {
		if resolver == nil {
			return nil, errors.New("shadow ingress requires a deployment-owned route resolver when enabled")
		}
		if err := validateSocketPath(cfg.SocketPath); err != nil {
			return nil, err
		}
	}
	return &Server{cfg: cfg, admit: admit, resolver: resolver, logger: logger, now: time.Now, authz: defaultPeerAuthorizer()}, nil
}

// validateSocketPath requires an absolute path in an existing owner-only
// directory. A world-writable parent would let another user pre-create or
// swap the socket, defeating the filesystem-permission half of the guard.
func validateSocketPath(path string) error {
	if path == "" {
		return errors.New("shadow ingress socket path is required when enabled")
	}
	if !filepath.IsAbs(path) {
		return errors.New("shadow ingress socket path must be absolute")
	}
	dir := filepath.Dir(path)
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("shadow ingress socket directory: %w", err)
	}
	if !info.IsDir() {
		return errors.New("shadow ingress socket directory is not a directory")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return errors.New("shadow ingress socket directory is group/world-writable")
	}
	return nil
}

// clearStaleSocket removes a leftover socket from a prior run. It Lstats the
// path first and removes it ONLY when it is a Unix socket; a regular file,
// directory, symlink, or any other type is preserved and startup fails. This
// prevents a misconfigured SocketPath from silently deleting real data.
func clearStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("shadow ingress inspect socket path: %w", err)
	}
	if info.Mode().Type()&os.ModeSocket == 0 {
		return fmt.Errorf("shadow ingress socket path %q exists and is not a unix socket (mode %v); refusing to remove", path, info.Mode().Type())
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("shadow ingress clear stale socket: %w", err)
	}
	return nil
}

// Serve listens on the Unix socket until ctx is cancelled. When the ingress is
// disabled it returns nil immediately without opening anything.
func (server *Server) Serve(ctx context.Context) error {
	if !server.cfg.Enabled {
		server.logger.Info("shadow ingress disabled; not listening")
		return nil
	}
	if !server.admit.Enabled() {
		// Fail closed: an enabled listener in front of a disabled admitter would
		// accept envelopes it can only reject. Refuse to start instead.
		return errors.New("shadow ingress enabled but shadowadmit service is disabled")
	}
	// Remove a stale socket from a prior crash, but ONLY if the path is actually
	// a Unix socket. Lstat first (not Stat) so a symlink is inspected as a
	// symlink; refuse to delete a regular file, directory, or any non-socket so
	// a misconfigured path can never make the ingress destroy real data.
	if err := clearStaleSocket(server.cfg.SocketPath); err != nil {
		return err
	}
	listener, err := net.Listen("unix", server.cfg.SocketPath)
	if err != nil {
		return fmt.Errorf("shadow ingress listen: %w", err)
	}
	defer listener.Close()
	defer os.Remove(server.cfg.SocketPath)
	if err := os.Chmod(server.cfg.SocketPath, socketMode); err != nil {
		return fmt.Errorf("shadow ingress chmod socket: %w", err)
	}
	server.logger.Info("shadow ingress listening", "socket", server.cfg.SocketPath, "mode", fmt.Sprintf("%#o", socketMode))

	go func() {
		<-ctx.Done()
		listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("shadow ingress accept: %w", err)
		}
		go server.handle(ctx, conn)
	}
}

// handle authenticates the peer, reads one bounded envelope, admits it through
// the shadowadmit service, and writes back a deterministic result. It never
// launches, enqueues, or mutates legacy dispatcher tables.
func (server *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	if err := server.authz.authorize(conn, server.cfg.AllowedUIDs); err != nil {
		server.logger.Warn("shadow ingress rejected peer", "error", err)
		return
	}
	_ = conn.SetReadDeadline(server.now().Add(readTimeout))
	raw, err := io.ReadAll(io.LimitReader(conn, MaxEnvelopeBytes+1))
	if err != nil {
		server.logger.Warn("shadow ingress read failed", "error", err)
		return
	}
	envelope, err := DecodeEnvelope(raw)
	if err != nil {
		server.writeError(conn, err)
		return
	}
	// Routing is deployment-owned, not caller-supplied: the resolver maps the
	// validated domain fact to a route snapshot and the (agent_type, mode)
	// binding that route selects. The caller names neither.
	event := envelope.Event(server.now())
	resolution, err := server.resolver.Resolve(ctx, event)
	if err != nil {
		server.writeError(conn, err)
		return
	}
	if !resolution.Matched {
		// An unmatched fact routes to no agent and is deterministically dropped:
		// no admission, no launch. Silence is correct; guessing is the flaw.
		server.logger.Info("shadow ingress dropped unmatched domain fact", "delivery", envelope.SourceDeliveryID, "object_kind", envelope.ObjectKind)
		server.writeResult(conn, Result{Matched: false, Launched: false})
		return
	}
	// Admit with the RESOLVER's route snapshot and admission-safe binding — never
	// any caller-supplied value. shadowadmit re-rejects any image/release/
	// generation selection as a second guard.
	candidate := shadowadmit.Candidate{
		RouteSnapshotID: resolution.RouteSnapshotID,
		Event:           event,
		Binding:         resolution.Binding.AdmissionBinding(),
	}
	outcome, err := server.admit.Admit(ctx, candidate)
	if err != nil {
		server.writeError(conn, err)
		return
	}
	server.writeResult(conn, Result{
		Matched:    true,
		WorkItemID: outcome.WorkItemID,
		EventID:    outcome.EventID,
		Duplicate:  outcome.Duplicate,
		Launched:   outcome.Launched, // always false
	})
}

func (server *Server) writeResult(conn net.Conn, result Result) {
	_ = conn.SetWriteDeadline(server.now().Add(readTimeout))
	if err := json.NewEncoder(conn).Encode(result); err != nil {
		server.logger.Warn("shadow ingress write result failed", "error", err)
	}
}

func (server *Server) writeError(conn net.Conn, cause error) {
	_ = conn.SetWriteDeadline(server.now().Add(readTimeout))
	_ = json.NewEncoder(conn).Encode(map[string]string{"error": cause.Error()})
}

// setAuthorizer overrides the peer authorizer. Test-only: production always
// uses the build-tagged defaultPeerAuthorizer selected in NewServer.
func (server *Server) setAuthorizer(authz peerAuthorizer) { server.authz = authz }
