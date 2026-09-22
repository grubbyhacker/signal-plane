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

// Server is the shadow-admission host ingress. It admits through the
// shadowadmit service ONLY; it holds no launcher, broker, or dispatcher handle.
type Server struct {
	cfg    Config
	admit  admitService
	logger *slog.Logger
	now    func() time.Time
	authz  peerAuthorizer
}

// peerAuthorizer authenticates the connecting local process. Its Linux
// implementation reads SO_PEERCRED; other platforms refuse (see the
// build-tagged files).
type peerAuthorizer interface {
	authorize(conn net.Conn, allowed []uint32) error
}

// NewServer validates config and constructs a Server. The admit service must be
// a *shadowadmit.Shadow (or a compatible admission-only service).
func NewServer(cfg Config, admit *shadowadmit.Shadow, logger *slog.Logger) (*Server, error) {
	if admit == nil {
		return nil, errors.New("shadow ingress requires a shadowadmit service")
	}
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.Enabled {
		if err := validateSocketPath(cfg.SocketPath); err != nil {
			return nil, err
		}
	}
	return &Server{cfg: cfg, admit: admit, logger: logger, now: time.Now, authz: defaultPeerAuthorizer()}, nil
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
	// Remove a stale socket from a prior crash, then listen with owner-only mode.
	if err := os.Remove(server.cfg.SocketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("shadow ingress clear stale socket: %w", err)
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
	// Admit with a ZERO agent binding: an emitter states a domain fact and names
	// no agent, image, release, or generation. The route-to-(agent_type,mode)
	// mapping is a deployment-owned dispatcher decision made elsewhere.
	candidate := shadowadmit.Candidate{
		RouteSnapshotID: envelope.RouteSnapshotID,
		Event:           envelope.Event(server.now()),
		Binding:         workledger.AgentBinding{},
	}
	outcome, err := server.admit.Admit(ctx, candidate)
	if err != nil {
		server.writeError(conn, err)
		return
	}
	server.writeResult(conn, Result{
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
