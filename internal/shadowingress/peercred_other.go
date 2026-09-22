//go:build !linux

package shadowingress

import (
	"fmt"
	"net"
)

// defaultPeerAuthorizer on non-Linux platforms refuses every peer. SO_PEERCRED
// is a Linux mechanism; signal-plane runs on Linux in production, and there is
// no portable, unforgeable local peer-credential check to substitute. The
// package still builds and tests here (macOS dev), but the ingress fails closed
// rather than accepting an unauthenticated local caller.
func defaultPeerAuthorizer() peerAuthorizer { return unsupportedPeerAuthorizer{} }

type unsupportedPeerAuthorizer struct{}

func (unsupportedPeerAuthorizer) authorize(conn net.Conn, allowed []uint32) error {
	return fmt.Errorf("%w: SO_PEERCRED peer authentication is only supported on linux", ErrUnauthorizedPeer)
}
