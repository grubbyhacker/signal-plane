//go:build linux

package shadowingress

import (
	"errors"
	"fmt"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

// defaultPeerAuthorizer returns the Linux SO_PEERCRED authorizer.
func defaultPeerAuthorizer() peerAuthorizer { return linuxPeerAuthorizer{} }

type linuxPeerAuthorizer struct{}

// authorize reads the connecting process's credentials from the kernel via
// SO_PEERCRED and checks its UID against the allow-list. SO_PEERCRED is set by
// the kernel at connect time and cannot be forged by the caller, so it is a
// true peer-credential check rather than a claimed identity.
func (linuxPeerAuthorizer) authorize(conn net.Conn, allowed []uint32) error {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return errors.New("shadow ingress peer is not a unix connection")
	}
	rawConn, err := unixConn.SyscallConn()
	if err != nil {
		return fmt.Errorf("shadow ingress peer syscall conn: %w", err)
	}
	var cred *unix.Ucred
	var credErr error
	if err := rawConn.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return fmt.Errorf("shadow ingress peer control: %w", err)
	}
	if credErr != nil {
		return fmt.Errorf("shadow ingress SO_PEERCRED: %w", credErr)
	}
	return authorizeUID(cred.Uid, allowed)
}

// authorizeUID accepts a peer whose UID is in the allow-list, or — when the
// list is empty — only the process's own UID. Split out so it is unit-testable
// without a live socket.
func authorizeUID(peerUID uint32, allowed []uint32) error {
	if len(allowed) == 0 {
		self := uint32(os.Getuid())
		if peerUID != self {
			return fmt.Errorf("%w: peer uid %d != own uid %d", ErrUnauthorizedPeer, peerUID, self)
		}
		return nil
	}
	for _, uid := range allowed {
		if peerUID == uid {
			return nil
		}
	}
	return fmt.Errorf("%w: peer uid %d not in allow-list", ErrUnauthorizedPeer, peerUID)
}
