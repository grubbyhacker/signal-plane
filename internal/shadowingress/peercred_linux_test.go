//go:build linux

package shadowingress

import (
	"context"
	"net"
	"os"
	"testing"
	"time"
)

func TestAuthorizeUID(t *testing.T) {
	self := uint32(os.Getuid())
	// Empty allow-list accepts only own uid.
	if err := authorizeUID(self, nil); err != nil {
		t.Fatalf("own uid rejected: %v", err)
	}
	if err := authorizeUID(self+1, nil); err == nil {
		t.Fatal("foreign uid accepted with empty allow-list")
	}
	// Explicit allow-list membership.
	if err := authorizeUID(4242, []uint32{1, 4242, 9}); err != nil {
		t.Fatalf("allow-listed uid rejected: %v", err)
	}
	if err := authorizeUID(4242, []uint32{1, 9}); err == nil {
		t.Fatal("non-member uid accepted against allow-list")
	}
}

// TestLinuxPeerCredAuthorizesOwnProcess exercises the real SO_PEERCRED path:
// the test process connects to its own socket, so the kernel-reported peer uid
// is this process's uid and the default (own-uid) policy must accept it.
func TestLinuxPeerCredAuthorizesOwnProcess(t *testing.T) {
	shadow, resolver, _ := newShadow(t)
	socket := shortSocketPath(t)
	server, err := NewServer(Config{Enabled: true, SocketPath: socket}, shadow, resolver, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Use the real Linux authorizer (default), no override.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Serve(ctx)
	waitForSocket(t, socket)

	env := goodEnvelope("d-peer", 1, "rev-peer")
	result := request(t, socket, env)
	if !result.Matched || result.WorkItemID == "" || result.Launched {
		t.Fatalf("own-process peer admission = %#v", result)
	}

	// A restrictive allow-list that excludes our uid must reject the peer.
	rejectSocket := shortSocketPath(t)
	rejectServer, err := NewServer(Config{Enabled: true, SocketPath: rejectSocket, AllowedUIDs: []uint32{uint32(os.Getuid()) + 1}}, shadow, resolver, nil)
	if err != nil {
		t.Fatal(err)
	}
	rctx, rcancel := context.WithCancel(context.Background())
	defer rcancel()
	go rejectServer.Serve(rctx)
	waitForSocket(t, rejectSocket)

	conn, err := net.Dial("unix", rejectSocket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write(mustJSON(t, env))
	if uc, ok := conn.(*net.UnixConn); ok {
		_ = uc.CloseWrite()
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 256)
	n, _ := conn.Read(buf)
	// The server rejects before replying, so the peer sees EOF (0 bytes).
	if n != 0 {
		t.Fatalf("rejected peer unexpectedly got a reply: %q", string(buf[:n]))
	}
}
