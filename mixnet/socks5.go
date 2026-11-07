// Package mixnet opens TCP streams through the Nym mixnet.
//
// It speaks SOCKS5 (RFC 1928, no auth) to a local nym-socks5-client, which
// turns every stream into Sphinx packets that cross the mixnet to an exit
// gateway's network requester. Nothing in this package ever opens a direct
// connection to a destination: if the SOCKS5 proxy is not listening, dialing
// fails with ErrProxyUnreachable and there is no clearnet fallback.
package mixnet

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"
)

// ErrProxyUnreachable means no SOCKS5 proxy is listening. Callers must not
// fall back to a direct connection.
var ErrProxyUnreachable = errors.New("no SOCKS5 proxy (nym-socks5-client) listening; refusing to fall back to a direct connection")

// SocksError is a refusal or protocol error reported by the SOCKS5 proxy.
type SocksError struct {
	Code byte
	Msg  string
}

func (e *SocksError) Error() string { return e.Msg }

var replyErrors = map[byte]string{
	1: "general SOCKS server failure",
	2: "connection not allowed by ruleset (exit policy?)",
	3: "network unreachable",
	4: "host unreachable",
	5: "connection refused",
	6: "TTL expired",
	7: "command not supported",
	8: "address type not supported",
}

// EncodeAddr encodes host as SOCKS5 ATYP + address bytes. Hostnames are sent
// as names (ATYP 3) so they are never resolved locally.
func EncodeAddr(host string) ([]byte, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Is4() {
			b := ip.As4()
			return append([]byte{1}, b[:]...), nil
		}
		b := ip.As16()
		return append([]byte{4}, b[:]...), nil
	}
	if len(host) == 0 || len(host) > 255 || strings.ContainsAny(host, " /\x00") {
		return nil, fmt.Errorf("bad hostname %q", host)
	}
	return append([]byte{3, byte(len(host))}, host...), nil
}

// socksConnect performs the SOCKS5 handshake and CONNECT on an already
// connected proxy socket.
func socksConnect(conn net.Conn, host string, port int) error {
	addr, err := EncodeAddr(host)
	if err != nil {
		return err
	}
	if _, err := conn.Write([]byte{5, 1, 0}); err != nil {
		return err
	}
	var hello [2]byte
	if _, err := io.ReadFull(conn, hello[:]); err != nil {
		return &SocksError{Msg: "SOCKS5 proxy closed the connection during handshake: " + err.Error()}
	}
	if hello[0] != 5 || hello[1] != 0 {
		return &SocksError{Msg: fmt.Sprintf("proxy does not accept no-auth SOCKS5 (ver=%d method=%d)", hello[0], hello[1])}
	}
	req := append([]byte{5, 1, 0}, addr...)
	req = binary.BigEndian.AppendUint16(req, uint16(port))
	if _, err := conn.Write(req); err != nil {
		return err
	}
	var head [4]byte
	if _, err := io.ReadFull(conn, head[:]); err != nil {
		return &SocksError{Msg: "SOCKS5 proxy closed the connection before replying: " + err.Error()}
	}
	if head[0] != 5 {
		return &SocksError{Msg: fmt.Sprintf("bad SOCKS version in reply: %d", head[0])}
	}
	if head[1] != 0 {
		msg, ok := replyErrors[head[1]]
		if !ok {
			msg = fmt.Sprintf("reply code %d", head[1])
		}
		return &SocksError{Code: head[1], Msg: fmt.Sprintf("SOCKS5 CONNECT to %s:%d failed: %s", host, port, msg)}
	}
	var skip int
	switch head[3] {
	case 1:
		skip = 4
	case 4:
		skip = 16
	case 3:
		var l [1]byte
		if _, err := io.ReadFull(conn, l[:]); err != nil {
			return err
		}
		skip = int(l[0])
	default:
		return &SocksError{Msg: fmt.Sprintf("bad ATYP in reply: %d", head[3])}
	}
	_, err = io.ReadFull(conn, make([]byte, skip+2))
	return err
}

// ParseProxy accepts "host:port", "socks5://host:port", "socks5h://host:port",
// or a bare host (port 1080).
func ParseProxy(spec string) (string, error) {
	spec = strings.TrimSpace(spec)
	for _, p := range []string{"socks5h://", "socks5://"} {
		spec = strings.TrimPrefix(spec, p)
	}
	if spec == "" {
		return "", errors.New("empty proxy address")
	}
	if _, _, err := net.SplitHostPort(spec); err != nil {
		spec = net.JoinHostPort(spec, "1080")
	}
	return spec, nil
}

// dialSocks connects to the proxy and asks it for host:port.
func dialSocks(ctx context.Context, proxy, host string, port int, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: 10 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", proxy)
	if err != nil {
		if ctx.Err() != nil {
			// The caller gave up (client hung up, deadline): not a missing proxy.
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w (%s: %v)", ErrProxyUnreachable, proxy, err)
	}
	deadline := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = conn.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	err = socksConnect(conn, host, port)
	stop()
	if err != nil {
		conn.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func splitPort(addr string) (string, int, error) {
	host, ps, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(ps)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("bad port in %q", addr)
	}
	return host, port, nil
}
