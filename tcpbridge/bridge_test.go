package tcpbridge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"universal-bypass-tool/transport"
)

// Every sender is echoed to itself and all other clients, just like a shared
// document. The hook can lose, duplicate or reorder messages on that channel.
type testHub struct {
	mu    sync.Mutex
	peers []*testTransport
	hook  func(frame, []byte) [][]byte
}
type testTransport struct {
	*transport.BaseTransport
	hub *testHub
}

func (h *testHub) peer() *testTransport {
	p := &testTransport{BaseTransport: transport.NewBaseTransport(transport.DefaultConfig()), hub: h}
	p.Start()
	p.SetConnected(true)
	h.mu.Lock()
	h.peers = append(h.peers, p)
	h.mu.Unlock()
	return p
}
func (p *testTransport) Send(data []byte) error {
	if !p.IsConnected() {
		return errors.New("disconnected")
	}
	p.hub.mu.Lock()
	defer p.hub.mu.Unlock()
	messages := [][]byte{data}
	if p.hub.hook != nil {
		f, err := parseFrame(data)
		if err != nil {
			return err
		}
		messages = p.hub.hook(f, data)
	}
	for _, m := range messages {
		for _, peer := range p.hub.peers {
			if peer.IsConnected() {
				peer.CallReceive(m)
			}
		}
	}
	return nil
}
func newBridge(t *testing.T, p *testTransport, target string) *Bridge {
	t.Helper()
	b, err := New(p, Config{Target: target, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}
func listenClient(t *testing.T, b *Bridge) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() { _ = b.Serve(l) }()
	return l.Addr().String()
}
func targetServer(t *testing.T, handler func(net.Conn)) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); handler(c) }()
		}
	}()
	return l.Addr().String()
}
func dialClient(t *testing.T, addr string) *net.TCPConn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(5 * time.Second))
	return c.(*net.TCPConn)
}
func waitFor(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func count(b *Bridge) int { b.mu.Lock(); defer b.mu.Unlock(); return len(b.conns) }

func TestMultipleClientsAndConnectionsWithHalfClose(t *testing.T) {
	target := targetServer(t, func(c net.Conn) {
		data, err := io.ReadAll(c) // Reply only after the client half-closes its write side.
		if err == nil {
			_ = writeAll(c, append([]byte("reply:"), data...))
		}
	})
	hub := &testHub{}
	server := newBridge(t, hub.peer(), target)
	var clients []*Bridge
	var addresses []string
	for i := 0; i < 3; i++ {
		b := newBridge(t, hub.peer(), "")
		clients = append(clients, b)
		addresses = append(addresses, listenClient(t, b))
	}
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for device, addr := range addresses {
		for stream := 0; stream < 4; stream++ {
			wg.Add(1)
			go func(device, stream int, addr string) {
				defer wg.Done()
				c, err := net.DialTimeout("tcp", addr, time.Second)
				if err != nil {
					errs <- err
					return
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(10 * time.Second))
				data := bytes.Repeat([]byte(fmt.Sprintf("device=%d stream=%d\x00\xff", device, stream)), 8192)
				if err = writeAll(c, data); err != nil {
					errs <- err
					return
				}
				if err = c.(*net.TCPConn).CloseWrite(); err != nil {
					errs <- err
					return
				}
				got, err := io.ReadAll(c)
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(got, append([]byte("reply:"), data...)) {
					errs <- fmt.Errorf("crossed or truncated stream %d/%d: got %d bytes", device, stream, len(got))
				}
			}(device, stream, addr)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	waitFor(t, func() bool {
		if count(server) != 0 {
			return false
		}
		for _, b := range clients {
			if count(b) != 0 {
				return false
			}
		}
		return true
	})
}

func TestTLSIsForwardedWithoutTermination(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("TLS through document")) }))
	defer target.Close()
	hub := &testHub{}
	newBridge(t, hub.peer(), target.Listener.Addr().String())
	addr := listenClient(t, newBridge(t, hub.peer(), ""))
	// The original test server's trusted certificate is checked by the client.
	client := target.Client()
	tr := client.Transport.(*http.Transport).Clone()
	defer tr.CloseIdleConnections()
	tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
	client.Transport = tr
	client.Timeout = 5 * time.Second
	resp, err := client.Get(target.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil || string(got) != "TLS through document" {
		t.Fatalf("TLS response: %q, %v", got, err)
	}
}

func TestDuplicateAndReorderedData(t *testing.T) {
	target := targetServer(t, func(c net.Conn) { io.Copy(c, c) })
	held := make(map[connectionID][]byte)
	hub := &testHub{hook: func(f frame, b []byte) [][]byte {
		if !f.server && f.kind == dataFrame {
			if f.seq == 1 {
				held[f.id] = append([]byte(nil), b...)
				return nil
			}
			if f.seq == 2 {
				return [][]byte{b, b, held[f.id], held[f.id]}
			}
		}
		return [][]byte{b}
	}}
	newBridge(t, hub.peer(), target)
	c := dialClient(t, listenClient(t, newBridge(t, hub.peer(), "")))
	data := bytes.Repeat([]byte("ordered\x00"), 8192)
	go func() { _ = writeAll(c, data); c.CloseWrite() }()
	got, err := io.ReadAll(c)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("reordered stream: %d bytes, %v", len(got), err)
	}
}

func TestLostDataClosesInsteadOfCorruptingStream(t *testing.T) {
	target := targetServer(t, func(c net.Conn) { io.Copy(c, c) })
	hub := &testHub{hook: func(f frame, b []byte) [][]byte {
		if !f.server && f.kind == dataFrame && f.seq == 2 {
			return nil
		}
		return [][]byte{b}
	}}
	server := newBridge(t, hub.peer(), target)
	client := newBridge(t, hub.peer(), "")
	c := dialClient(t, listenClient(t, client))
	data := bytes.Repeat([]byte("abcdefgh"), chunkSize)
	_ = writeAll(c, data)
	got, err := io.ReadAll(c)
	if len(got) > chunkSize || !bytes.Equal(got, data[:len(got)]) {
		t.Fatal("bytes after a gap were delivered")
	}
	if err == nil {
		t.Fatal("truncated stream was reported as a successful EOF")
	}
	if err != nil {
		if e, ok := err.(net.Error); ok && e.Timeout() {
			t.Fatal("broken stream hung instead of closing")
		}
	}
	waitFor(t, func() bool { return count(server) == 0 && count(client) == 0 })
}

func TestMissingAckBoundsOutstandingData(t *testing.T) {
	for _, windowSize := range []int{16, DefaultWindowSize, 128} {
		t.Run(fmt.Sprint(windowSize), func(t *testing.T) {
			target := targetServer(t, func(c net.Conn) { io.Copy(io.Discard, c) })
			var dataFrames int
			hub := &testHub{hook: func(f frame, b []byte) [][]byte {
				if !f.server && f.kind == dataFrame {
					dataFrames++
				}
				if f.server && f.kind == ackFrame {
					return nil
				}
				return [][]byte{b}
			}}
			newWindowBridge := func(target string) *Bridge {
				b, err := New(hub.peer(), Config{Target: target, Timeout: time.Second, WindowSize: windowSize})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { b.Close() })
				return b
			}
			newWindowBridge(target)
			c := dialClient(t, listenClient(t, newWindowBridge("")))
			_ = writeAll(c, bytes.Repeat([]byte{42}, chunkSize*windowSize*4))
			_, err := io.ReadAll(c)
			if e, ok := err.(net.Error); ok && e.Timeout() {
				t.Fatal("missing ACK did not close stream")
			}
			hub.mu.Lock()
			n := dataFrames
			hub.mu.Unlock()
			if n != windowSize {
				t.Fatalf("sent %d unacknowledged frames, want %d", n, windowSize)
			}
		})
	}
}

func TestReconnectClosesOldStreamsAndAllowsNewOnes(t *testing.T) {
	target := targetServer(t, func(c net.Conn) { io.Copy(c, c) })
	hub := &testHub{}
	newBridge(t, hub.peer(), target)
	p := hub.peer()
	client := newBridge(t, p, "")
	addr := listenClient(t, client)
	c := dialClient(t, addr)
	_ = writeAll(c, []byte("before"))
	got := make([]byte, 6)
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	// A very fast reconnect must also invalidate existing streams.
	p.SetConnected(false)
	p.SetConnected(true)
	if _, err := c.Read(got); err == nil {
		t.Fatal("old stream survived reconnect")
	}
	waitFor(t, func() bool { return count(client) == 0 })
	next := dialClient(t, addr)
	_ = writeAll(next, []byte("after"))
	got = make([]byte, 5)
	if _, err := io.ReadFull(next, got); err != nil || string(got) != "after" {
		t.Fatalf("new stream failed: %q %v", got, err)
	}
}

func TestTargetFailureAndUnknownMessages(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := l.Addr().String()
	l.Close()
	hub := &testHub{}
	server := newBridge(t, hub.peer(), target)
	p := hub.peer()
	c := dialClient(t, listenClient(t, newBridge(t, p, "")))
	_ = writeAll(c, []byte("hello"))
	if _, err = c.Read(make([]byte, 1)); err == nil {
		t.Fatal("unreachable target accepted data")
	}
	waitFor(t, func() bool { return count(server) == 0 })
	var id connectionID
	id[0] = 1
	for _, kind := range []byte{dataFrame, finFrame, ackFrame, pingFrame, resetFrame} {
		f := frame{id: id, kind: kind}
		if kind == dataFrame {
			f.seq = 1
			f.data = []byte("not an OPEN")
		}
		if kind == finFrame {
			f.seq = 1
		}
		p.Send(f.marshal())
	}
	if count(server) != 0 {
		t.Fatal("unknown ID opened a socket")
	}
}

func TestClosedOpenIsNotReplayed(t *testing.T) {
	target := targetServer(t, func(c net.Conn) { io.Copy(c, c) })
	hub := &testHub{}
	server := newBridge(t, hub.peer(), target)
	p := hub.peer()
	var id connectionID
	id[0] = 9
	p.Send((frame{id: id, kind: openFrame}).marshal())
	waitFor(t, func() bool { return count(server) == 1 })
	p.Send((frame{id: id, kind: resetFrame}).marshal())
	waitFor(t, func() bool { return count(server) == 0 })
	p.Send((frame{id: id, kind: openFrame}).marshal())
	if count(server) != 0 {
		t.Fatal("closed OPEN redialed target")
	}
}

func FuzzParseFrame(f *testing.F) {
	var id connectionID
	id[0] = 1
	f.Add((frame{id: id, kind: dataFrame, seq: 1, data: []byte("test")}).marshal())
	f.Add([]byte("legacy packet"))
	f.Fuzz(func(t *testing.T, data []byte) {
		parsed, err := parseFrame(data)
		if err == nil && !bytes.Equal(parsed.marshal(), data) {
			t.Fatal("frame did not round trip")
		}
	})
}
