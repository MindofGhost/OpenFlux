package tcpbridge

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"universal-bypass-tool/transport"
)

func newPoolForTest(t *testing.T, peers []transport.Transport, config Config) *Bridge {
	t.Helper()
	config.Timeout = 2 * time.Second
	b, err := NewPool(peers, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func echoPool(t *testing.T, c net.Conn) {
	t.Helper()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	payload := bytes.Repeat([]byte("pool\x00\xff"), 400)
	if err := writeAll(c, payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("pool payload mismatch")
	}
}

func TestPoolRoutingAndDocumentFailureIsolation(t *testing.T) {
	target := targetServer(t, func(c net.Conn) { io.Copy(c, c) })
	hubs := []*testHub{{}, {}}
	server := newPoolForTest(t, []transport.Transport{hubs[0].peer(), hubs[1].peer()}, Config{Target: target})
	peers := []*testTransport{hubs[0].peer(), hubs[1].peer()}
	client := newPoolForTest(t, []transport.Transport{peers[0], peers[1]}, Config{})
	addr := listenClient(t, client)
	var connections []*net.TCPConn
	for i := 0; i < 6; i++ {
		c := dialClient(t, addr)
		echoPool(t, c)
		connections = append(connections, c)
	}
	client.mu.Lock()
	counts := [2]int{}
	var firstID connectionID
	for id, s := range client.conns {
		counts[s.lane]++
		if s.lane == 0 {
			firstID = id
		}
	}
	client.mu.Unlock()
	if counts != [2]int{3, 3} {
		t.Fatalf("round-robin distribution: %v", counts)
	}
	if count(server) != 6 {
		t.Fatalf("server streams: %d", count(server))
	}
	// Even a valid stream ID must not accept data/control from another document.
	client.receive(1, (frame{server: true, kind: resetFrame, id: firstID}).marshal())
	for _, c := range connections {
		echoPool(t, c)
	}
	// Clients may use only a subset, with their own order of document URLs.
	for _, hub := range hubs {
		single := newBridge(t, hub.peer(), "")
		echoPool(t, dialClient(t, listenClient(t, single)))
	}
	peers[0].SetConnected(false)
	waitFor(t, func() bool { return count(client) == 3 })
	for i, c := range connections {
		if i%2 == 1 {
			echoPool(t, c)
		} else {
			c.SetReadDeadline(time.Now().Add(time.Second))
			var buf [1]byte
			if _, err := c.Read(buf[:]); err == nil {
				t.Fatal("lost document stream remained open")
			}
		}
	}
	// New streams skip the offline document.
	echoPool(t, dialClient(t, addr))
	client.mu.Lock()
	for _, s := range client.conns {
		if s.lane != 1 {
			t.Error("selected disconnected document")
		}
	}
	client.mu.Unlock()
	// Reconnection makes that document eligible again without moving survivors.
	peers[0].SetConnected(true)
	echoPool(t, dialClient(t, addr))
	client.mu.Lock()
	counts = [2]int{}
	for _, s := range client.conns {
		counts[s.lane]++
	}
	client.mu.Unlock()
	if counts != [2]int{1, 4} {
		t.Fatalf("distribution after reconnect: %v", counts)
	}
}

func TestPoolConnectionLimitIsGlobal(t *testing.T) {
	for _, limitedSide := range []string{"client", "server"} {
		t.Run(limitedSide, func(t *testing.T) {
			hubs := []*testHub{{}, {}}
			target := targetServer(t, func(c net.Conn) { io.Copy(c, c) })
			sc, cc := Config{Target: target}, Config{}
			if limitedSide == "client" {
				cc.MaxConnections = 2
			} else {
				sc.MaxConnections = 2
			}
			server := newPoolForTest(t, []transport.Transport{hubs[0].peer(), hubs[1].peer()}, sc)
			client := newPoolForTest(t, []transport.Transport{hubs[0].peer(), hubs[1].peer()}, cc)
			addr := listenClient(t, client)
			for i := 0; i < 2; i++ {
				echoPool(t, dialClient(t, addr))
			}
			c := dialClient(t, addr)
			c.SetReadDeadline(time.Now().Add(4 * time.Second))
			var b [1]byte
			_, err := c.Read(b[:])
			if err == nil {
				t.Fatal("extra connection accepted")
			}
			if e, ok := err.(net.Error); ok && e.Timeout() {
				t.Fatal("extra connection was not closed")
			}
			waitFor(t, func() bool { return count(client) == 2 && count(server) == 2 })
		})
	}
}

func TestEmptyTransportPool(t *testing.T) {
	for _, peers := range [][]transport.Transport{nil, {nil}} {
		if b, err := NewPool(peers, Config{}); err == nil {
			b.Close()
			t.Fatal("invalid pool accepted")
		}
	}
}
