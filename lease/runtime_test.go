package lease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"universal-bypass-tool/tcpbridge"
	"universal-bypass-tool/transport"
)

type testBus struct {
	mu        sync.Mutex
	peers     map[string][]*testPeer
	dropReady atomic.Bool
	attaches  atomic.Int64
	grants    atomic.Int64
	dropGrant atomic.Bool
}

type testPeer struct {
	*transport.BaseTransport
	bus *testBus
	url string
}

func (b *testBus) factory(url string) (transport.Transport, error) {
	p := &testPeer{BaseTransport: transport.NewBaseTransport(transport.DefaultConfig()), bus: b, url: url}
	b.mu.Lock()
	if b.peers == nil {
		b.peers = make(map[string][]*testPeer)
	}
	b.peers[url] = append(b.peers[url], p)
	b.mu.Unlock()
	return p, nil
}

func (b *testBus) connected(url string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, p := range b.peers[url] {
		if p.IsConnected() {
			n++
		}
	}
	return n
}

func (p *testPeer) Start() error {
	p.BaseTransport.Start()
	p.SetConnected(true)
	return nil
}

func (p *testPeer) Send(data []byte) error {
	if !p.IsConnected() {
		return errors.New("disconnected")
	}
	if bytes.HasPrefix(data, []byte(controlMagic)) {
		var m message
		json.Unmarshal(data[len(controlMagic):], &m)
		if m.Kind == "attach" {
			p.bus.attaches.Add(1)
		}
		if m.Kind == "ready" && p.bus.dropReady.Load() {
			return nil
		}
		if m.Kind == "grant" {
			p.bus.grants.Add(1)
			if p.bus.dropGrant.Swap(false) {
				return nil
			}
		}
	}
	p.bus.mu.Lock()
	peers := append([]*testPeer(nil), p.bus.peers[p.url]...)
	p.bus.mu.Unlock()
	for _, peer := range peers {
		if peer.IsConnected() {
			peer.CallReceive(append([]byte(nil), data...))
		}
	}
	return nil
}

func testConfig(t *testing.T, bootstrap string, pool ...string) Config {
	t.Helper()
	c := DefaultConfig()
	c.StateFile = filepath.Join(t.TempDir(), "state.json")
	c.Bootstrap, c.Pool = []string{bootstrap}, pool
	c.TTL, c.Renew, c.Drain = 10*time.Second, 2*time.Second, 350*time.Millisecond
	c.Timeout, c.Discover, c.Retry = 700*time.Millisecond, 80*time.Millisecond, 30*time.Millisecond
	c.Heartbeat, c.Presence = 100*time.Millisecond, time.Second
	return c
}

func newRuntime(t *testing.T, c Config, target string, bus *testBus) *Runtime {
	t.Helper()
	r, err := New(c, tcpbridge.Config{Target: target, Timeout: 2 * time.Second}, bus.factory)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func startRuntime(t *testing.T, r *Runtime) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("lease runtime: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("lease runtime did not stop")
			}
			r.Close()
		})
	}
	t.Cleanup(stop)
	return stop
}

func await(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not reached")
}

func readClient(t *testing.T, path string) clientState {
	t.Helper()
	var c clientState
	if _, err := readState(path, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func echoTarget(t *testing.T) string {
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
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	return l.Addr().String()
}

func listen(t *testing.T, r *Runtime) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go r.Bridge().Serve(l)
	return l.Addr().String()
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func echo(t *testing.T, c net.Conn) {
	t.Helper()
	c.SetDeadline(time.Now().Add(time.Second))
	b := bytes.Repeat([]byte("lease\x00\xff"), 500)
	if _, err := c.Write(b); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(b))
	if _, err := io.ReadFull(c, got); err != nil || !bytes.Equal(b, got) {
		t.Fatalf("TCP echo through lease: %v", err)
	}
}

func TestClientMigrationPreservesStreamsUntilDrainAndRestoresCache(t *testing.T) {
	const boot, pool = "https://disk.yandex.ru/i/boot", "https://disk.yandex.ru/i/pool"
	bus := &testBus{}
	sc := testConfig(t, boot, pool)
	s := newRuntime(t, sc, echoTarget(t), bus)
	startRuntime(t, s)
	cc := testConfig(t, boot)
	c := newRuntime(t, cc, "", bus)
	t.Cleanup(c.Close)
	bootstrapTransport := c.docs[boot].tr
	addr := listen(t, c)
	old := dial(t, addr)
	echo(t, old) // Traffic starts before lease discovery.
	bus.dropReady.Store(true)
	bus.dropGrant.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	migrated := make(chan error, 1)
	go func() { migrated <- c.discover(ctx) }()
	await(t, func() bool { return bus.attaches.Load() > 0 })
	if got := readClient(t, cc.StateFile); got.URL != "" {
		t.Fatal("switched before server readiness on the leased document")
	}
	echo(t, old)
	bus.dropReady.Store(false)
	// A renamed state file is visible before directory fsync and route selection
	// finish. Await the whole operation, not just visibility of the new file.
	select {
	case err := <-migrated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("migration did not finish")
	}
	stop := startRuntime(t, c)
	cached := readClient(t, cc.StateFile)
	if bus.grants.Load() < 2 {
		t.Fatal("lost grant was not retried")
	}
	fresh := dial(t, addr)
	echo(t, fresh)
	echo(t, old)
	await(t, func() bool { return c.Bridge().LaneConnections(0) == 0 })
	await(t, func() bool { return !bootstrapTransport.IsConnected() })
	old.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := old.Read(b[:]); err == nil {
		t.Fatal("old stream survived drain timeout")
	}
	echo(t, fresh)
	stop()
	restarted := newRuntime(t, cc, "", bus)
	if len(restarted.docs) != 1 || restarted.docs[pool] == nil || restarted.client.ID != cached.ID {
		t.Fatal("unexpired cache did not bypass bootstrap or preserve identity")
	}
	stopRestarted := startRuntime(t, restarted)
	echo(t, dial(t, listen(t, restarted)))
	stopRestarted()
	cached.Expires = time.Now().Add(-time.Second)
	if err := saveState(cc.StateFile, cached); err != nil {
		t.Fatal(err)
	}
	expired := newRuntime(t, cc, "", bus)
	defer expired.Close()
	if len(expired.docs) != 1 || expired.docs[boot] == nil {
		t.Fatal("expired cache bypassed bootstrap")
	}
}

func TestLeastClientsSelectionAndUnselectedServerDoesNotReserve(t *testing.T) {
	bus := &testBus{}
	const b1, b2 = "https://disk.yandex.ru/i/b1", "https://disk.yandex.ru/i/b2"
	const p1, p2 = "https://disk.yandex.ru/i/p1", "https://disk.yandex.ru/i/p2"
	s1c, s2c := testConfig(t, b1, p1), testConfig(t, b2, p2)
	s1 := newRuntime(t, s1c, echoTarget(t), bus)
	s2 := newRuntime(t, s2c, echoTarget(t), bus)
	for i := 0; i < 5; i++ {
		s1.seen[newID()] = time.Now()
	}
	stop1, stop2 := startRuntime(t, s1), startRuntime(t, s2)
	cc := testConfig(t, b1)
	cc.Bootstrap, cc.Selection = []string{b1, b2}, "least-clients"
	c := newRuntime(t, cc, "", bus)
	stopClient := startRuntime(t, c)
	await(t, func() bool { return readClient(t, cc.StateFile).URL == p2 })
	await(t, func() bool { return bus.connected(b1) == 1 && bus.connected(b2) == 1 })
	echo(t, dial(t, listen(t, c)))
	stopClient()
	stop1()
	stop2()
	if len(s1.server.Leases) != 0 || len(s2.server.Leases) != 1 {
		t.Fatal("discovery reserved documents at an unselected server")
	}
}

func TestRenewalMovesSharedLeaseAndRetriesLostGrant(t *testing.T) {
	const boot, p1, p2 = "https://disk.yandex.ru/i/boot", "https://disk.yandex.ru/i/p1", "https://disk.yandex.ru/i/p2"
	bus := &testBus{}
	sc := testConfig(t, boot, p1, p2)
	s := newRuntime(t, sc, echoTarget(t), bus)
	id, token, now := newID(), newID(), time.Now()
	s.server.Leases = []allocation{
		{Client: id, URL: p1, Token: token, Expires: now.Add(5 * time.Second), Current: true},
		{Client: newID(), URL: p1, Token: newID(), Expires: now.Add(5 * time.Second), Current: true},
		{Client: newID(), URL: p2, Token: newID(), Expires: now.Add(150 * time.Millisecond), Current: true},
	}
	if err := saveState(sc.StateFile, s.server); err != nil {
		t.Fatal(err)
	}
	startRuntime(t, s)
	cc := testConfig(t, boot)
	cc.Renew = 300 * time.Millisecond
	cache := clientState{Version: 1, ID: id, Server: s.server.ID, URL: p1, Token: token, Expires: now.Add(5 * time.Second)}
	if err := saveState(cc.StateFile, cache); err != nil {
		t.Fatal(err)
	}
	c := newRuntime(t, cc, "", bus)
	addr := listen(t, c)
	old := dial(t, addr)
	echo(t, old)
	bus.dropGrant.Store(true)
	startRuntime(t, c)
	await(t, func() bool { return readClient(t, cc.StateFile).URL == p2 })
	updated := readClient(t, cc.StateFile)
	if updated.Token == token || !updated.Expires.After(cache.Expires) || bus.grants.Load() < 2 {
		t.Fatal("renewal failed to move, extend or retry the lease")
	}
	echo(t, old)
	await(t, func() bool { return c.Bridge().LaneConnections(0) == 0 })
	echo(t, dial(t, addr))
}

func TestFirstSelectionDoesNotWaitForOtherServers(t *testing.T) {
	const b1, b2, pool = "https://disk.yandex.ru/i/b1", "https://disk.yandex.ru/i/b2", "https://disk.yandex.ru/i/pool"
	bus := &testBus{}
	s := newRuntime(t, testConfig(t, b1, pool), echoTarget(t), bus)
	for i := 0; i < 20; i++ {
		s.seen[newID()] = time.Now()
	}
	startRuntime(t, s)
	cc := testConfig(t, b1)
	cc.Bootstrap = []string{b2, b1} // First URL is unavailable.
	c := newRuntime(t, cc, "", bus)
	startRuntime(t, c)
	await(t, func() bool { return readClient(t, cc.StateFile).URL == pool })
	await(t, func() bool { return bus.connected(b1) == 1 && bus.connected(b2) == 0 })
	echo(t, dial(t, listen(t, c)))
}

func TestStateLockAndServerRestartRetainAssignments(t *testing.T) {
	const boot, pool = "https://disk.yandex.ru/i/boot", "https://disk.yandex.ru/i/pool"
	bus := &testBus{}
	sc := testConfig(t, boot, pool)
	s := newRuntime(t, sc, "127.0.0.1:443", bus)
	if other, err := New(sc, tcpbridge.Config{Target: "127.0.0.1:443"}, bus.factory); err == nil {
		other.Close()
		t.Fatal("two runtimes locked the same state file")
	}
	id := newID()
	request := event{boot, message{Kind: "acquire", Client: id, Request: newID(), Server: s.server.ID}}
	if err := s.serverMessage(request, time.Now()); err != nil {
		t.Fatal(err)
	}
	want := s.server.Leases[0]
	s.Close()
	restarted := newRuntime(t, sc, "127.0.0.1:443", bus)
	defer restarted.Close()
	if got := restarted.server.Leases[0]; got.Token != want.Token || got.URL != want.URL || !got.Expires.Equal(want.Expires) {
		t.Fatal("server restart lost the lease")
	}
	request.msg.Request = newID()
	if err := restarted.serverMessage(request, time.Now()); err != nil || len(restarted.server.Leases) != 1 {
		t.Fatal("retry after restart duplicated the lease")
	}
	// A persistence failure must not publish or remember a successful grant.
	request.msg.Client = newID()
	restarted.config.StateFile = filepath.Join(t.TempDir(), "missing", "state.json")
	before := bus.grants.Load()
	if err := restarted.serverMessage(request, time.Now()); err == nil || bus.grants.Load() != before || len(restarted.server.Leases) != 1 {
		t.Fatal("server granted an unpersisted assignment")
	}
}

func TestCachedUnavailableDocumentFallsBackToBootstrap(t *testing.T) {
	const boot, pool = "https://disk.yandex.ru/i/boot", "https://disk.yandex.ru/i/pool"
	bus := &testBus{}
	s := newRuntime(t, testConfig(t, boot, pool), echoTarget(t), bus)
	startRuntime(t, s)
	cc := testConfig(t, boot)
	cache := clientState{Version: 1, ID: newID(), Server: newID(), URL: "https://disk.yandex.ru/i/gone", Token: newID(), Expires: time.Now().Add(time.Hour)}
	if err := saveState(cc.StateFile, cache); err != nil {
		t.Fatal(err)
	}
	c := newRuntime(t, cc, "", bus)
	startRuntime(t, c)
	await(t, func() bool { return readClient(t, cc.StateFile).URL == pool })
	await(t, func() bool { return bus.connected(boot) == 1 })
	echo(t, dial(t, listen(t, c)))
}

func TestInvalidConfigurationAndStateFailWithoutOverwriting(t *testing.T) {
	bus := &testBus{}
	c := testConfig(t, "https://disk.yandex.ru/i/boot")
	if err := os.WriteFile(c.StateFile, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if r, err := New(c, tcpbridge.Config{}, bus.factory); err == nil {
		r.Close()
		t.Fatal("corrupted client state accepted")
	}
	b, _ := os.ReadFile(c.StateFile)
	if string(b) != "corrupt" {
		t.Fatal("corrupted state was overwritten")
	}
	c.Pool = append([]string(nil), c.Bootstrap...)
	if r, err := New(c, tcpbridge.Config{Target: "127.0.0.1:443"}, bus.factory); err == nil {
		r.Close()
		t.Fatal("bootstrap document was allowed into the lease pool")
	}
}
