package tcpbridge

import (
	"io"
	"net"
	"testing"
	"time"

	"universal-bypass-tool/transport"
)

func TestDynamicRouteDrainsOldStreamsAndRetiresLane(t *testing.T) {
	target := targetServer(t, func(c net.Conn) { io.Copy(c, c) })
	hubs := []*testHub{{}, {}}
	newPoolForTest(t, []transport.Transport{hubs[0].peer(), hubs[1].peer()}, Config{Target: target})
	client := newPoolForTest(t, []transport.Transport{hubs[0].peer()}, Config{})
	addr := listenClient(t, client)
	old := dialClient(t, addr)
	echoPool(t, old)
	lane, err := client.AddTransport(hubs[1].peer(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SelectTransports(lane); err != nil {
		t.Fatal(err)
	}
	fresh := dialClient(t, addr)
	echoPool(t, fresh)
	echoPool(t, old)
	if client.LaneConnections(0) != 1 || client.LaneConnections(lane) != 1 {
		t.Fatal("streams moved documents or new stream used old route")
	}
	client.RetireTransport(0)
	old.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := old.Read(b[:]); err == nil {
		t.Fatal("retired stream remained open")
	}
	echoPool(t, fresh)
	next, err := client.AddTransport(hubs[0].peer(), false)
	if err != nil || next <= lane {
		t.Fatal("retired lane ID was reused")
	}
	if err := client.SelectTransports(0); err == nil {
		t.Fatal("retired route was selectable")
	}
}
