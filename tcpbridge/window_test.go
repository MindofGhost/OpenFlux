package tcpbridge

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

func TestWindowConfigurationBounds(t *testing.T) {
	for _, size := range []int{-1, MaxWindowSize + 1} {
		if b, err := New((&testHub{}).peer(), Config{WindowSize: size}); err == nil {
			b.Close()
			t.Fatalf("accepted invalid window %d", size)
		}
	}
}

func TestReorderAcrossLargerWindow(t *testing.T) {
	const size = 64
	var held []byte
	hub := &testHub{hook: func(f frame, b []byte) [][]byte {
		if !f.server && f.kind == dataFrame {
			if f.seq == 1 {
				held = append([]byte(nil), b...)
				return nil
			}
			if f.seq == size {
				return [][]byte{b, held}
			}
		}
		return [][]byte{b}
	}}
	target := targetServer(t, func(c net.Conn) { io.Copy(c, c) })
	makeBridge := func(target string) *Bridge {
		b, err := New(hub.peer(), Config{Target: target, Timeout: 2 * time.Second, WindowSize: size})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { b.Close() })
		return b
	}
	makeBridge(target)
	c := dialClient(t, listenClient(t, makeBridge("")))
	payload := bytes.Repeat([]byte{0, 1, 2, 255}, chunkSize*size/4)
	if err := writeAll(c, payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("larger receive window reordered or truncated payload")
	}
}
