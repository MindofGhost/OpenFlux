package yandex

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"universal-bypass-tool/transport"
)

func TestBatchRoundTripAndMalformedEnvelope(t *testing.T) {
	queue := make(chan []byte, 8)
	for i := 1; i < 8; i++ {
		queue <- bytes.Repeat([]byte{byte(i)}, 1055)
	}
	first := bytes.Repeat([]byte{0}, 1055)
	encoded, carry, count, ok := collectBatch(context.Background(), queue, first, 8)
	if !ok || carry != nil || count != 8 {
		t.Fatal("batch collection failed")
	}
	packets, err := unpackBatch(encoded)
	if err != nil || len(packets) != 8 {
		t.Fatalf("decode: %v", err)
	}
	for i, p := range packets {
		if !bytes.Equal(p, bytes.Repeat([]byte{byte(i)}, 1055)) {
			t.Fatal("packet changed")
		}
	}
	tr := NewYandexDocsTransport("", transport.DefaultConfig())
	var got [][]byte
	tr.Receive(func(b []byte) { got = append(got, b) })
	for _, bad := range [][]byte{encoded[:5], encoded[:len(encoded)-1], append(append([]byte(nil), encoded...), 0)} {
		tr.handleMessage(nil, []byte(fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, base64.StdEncoding.EncodeToString(bad))))
		if len(got) != 0 {
			t.Fatal("malformed batch delivered a partial payload")
		}
	}
	tr.handleMessage(nil, []byte(fmt.Sprintf(`42["message",{"type":"cursor","cursor":"18;%s"}]`, base64.StdEncoding.EncodeToString(encoded))))
	if len(got) != 8 {
		t.Fatal("batch not split before callback")
	}
}

func TestBatchByteLimitAndCancellation(t *testing.T) {
	queue := make(chan []byte, 2)
	large := bytes.Repeat([]byte{1}, maxBatchBytes)
	queue <- large
	first := []byte("first")
	packet, carry, n, ok := collectBatch(context.Background(), queue, first, 8)
	if !ok || n != 1 || !bytes.Equal(packet, first) || !bytes.Equal(carry, large) {
		t.Fatal("oversized next packet lost or reordered")
	}
	packet, carry, n, ok = collectBatch(context.Background(), queue, carry, 8)
	if !ok || n != 1 || carry != nil || !bytes.Equal(packet, large) {
		t.Fatal("large standalone packet changed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, ok = collectBatch(ctx, queue, first, 8)
	if ok {
		t.Fatal("cancelled batch flushed")
	}
}

func TestWriterBatchesAndFlushesPartialBatch(t *testing.T) {
	accepted := make(chan *websocket.Conn, 1)
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err == nil {
			accepted <- c
		}
	}))
	defer h.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(h.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	remote := <-accepted
	defer remote.Close()
	remote.SetReadDeadline(time.Now().Add(2 * time.Second))
	config := transport.DefaultConfig()
	config.KeepAliveInterval = time.Hour
	tr := NewYandexDocsTransportWithBatch("", config, DefaultBatchSize)
	session := &DocSession{Conn: conn, WriteQueue: make(chan []byte, 16)}
	for i := 0; i < DefaultBatchSize+1; i++ {
		session.WriteQueue <- []byte(fmt.Sprintf("packet-%d", i))
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); tr.writerLoop(ctx, session) }()
	defer func() { cancel(); <-done }()
	var got [][]byte
	for i := 0; i < 2; i++ {
		_, msg, err := remote.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		payloads := extractPayloads(msg)
		if len(payloads) != 1 {
			t.Fatal("expected one cursor event")
		}
		b, err := base64.StdEncoding.DecodeString(payloads[0])
		if err != nil {
			t.Fatal(err)
		}
		packets, err := unpackBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, packets...)
	}
	if len(got) != DefaultBatchSize+1 {
		t.Fatalf("delivered %d packets", len(got))
	}
	for i, p := range got {
		if string(p) != fmt.Sprintf("packet-%d", i) {
			t.Fatal("writer changed packet order")
		}
	}
	cancel()
	<-done
	stats := tr.BatchStats()
	if stats.CursorMessagesSent != 2 || stats.PayloadMessagesSent != uint64(DefaultBatchSize+1) {
		t.Fatalf("batch counters: %+v", stats)
	}
}
