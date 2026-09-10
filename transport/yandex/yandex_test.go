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

func TestBatchedCursorsIncludingKeepAlive(t *testing.T) {
	tr := NewYandexDocsTransport("", transport.DefaultConfig())
	var got [][]byte
	tr.Receive(func(b []byte) { got = append(got, b) })
	tr.handleMessage(nil, []byte(`42["message",{"type":"cursor","messages":[{"cursor":"18;---KA---"},{"cursor":"18;Zmlyc3Q="},{"cursor":"18;c2Vjb25k"},{"cursor":"18;invalid!"}]}]`))
	if len(got) != 2 || string(got[0]) != "first" || string(got[1]) != "second" {
		t.Fatalf("batched payloads: %q", got)
	}
	tr.handleMessage(nil, []byte(`42["message",{"type":"saveChanges","excelAdditionalInfo":"dGhpcmQ="}]`))
	if len(got) != 3 || string(got[2]) != "third" {
		t.Fatalf("legacy payload: %q", got)
	}
}

func TestSessionPingPayloadAndDisconnect(t *testing.T) {
	accepted := make(chan *websocket.Conn, 1)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		accepted <- c
	}))
	defer httpServer.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	remote := <-accepted
	defer remote.Close()
	remote.SetReadDeadline(time.Now().Add(5 * time.Second))
	config := transport.DefaultConfig()
	config.KeepAliveInterval = time.Hour
	tr := NewYandexDocsTransport("", config)
	tr.BaseTransport.Start()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &DocSession{Conn: conn, WriteQueue: make(chan []byte, 4), UserID: "test"}
	done := make(chan error, 1)
	go func() { done <- tr.serveSession(ctx, session) }()
	// The client waits for each handshake stage before sending the next one.
	if err := remote.WriteMessage(websocket.TextMessage, []byte(`0{"sid":"engine-session","pingInterval":25000,"pingTimeout":20000}`)); err != nil {
		t.Fatal(err)
	}
	_, b, err := remote.ReadMessage()
	if err != nil || !strings.HasPrefix(string(b), "40") {
		t.Fatalf("Socket.IO auth: %s, %v", b, err)
	}
	if tr.IsConnected() {
		t.Fatal("transport connected before Socket.IO acknowledgement")
	}
	remote.WriteMessage(websocket.TextMessage, []byte(`40{"sid":"socket-session"}`))
	_, b, err = remote.ReadMessage()
	if err != nil || !strings.HasPrefix(string(b), "42") {
		t.Fatalf("document auth: %s, %v", b, err)
	}
	if tr.IsConnected() {
		t.Fatal("transport connected before document authentication")
	}
	remote.WriteMessage(websocket.TextMessage, []byte(`42["message",{"type":"authChanges","changes":[]}]`))
	_, b, err = remote.ReadMessage()
	if err != nil || eventMetadata(b).Type != "authChangesAck" {
		t.Fatalf("auth changes acknowledgement: %s, %v", b, err)
	}
	remote.WriteMessage(websocket.TextMessage, []byte(`42["message",{"type":"auth","result":1}]`))
	deadline := time.Now().Add(time.Second)
	for !tr.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("session not connected")
		}
		time.Sleep(time.Millisecond)
	}
	generation := tr.ConnectionGeneration()
	payload := bytes.Repeat([]byte{0, 1, 2, 255}, 200)
	if err := tr.Send(payload); err != nil {
		t.Fatal(err)
	}
	_, message, err := remote.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	values := extractPayloads(message)
	if len(values) != 1 || values[0] != base64.StdEncoding.EncodeToString(payload) {
		t.Fatalf("outgoing payload: %s", message)
	}
	remote.WriteMessage(websocket.TextMessage, []byte("2"))
	_, message, err = remote.ReadMessage()
	if err != nil || string(message) != "3" {
		t.Fatalf("pong: %s, %v", message, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not stop session")
	}
	if tr.IsConnected() || tr.ConnectionGeneration() == generation {
		t.Fatal("disconnect not published")
	}
	if err := tr.Send([]byte("stale")); err == nil {
		t.Fatal("disconnected session accepted bytes")
	}
}

func TestSendOwnsBufferAndQueueIsBounded(t *testing.T) {
	tr := NewYandexDocsTransport("", transport.DefaultConfig())
	tr.session = &DocSession{WriteQueue: make(chan []byte, 1)}
	tr.SetConnected(true)
	data := []byte("original")
	if err := tr.Send(data); err != nil {
		t.Fatal(err)
	}
	data[0] = 'X'
	if err := tr.Send([]byte("overflow")); err == nil {
		t.Fatal("queue overflow accepted")
	}
	if got := <-tr.session.WriteQueue; string(got) != "original" {
		t.Fatalf("queued caller buffer was modified: %s", got)
	}
}

func TestInvalidEditorReturnsError(t *testing.T) {
	for _, config := range []string{`{}`, `{"officeActionData":{}}`, `{"officeActionData":{"editor_config":{"document":{}}}}`, `not json`} {
		t.Run(config, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, "<script id=\"client-config\">\n%s\n</script>", config)
			}))
			defer server.Close()
			tr := NewYandexDocsTransport(server.URL, transport.DefaultConfig())
			if _, err := tr.fetchDocInfo(context.Background(), server.URL, "test"); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestStopCancelsPendingDocumentRequest(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	defer server.Close()
	tr := NewYandexDocsTransport(server.URL, transport.DefaultConfig())
	if err := tr.Start(); err != nil {
		t.Fatal(err)
	}
	defer tr.Stop()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	done := make(chan struct{})
	go func() { tr.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel HTTP request")
	}
}
