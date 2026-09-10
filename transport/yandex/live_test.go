package yandex_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"universal-bypass-tool/tcpbridge"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/utils"
)

// Opt-in: this test sends cursor messages through a real shared document.
// The document URL and its access tokens are never stored in the repository.
func TestLiveYandexTCPBridge(t *testing.T) {
	url := os.Getenv("OPENFLUX_YANDEX_TEST_URL")
	if url == "" {
		t.Skip("set OPENFLUX_YANDEX_TEST_URL to test a real shared document")
	}
	utils.EnableDebug()
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read failed", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(data)
	}))
	defer target.Close()
	makeBridge := func(target string) (*tcpbridge.Bridge, *yandex.YandexDocsTransport) {
		config := transport.DefaultConfig()
		config.MaxReconnectAttempts = 2
		tr := yandex.NewYandexDocsTransport(url, config)
		b, err := tcpbridge.New(tr, tcpbridge.Config{Target: target, Timeout: 30 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { b.Close(); tr.Stop() })
		if err := tr.Start(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(60 * time.Second)
		for !tr.IsConnected() {
			if time.Now().After(deadline) {
				t.Fatal("Yandex transport did not connect; see debug output")
			}
			time.Sleep(50 * time.Millisecond)
		}
		return b, tr
	}
	_, serverTransport := makeBridge(target.Listener.Addr().String())
	t.Log("server WebSocket connected")
	var clients []*http.Client
	var transports []*yandex.YandexDocsTransport
	for i := 0; i < 2; i++ {
		bridge, tr := makeBridge("")
		transports = append(transports, tr)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		go func() { _ = bridge.Serve(l) }()
		httpTransport := target.Client().Transport.(*http.Transport).Clone()
		httpTransport.DisableKeepAlives = true
		httpTransport.ForceAttemptHTTP2 = false
		addr := l.Addr().String()
		httpTransport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}
		t.Cleanup(httpTransport.CloseIdleConnections)
		clients = append(clients, &http.Client{Transport: httpTransport, Timeout: 45 * time.Second})
		t.Logf("client %d WebSocket connected", i+1)
	}
	var wg sync.WaitGroup
	for device, client := range clients {
		for stream := 0; stream < 2; stream++ {
			wg.Add(1)
			go func(device, stream int, client *http.Client) {
				defer wg.Done()
				payload := bytes.Repeat([]byte(fmt.Sprintf("OpenFlux live TLS device=%d stream=%d\x00\xff\n", device, stream)), 256)
				start := time.Now()
				resp, err := client.Post(target.URL, "application/octet-stream", bytes.NewReader(payload))
				if err != nil {
					t.Errorf("client %d stream %d: %v", device, stream, err)
					return
				}
				defer resp.Body.Close()
				received, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Errorf("client %d stream %d read: %v", device, stream, err)
					return
				}
				if resp.StatusCode != http.StatusOK || !bytes.Equal(payload, received) {
					t.Errorf("client %d stream %d: status=%d, mismatched response (%d bytes)", device, stream, resp.StatusCode, len(received))
					return
				}
				t.Logf("client %d stream %d: %d bytes echoed through TLS, SHA256=%x, elapsed=%s", device, stream, len(received), sha256.Sum256(received), time.Since(start).Round(time.Millisecond))
			}(device, stream, client)
		}
	}
	wg.Wait()
	t.Logf("server stats: %+v", serverTransport.Stats())
	for i, tr := range transports {
		t.Logf("client %d stats: %+v", i, tr.Stats())
	}
}
