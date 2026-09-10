package yandex_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"universal-bypass-tool/tcpbridge"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/utils"
)

const speedBlockSize = 64 << 10
const speedMaxBytes = 32 << 20 // Per stream, per direction.
const speedSocketWriteBuffer = 64 << 10

type speedStreamResult struct {
	Stream  int     `json:"stream"`
	Bytes   uint64  `json:"bytes"`
	Seconds float64 `json:"seconds"`
	Mbps    float64 `json:"mbps"`
	SHA256  string  `json:"sha256"`
	Error   string  `json:"error,omitempty"`
}
type speedPhaseResult struct {
	Direction string              `json:"direction"`
	Bytes     uint64              `json:"bytes"`
	Seconds   float64             `json:"seconds"`
	Mbps      float64             `json:"mbps"`
	Streams   []speedStreamResult `json:"streams"`
}

// Opt-in throughput test: configurable bridge clients, TCP streams and
// a pool of shared documents. Timings exclude document login and TCP establishment, include
// queue draining and receiver digest confirmation, and count application bytes.
func TestLiveYandexTCPSpeed(t *testing.T) {
	urls := []string{os.Getenv("OPENFLUX_YANDEX_TEST_URL")}
	if os.Getenv("OPENFLUX_YANDEX_SPEED_TEST") != "1" {
		t.Skip("set OPENFLUX_YANDEX_SPEED_TEST=1 and document URL(s)")
	}
	if v := os.Getenv("OPENFLUX_YANDEX_TEST_URLS"); v != "" {
		if err := json.Unmarshal([]byte(v), &urls); err != nil {
			t.Fatal("OPENFLUX_YANDEX_TEST_URLS must be a JSON array of document URLs")
		}
	}
	if len(urls) == 1 && urls[0] == "" {
		t.Skip("set OPENFLUX_YANDEX_TEST_URL or OPENFLUX_YANDEX_TEST_URLS")
	}
	if len(urls) == 0 {
		t.Fatal("document pool is empty")
	}
	seen := make(map[string]bool)
	for _, url := range urls {
		if url == "" || seen[url] {
			t.Fatal("document URLs must be nonempty and distinct")
		}
		seen[url] = true
	}
	utils.EnableDebug()
	connections := 12
	if v := os.Getenv("OPENFLUX_YANDEX_SPEED_CONNECTIONS"); v != "" {
		var err error
		connections, err = strconv.Atoi(v)
		if err != nil || connections < 1 || connections > 64 {
			t.Fatal("speed connections must be between 1 and 64")
		}
	}
	clients := 1
	if v := os.Getenv("OPENFLUX_YANDEX_SPEED_CLIENTS"); v != "" {
		var err error
		clients, err = strconv.Atoi(v)
		if err != nil || clients < 1 || clients > connections {
			t.Fatal("speed clients must be between 1 and the number of TCP connections")
		}
	}
	if connections%clients != 0 {
		t.Fatal("TCP connection count must be divisible by the client count")
	}
	windowSize := tcpbridge.DefaultWindowSize
	if v := os.Getenv("OPENFLUX_YANDEX_SPEED_WINDOW"); v != "" {
		var err error
		windowSize, err = strconv.Atoi(v)
		if err != nil || windowSize < 1 || windowSize > tcpbridge.MaxWindowSize {
			t.Fatalf("speed window must be between 1 and %d frames", tcpbridge.MaxWindowSize)
		}
	}
	duration := 20 * time.Second
	if v := os.Getenv("OPENFLUX_YANDEX_SPEED_DURATION"); v != "" {
		var err error
		duration, err = time.ParseDuration(v)
		if err != nil || duration < time.Second || duration > time.Minute {
			t.Fatal("speed duration must be between 1s and 1m")
		}
	}
	var uploaded, downloaded atomic.Uint64
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { target.Close() })
	var targetWG sync.WaitGroup
	acceptDone := make(chan struct{})
	// Close all bridge sockets before waiting for target handlers to exit.
	t.Cleanup(func() { target.Close(); <-acceptDone; targetWG.Wait() })
	go func() {
		defer close(acceptDone)
		for {
			c, err := target.Accept()
			if err != nil {
				return
			}
			targetWG.Add(1)
			go func() { defer targetWG.Done(); defer c.Close(); speedTarget(c, duration, &uploaded) }()
		}
	}()
	var backendTransports []*yandex.YandexDocsTransport
	connect := func(target string) *tcpbridge.Bridge {
		var peers []transport.Transport
		var raw []*yandex.YandexDocsTransport
		for _, url := range urls {
			config := transport.DefaultConfig()
			config.MaxReconnectAttempts = 2
			tr := yandex.NewYandexDocsTransport(url, config)
			raw = append(raw, tr)
			backendTransports = append(backendTransports, tr)
			peers = append(peers, transport.NewUncompressedTransport(tr))
			t.Cleanup(func() { tr.Stop() })
		}
		bridge, err := tcpbridge.NewPool(peers, tcpbridge.Config{Target: target, Timeout: 30 * time.Second, WindowSize: windowSize})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { bridge.Close() })
		for doc, tr := range raw {
			if err := tr.Start(); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(60 * time.Second)
			for !tr.IsConnected() {
				if time.Now().After(deadline) {
					t.Fatalf("document %d did not authenticate", doc+1)
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
		return bridge
	}
	connect(target.Addr().String())
	var conns []net.Conn
	for device := 0; device < clients; device++ {
		bridge := connect("")
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		go func() { _ = bridge.Serve(l) }()
		for stream := 0; stream < connections/clients; stream++ {
			c, err := net.DialTimeout("tcp", l.Addr().String(), 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			if err := c.(*net.TCPConn).SetWriteBuffer(speedSocketWriteBuffer); err != nil {
				t.Fatal(err)
			}
			c.SetDeadline(time.Now().Add(35 * time.Second))
			var ready [1]byte
			if _, err := io.ReadFull(c, ready[:]); err != nil || ready[0] != 'R' {
				t.Fatalf("stream establishment failed: %v", err)
			}
			conns = append(conns, c)
		}
	}
	for _, tr := range backendTransports {
		if !tr.IsConnected() {
			t.Fatal("a document disconnected during setup")
		}
	}
	t.Logf("Ready: %d TCP streams, %d clients, 1 server, %d documents, %d backend sessions; window=%d KiB per stream, compression=none; %s send window per direction", len(conns), clients, len(urls), len(backendTransports), windowSize, duration)
	var phases []speedPhaseResult
	for _, direction := range []string{"upload", "download"} {
		phase := speedPhaseResult{Direction: direction, Streams: make([]speedStreamResult, len(conns))}
		var wg sync.WaitGroup
		startSignal := make(chan struct{})
		counter := &uploaded
		if direction == "download" {
			counter = &downloaded
		}
		initial := counter.Load()
		progressDone := make(chan struct{})
		start := time.Now()
		go func() {
			tick := time.NewTicker(5 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-tick.C:
					t.Logf("%s progress: %.2f MiB received in %.1fs", direction, float64(counter.Load()-initial)/(1<<20), time.Since(start).Seconds())
				case <-progressDone:
					return
				}
			}
		}()
		for i, c := range conns {
			wg.Add(1)
			go func(i int, c net.Conn) {
				defer wg.Done()
				<-startSignal
				c.SetDeadline(time.Now().Add(duration + 60*time.Second))
				streamStart := time.Now()
				result := speedStreamResult{Stream: i + 1}
				var err error
				var digest []byte
				if direction == "upload" {
					result.Bytes, digest, err = speedUpload(c, duration)
				} else {
					result.Bytes, digest, err = speedDownload(c, &downloaded)
				}
				result.Seconds = time.Since(streamStart).Seconds()
				result.Mbps = float64(result.Bytes) * 8 / result.Seconds / 1e6
				result.SHA256 = fmt.Sprintf("%x", digest)
				if err != nil {
					result.Error = err.Error()
				}
				phase.Streams[i] = result
			}(i, c)
		}
		close(startSignal)
		wg.Wait()
		close(progressDone)
		phase.Seconds = time.Since(start).Seconds()
		for _, result := range phase.Streams {
			phase.Bytes += result.Bytes
			t.Logf("%s stream %02d: %.2f MiB / %.3fs = %.3f Mbps, error=%s", direction, result.Stream, float64(result.Bytes)/(1<<20), result.Seconds, result.Mbps, result.Error)
			if result.Error != "" {
				t.Errorf("%s stream %d failed: %s", direction, result.Stream, result.Error)
			}
		}
		phase.Mbps = float64(phase.Bytes) * 8 / phase.Seconds / 1e6
		t.Logf("TOTAL %s: bytes=%d seconds=%.3f Mbps=%.3f", direction, phase.Bytes, phase.Seconds, phase.Mbps)
		phases = append(phases, phase)
		if t.Failed() {
			break
		}
	}
	report := struct {
		UTC               string                     `json:"utc"`
		Connections       int                        `json:"connections"`
		Clients           int                        `json:"clients"`
		Documents         int                        `json:"documents"`
		Compression       string                     `json:"compression"`
		WindowFrames      int                        `json:"window_frames"`
		SocketWriteBuffer int                        `json:"socket_write_buffer"`
		SendSeconds       float64                    `json:"send_seconds"`
		Phases            []speedPhaseResult         `json:"phases"`
		BackendStats      []transport.TransportStats `json:"backend_stats"`
	}{UTC: time.Now().UTC().Format(time.RFC3339), Connections: len(conns), Clients: clients, Documents: len(urls), Compression: "none", WindowFrames: windowSize, SocketWriteBuffer: speedSocketWriteBuffer, SendSeconds: duration.Seconds(), Phases: phases}
	for _, tr := range backendTransports {
		report.BackendStats = append(report.BackendStats, tr.Stats())
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("OPENFLUX_YANDEX_SPEED_REPORT"); path != "" {
		if err := os.WriteFile(path, append(encoded, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for i, stats := range report.BackendStats {
		t.Logf("backend %d stats: %+v", i, stats)
	}
}

func speedWrite(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func speedSendBlocks(c net.Conn, duration time.Duration) (uint64, []byte, error) {
	block := make([]byte, speedBlockSize)
	if _, err := rand.Read(block); err != nil {
		return 0, nil, err
	}
	digest := sha256.New()
	deadline := time.Now().Add(duration)
	var total uint64
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(block)))
	for time.Now().Before(deadline) && total < speedMaxBytes {
		if err := speedWrite(c, header[:]); err != nil {
			return total, digest.Sum(nil), err
		}
		if err := speedWrite(c, block); err != nil {
			return total, digest.Sum(nil), err
		}
		digest.Write(block)
		total += uint64(len(block))
	}
	if err := speedWrite(c, []byte{0, 0, 0, 0}); err != nil {
		return total, digest.Sum(nil), err
	}
	return total, digest.Sum(nil), nil
}
func speedReceiveBlocks(c net.Conn, counter *atomic.Uint64) (uint64, []byte, error) {
	digest := sha256.New()
	block := make([]byte, speedBlockSize)
	var total uint64
	for {
		var header [4]byte
		if _, err := io.ReadFull(c, header[:]); err != nil {
			return total, digest.Sum(nil), err
		}
		n := binary.BigEndian.Uint32(header[:])
		if n == 0 {
			return total, digest.Sum(nil), nil
		}
		if n > speedBlockSize {
			return total, digest.Sum(nil), fmt.Errorf("invalid block length %d", n)
		}
		if _, err := io.ReadFull(c, block[:n]); err != nil {
			return total, digest.Sum(nil), err
		}
		digest.Write(block[:n])
		total += uint64(n)
		counter.Add(uint64(n))
	}
}
func speedFooter(total uint64, digest []byte) []byte {
	footer := make([]byte, 40)
	binary.BigEndian.PutUint64(footer[:8], total)
	copy(footer[8:], digest)
	return footer
}
func speedCheckFooter(c net.Conn, total uint64, digest []byte) error {
	var footer [40]byte
	if _, err := io.ReadFull(c, footer[:]); err != nil {
		return err
	}
	if !bytes.Equal(footer[:], speedFooter(total, digest)) {
		return fmt.Errorf("receiver byte count or SHA256 mismatch")
	}
	return nil
}
func speedTarget(c net.Conn, duration time.Duration, uploaded *atomic.Uint64) {
	if err := c.(*net.TCPConn).SetWriteBuffer(speedSocketWriteBuffer); err != nil {
		return
	}
	if err := speedWrite(c, []byte{'R'}); err != nil {
		return
	}
	for {
		var command [1]byte
		if _, err := io.ReadFull(c, command[:]); err != nil {
			return
		}
		c.SetDeadline(time.Now().Add(duration + 60*time.Second))
		var total uint64
		var digest []byte
		var err error
		switch command[0] {
		case 'U':
			total, digest, err = speedReceiveBlocks(c, uploaded)
		case 'D':
			total, digest, err = speedSendBlocks(c, duration)
		default:
			return
		}
		if err != nil {
			return
		}
		if err := speedWrite(c, speedFooter(total, digest)); err != nil {
			return
		}
	}
}
func speedUpload(c net.Conn, duration time.Duration) (uint64, []byte, error) {
	if err := speedWrite(c, []byte{'U'}); err != nil {
		return 0, nil, err
	}
	total, digest, err := speedSendBlocks(c, duration)
	if err == nil {
		err = speedCheckFooter(c, total, digest)
	}
	return total, digest, err
}
func speedDownload(c net.Conn, counter *atomic.Uint64) (uint64, []byte, error) {
	if err := speedWrite(c, []byte{'D'}); err != nil {
		return 0, nil, err
	}
	total, digest, err := speedReceiveBlocks(c, counter)
	if err == nil {
		err = speedCheckFooter(c, total, digest)
	}
	return total, digest, err
}
