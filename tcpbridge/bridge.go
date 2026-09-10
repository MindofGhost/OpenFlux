// Package tcpbridge forwards TCP byte streams through a message transport.
// It does not encapsulate IP packets or terminate the forwarded TLS session.
package tcpbridge

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"universal-bypass-tool/transport"
	"universal-bypass-tool/utils"
)

type Config struct {
	// Target selects server mode. An empty Target selects client mode.
	Target         string
	Timeout        time.Duration
	MaxConnections int
}

type Bridge struct {
	trans  transport.Transport
	config Config
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	conns  map[connectionID]*stream
	closed map[connectionID]time.Time
	wg     sync.WaitGroup
	stop   sync.Once
}

func New(trans transport.Transport, config Config) (*Bridge, error) {
	if config.Target != "" {
		if _, _, err := net.SplitHostPort(config.Target); err != nil {
			return nil, fmt.Errorf("target: %w", err)
		}
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = 1024
	}
	if config.Timeout < time.Second || config.MaxConnections < 1 {
		return nil, errors.New("timeout must be at least 1s and max connections must be positive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Bridge{trans: trans, config: config, ctx: ctx, cancel: cancel,
		conns: make(map[connectionID]*stream), closed: make(map[connectionID]time.Time)}
	trans.Receive(b.receive)
	b.wg.Add(1)
	go b.monitor()
	return b, nil
}

func (b *Bridge) isServer() bool { return b.config.Target != "" }

// Serve accepts client-side TCP connections. The caller owns the transport's
// lifetime; Close stops the bridge and closes the listener and all TCP streams.
func (b *Bridge) Serve(listener net.Listener) error {
	if b.isServer() {
		return errors.New("a server bridge does not accept local connections")
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-b.ctx.Done():
			listener.Close()
		case <-done:
		}
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if b.ctx.Err() != nil {
				return nil
			}
			return err
		}
		var id connectionID
		if _, err := rand.Read(id[:]); err != nil {
			conn.Close()
			return fmt.Errorf("connection ID: %w", err)
		}
		if !b.trans.IsConnected() || !b.add(id, conn) {
			conn.Close()
		}
	}
}

func (b *Bridge) add(id connectionID, conn net.Conn) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ctx.Err() != nil || len(b.conns) >= b.config.MaxConnections || b.conns[id] != nil {
		return false
	}
	if until, ok := b.closed[id]; ok && time.Now().Before(until) {
		return false
	}
	ctx, cancel := context.WithCancel(b.ctx)
	s := &stream{bridge: b, id: id, ctx: ctx, cancel: cancel, conn: conn,
		in: make(chan frame, 4*windowSize), generation: b.generation()}
	b.conns[id] = s
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		s.run()
	}()
	return true
}

func (b *Bridge) receive(data []byte) {
	f, err := parseFrame(data)
	if err != nil || f.server == b.isServer() || b.ctx.Err() != nil {
		return
	}
	b.mu.Lock()
	s := b.conns[f.id]
	b.mu.Unlock()
	if s == nil {
		// Unknown DATA/FIN/ACK never open sockets. Only clients initiate OPEN.
		if b.isServer() && f.kind == openFrame && b.trans.IsConnected() {
			b.add(f.id, nil)
		}
		return
	}
	select {
	case s.in <- f:
	case <-s.ctx.Done():
	default:
		// Never block the shared transport's receive loop on one slow stream.
		s.cancel()
	}
}

func (b *Bridge) remove(s *stream) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.conns, s.id)
	// A bounded replay cache prevents delayed OPEN from redialing a closed
	// connection. IDs are never deliberately reused.
	if len(b.closed) >= 2*b.config.MaxConnections {
		var oldest connectionID
		var earliest time.Time
		for id, until := range b.closed {
			if earliest.IsZero() || until.Before(earliest) {
				oldest, earliest = id, until
			}
		}
		delete(b.closed, oldest)
	}
	b.closed[s.id] = time.Now().Add(2 * b.config.Timeout)
}

// Backends can expose a generation counter to detect a disconnect/reconnect
// that occurs between monitor ticks. Other backends still get stream heartbeats.
func (b *Bridge) generation() uint64 {
	if t, ok := b.trans.(interface{ ConnectionGeneration() uint64 }); ok {
		return t.ConnectionGeneration()
	}
	return 0
}

func (b *Bridge) monitor() {
	defer b.wg.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case now := <-ticker.C:
			b.mu.Lock()
			generation := b.generation()
			for _, s := range b.conns {
				if !b.trans.IsConnected() || generation != s.generation {
					s.cancel()
				}
			}
			for id, until := range b.closed {
				if now.After(until) {
					delete(b.closed, id)
				}
			}
			b.mu.Unlock()
		}
	}
}

func (b *Bridge) Close() error {
	b.stop.Do(func() {
		b.mu.Lock()
		b.cancel()
		b.mu.Unlock()
		b.trans.Receive(nil)
		b.wg.Wait()
	})
	return nil
}

type stream struct {
	bridge     *Bridge
	id         connectionID
	ctx        context.Context
	cancel     context.CancelFunc
	conn       net.Conn
	in         chan frame
	generation uint64
}

func (s *stream) send(kind byte, seq uint64, data []byte) error {
	if !s.bridge.trans.IsConnected() || s.generation != s.bridge.generation() {
		return errors.New("transport session lost")
	}
	return s.bridge.trans.Send((frame{server: s.bridge.isServer(), kind: kind, id: s.id, seq: seq, data: data}).marshal())
}

type readResult struct {
	data []byte
	err  error
}

func (s *stream) run() {
	reset := true
	abort := true
	defer func() {
		s.cancel()
		if s.conn != nil {
			if abort {
				abortConnection(s.conn)
			} else {
				s.conn.Close()
			}
		}
		if reset {
			_ = s.send(resetFrame, 0, nil)
		}
		s.bridge.remove(s)
	}()
	timeout := s.bridge.config.Timeout
	if s.bridge.isServer() {
		ctx, cancel := context.WithTimeout(s.ctx, timeout/2)
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", s.bridge.config.Target)
		cancel()
		if err != nil {
			utils.Debugf("[TCP] %x target dial failed: %v", s.id, err)
			return
		}
		s.conn = conn
		if s.ctx.Err() != nil || s.send(openedFrame, 0, nil) != nil {
			return
		}
	} else {
		if s.send(openFrame, 0, nil) != nil {
			return
		}
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case f := <-s.in:
			if f.kind != openedFrame {
				reset = f.kind != resetFrame
				return
			}
		case <-s.ctx.Done():
			return
		case <-timer.C:
			return
		}
	}

	utils.Debugf("[TCP] %x opened", s.id)
	// Cancellation must unblock a socket write as well as a socket read.
	watchDone := make(chan struct{})
	watchExited := make(chan struct{})
	defer func() {
		close(watchDone)
		<-watchExited
	}()
	go func() {
		defer close(watchExited)
		select {
		case <-s.ctx.Done():
			abortConnection(s.conn)
		case <-watchDone:
		}
	}()
	reads := make(chan readResult)
	go s.readLoop(reads)
	ticker := time.NewTicker(timeout / 4)
	defer ticker.Stop()
	lastReceive, lastAck := time.Now(), time.Now()
	var sent, acked, received uint64
	var localFIN, remoteFIN bool
	pending := make(map[uint64]frame)

	for {
		if localFIN && remoteFIN && sent == acked {
			reset = false
			abort = false
			return
		}
		var readCh <-chan readResult
		if !localFIN && sent-acked < windowSize {
			readCh = reads
		}
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			if now.Sub(lastReceive) >= timeout || (sent > acked && now.Sub(lastAck) >= timeout) {
				utils.Debugf("[TCP] %x peer/ack timeout", s.id)
				return
			}
			if s.send(pingFrame, 0, nil) != nil {
				return
			}
		case r := <-readCh:
			kind := dataFrame
			if r.err != nil {
				if r.err != io.EOF {
					return
				}
				kind, localFIN = finFrame, true
			}
			if sent == acked {
				lastAck = time.Now()
			}
			sent++
			if s.send(kind, sent, r.data) != nil {
				return
			}
		case f := <-s.in:
			lastReceive = time.Now()
			switch f.kind {
			case resetFrame:
				reset = false
				return
			case openFrame:
				if !s.bridge.isServer() || s.send(openedFrame, 0, nil) != nil {
					return
				}
			case openedFrame:
				if s.bridge.isServer() {
					return
				}
			case pingFrame:
				if s.send(pongFrame, 0, nil) != nil {
					return
				}
			case pongFrame:
			case ackFrame:
				if f.seq > sent {
					return
				}
				if f.seq > acked {
					acked, lastAck = f.seq, time.Now()
				}
			case dataFrame, finFrame:
				if f.seq <= received {
					if s.send(ackFrame, received, nil) != nil {
						return
					}
					continue
				}
				if remoteFIN || f.seq-received > windowSize {
					return
				}
				pending[f.seq] = f
				for {
					next, ok := pending[received+1]
					if !ok {
						break
					}
					if remoteFIN {
						return
					}
					if next.kind == finFrame {
						if c, ok := s.conn.(interface{ CloseWrite() error }); !ok || c.CloseWrite() != nil {
							return
						}
						remoteFIN = true
					} else {
						_ = s.conn.SetWriteDeadline(time.Now().Add(timeout))
						if err := writeAll(s.conn, next.data); err != nil {
							return
						}
					}
					received++
					delete(pending, received)
				}
				if s.send(ackFrame, received, nil) != nil {
					return
				}
			}
		}
	}
}

// A failed stream must appear as a TCP error, not a successful EOF on a
// truncated response. Normal FIN completion still uses an ordinary Close.
func abortConnection(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}

func (s *stream) readLoop(out chan<- readResult) {
	buf := make([]byte, chunkSize)
	for {
		n, err := s.conn.Read(buf)
		if n > 0 {
			select {
			case out <- readResult{data: append([]byte(nil), buf[:n]...)}:
			case <-s.ctx.Done():
				return
			}
		}
		if err != nil {
			select {
			case out <- readResult{err: err}:
			case <-s.ctx.Done():
			}
			return
		}
	}
}

func writeAll(w io.Writer, b []byte) error {
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
