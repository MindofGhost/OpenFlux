package tcpbridge

import (
	"fmt"

	"universal-bypass-tool/transport"
)

// AddTransport registers a lane without changing existing streams. The caller
// starts/stops transports. Lane IDs are never reused, including after retirement.
func (b *Bridge) AddTransport(tr transport.Transport, acceptNew bool) (int, error) {
	if tr == nil {
		return 0, fmt.Errorf("nil transport")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ctx.Err() != nil {
		return 0, fmt.Errorf("bridge closed")
	}
	lane := b.nextLane
	b.nextLane++
	b.transports[lane] = tr
	b.lanes = append(b.lanes, lane)
	b.eligible[lane] = acceptNew
	tr.Receive(func(data []byte) { b.receive(lane, data) })
	return lane, nil
}

// SelectTransports atomically changes routing for new client streams. Existing
// streams remain pinned to their original lane until completion or retirement.
func (b *Bridge) SelectTransports(lanes ...int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, lane := range lanes {
		if b.transports[lane] == nil {
			return fmt.Errorf("unknown transport lane %d", lane)
		}
	}
	clear(b.eligible)
	for _, lane := range lanes {
		b.eligible[lane] = true
	}
	return nil
}

func (b *Bridge) LaneConnections(lane int) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, s := range b.conns {
		if s.lane == lane {
			n++
		}
	}
	return n
}

// RetireTransport aborts remaining streams and detaches the receive callback.
// It does not stop the underlying transport, which is owned by the caller.
func (b *Bridge) RetireTransport(lane int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if tr := b.transports[lane]; tr != nil {
		tr.Receive(nil)
	}
	delete(b.transports, lane)
	delete(b.eligible, lane)
	for i, id := range b.lanes {
		if id == lane {
			b.lanes = append(b.lanes[:i], b.lanes[i+1:]...)
			break
		}
	}
	for _, s := range b.conns {
		if s.lane == lane {
			s.cancel()
		}
	}
}
