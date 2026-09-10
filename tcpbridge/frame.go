package tcpbridge

import (
	"encoding/binary"
	"errors"
)

// Each backend message contains one frame. IDs name TCP connections, not devices.
// The direction bit keeps clients from interpreting another client's requests
// (or their own echoed messages) as server responses on a broadcast transport.
const (
	headerSize = 30
	chunkSize  = 1024
	// Window sizes are counts of 1024-byte frames, per stream and direction.
	DefaultWindowSize = 16
	MaxWindowSize     = 256
)

const (
	openFrame byte = iota + 1
	openedFrame
	dataFrame
	finFrame
	ackFrame
	resetFrame
	pingFrame
	pongFrame
)

type connectionID [16]byte

type frame struct {
	server bool
	kind   byte
	id     connectionID
	seq    uint64
	data   []byte
}

func (f frame) marshal() []byte {
	b := make([]byte, headerSize+len(f.data))
	copy(b, "OFT1")
	if f.server {
		b[4] = 1
	}
	b[5] = f.kind
	copy(b[6:22], f.id[:])
	binary.BigEndian.PutUint64(b[22:30], f.seq)
	copy(b[headerSize:], f.data)
	return b
}

func parseFrame(b []byte) (frame, error) {
	if len(b) < headerSize || len(b) > headerSize+chunkSize || string(b[:4]) != "OFT1" || b[4] > 1 {
		return frame{}, errors.New("invalid TCP bridge frame")
	}
	f := frame{server: b[4] == 1, kind: b[5], seq: binary.BigEndian.Uint64(b[22:30])}
	copy(f.id[:], b[6:22])
	if f.kind < openFrame || f.kind > pongFrame || f.id == (connectionID{}) {
		return frame{}, errors.New("invalid TCP bridge frame type or ID")
	}
	if f.kind == dataFrame {
		if len(b) == headerSize || f.seq == 0 {
			return frame{}, errors.New("invalid data frame")
		}
		f.data = append([]byte(nil), b[headerSize:]...)
	} else if len(b) != headerSize {
		return frame{}, errors.New("unexpected frame payload")
	}
	if f.kind == finFrame && f.seq == 0 {
		return frame{}, errors.New("invalid FIN sequence")
	}
	if f.kind != dataFrame && f.kind != finFrame && f.kind != ackFrame && f.seq != 0 {
		return frame{}, errors.New("unexpected frame sequence")
	}
	return f, nil
}
