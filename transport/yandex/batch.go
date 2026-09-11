package yandex

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"
)

const (
	DefaultBatchSize = 6
	MaxBatchSize     = 64
	BatchDelay       = time.Millisecond
	maxBatchBytes    = 64 << 10
	batchMagic       = "YDB1"
)

type BatchStats struct {
	CursorMessagesSent  uint64 `json:"cursor_messages_sent"`
	PayloadMessagesSent uint64 `json:"payload_messages_sent"`
}

func (t *YandexDocsTransport) BatchStats() BatchStats {
	return BatchStats{t.cursorMessagesSent.Load(), t.payloadMessagesSent.Load()}
}

// collectBatch limits both size and waiting time. A packet that does not fit
// is carried into the next write; messages are never reordered or discarded.
func collectBatch(ctx context.Context, queue <-chan []byte, first []byte, limit int) ([]byte, []byte, int, bool) {
	packets := [][]byte{first}
	size := 6 + 2 + len(first)
	var carry []byte
	if limit > 1 && size <= maxBatchBytes {
		timer := time.NewTimer(BatchDelay)
		defer timer.Stop()
	collect:
		for len(packets) < limit {
			select {
			case <-ctx.Done():
				return nil, nil, 0, false
			case <-timer.C:
				break collect
			case packet := <-queue:
				if size+2+len(packet) > maxBatchBytes {
					carry = packet
					break collect
				}
				packets = append(packets, packet)
				size += 2 + len(packet)
			}
		}
	}
	if len(packets) == 1 {
		return first, carry, 1, true
	}
	out := make([]byte, 6, size)
	copy(out, batchMagic)
	binary.BigEndian.PutUint16(out[4:], uint16(len(packets)))
	for _, packet := range packets {
		out = binary.BigEndian.AppendUint16(out, uint16(len(packet)))
		out = append(out, packet...)
	}
	return out, carry, len(packets), true
}

// Validate the complete envelope before delivering any contained message.
// Old single-message payloads remain readable, but old binaries cannot read
// the new multi-message envelopes: update all peers before enabling batching.
func unpackBatch(data []byte) ([][]byte, error) {
	if !bytes.HasPrefix(data, []byte(batchMagic)) {
		return [][]byte{data}, nil
	}
	if len(data) < 6 || len(data) > maxBatchBytes {
		return nil, fmt.Errorf("invalid batch size")
	}
	count := int(binary.BigEndian.Uint16(data[4:6]))
	if count < 2 || count > MaxBatchSize {
		return nil, fmt.Errorf("invalid batch count")
	}
	data = data[6:]
	packets := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		if len(data) < 2 {
			return nil, fmt.Errorf("truncated batch length")
		}
		n := int(binary.BigEndian.Uint16(data[:2]))
		data = data[2:]
		if n == 0 || n > len(data) {
			return nil, fmt.Errorf("invalid batch payload length")
		}
		packets = append(packets, data[:n])
		data = data[n:]
	}
	if len(data) != 0 {
		return nil, fmt.Errorf("trailing batch data")
	}
	return packets, nil
}
