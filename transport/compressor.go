package transport

import (
	"bytes"
	"io"

	"github.com/pierrec/lz4/v4"
)

const (
	MinCompressSize   = 200
	CompressionMarker = 0x1F
)

type CompressedTransport struct {
	Transport
}

func NewCompressedTransport(inner Transport) Transport {
	return &CompressedTransport{Transport: inner}
}

func (c *CompressedTransport) Send(data []byte) error {
	compressed := compress(data)
	return c.Transport.Send(compressed)
}

func (c *CompressedTransport) Receive(callback func([]byte)) {
	if callback == nil {
		c.Transport.Receive(nil)
		return
	}
	c.Transport.Receive(func(data []byte) {
		decompressed, err := decompress(data)
		if err != nil {
			callback(data) // fallback
			return
		}
		callback(decompressed)
	})
}

// Preserve session invalidation through the compression wrapper, including a
// disconnect/reconnect too brief for the bridge's IsConnected poll to observe.
func (c *CompressedTransport) ConnectionGeneration() uint64 {
	if inner, ok := c.Transport.(interface{ ConnectionGeneration() uint64 }); ok {
		return inner.ConnectionGeneration()
	}
	return 0
}

func compress(data []byte) []byte {
	if len(data) <= MinCompressSize {
		out := make([]byte, 1, len(data)+1)
		out[0] = 0x00
		out = append(out, data...)
		return out
	}

	var buf bytes.Buffer
	buf.WriteByte(CompressionMarker)

	w := lz4.NewWriter(&buf)
	w.Write(data)
	w.Close()

	if buf.Len() >= len(data)+1 {
		out := make([]byte, 1, len(data)+1)
		out[0] = 0x00
		out = append(out, data...)
		return out
	}

	return buf.Bytes()
}

func decompress(data []byte) ([]byte, error) {
	if len(data) < 1 {
		return data, nil
	}

	if data[0] == 0x00 {
		return data[1:], nil
	}

	r := lz4.NewReader(bytes.NewReader(data[1:]))
	return io.ReadAll(r)
}
