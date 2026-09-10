package transport

import (
	"bytes"
	"testing"
)

type captureTransport struct {
	*BaseTransport
	packet []byte
}

func (t *captureTransport) Send(b []byte) error {
	t.packet = append([]byte(nil), b...)
	return nil
}

func TestUncompressedSendAndCompatibleReceive(t *testing.T) {
	inner := &captureTransport{BaseTransport: NewBaseTransport(DefaultConfig())}
	plain := NewUncompressedTransport(inner)
	payload := bytes.Repeat([]byte("compressible payload"), 256)
	if err := plain.Send(payload); err != nil {
		t.Fatal(err)
	}
	if len(inner.packet) != len(payload)+1 || inner.packet[0] != 0 || !bytes.Equal(inner.packet[1:], payload) {
		t.Fatal("uncompressed transport changed or compressed the payload")
	}
	var received []byte
	plain.Receive(func(b []byte) { received = append([]byte(nil), b...) })
	inner.CallReceive(inner.packet)
	if !bytes.Equal(received, payload) {
		t.Fatal("uncompressed round trip failed")
	}
	// Retain decoding of a peer that still sends LZ4-framed messages.
	old := &captureTransport{BaseTransport: NewBaseTransport(DefaultConfig())}
	if err := NewCompressedTransport(old).Send(payload); err != nil {
		t.Fatal(err)
	}
	if old.packet[0] != CompressionMarker {
		t.Fatal("expected compressed fixture")
	}
	inner.CallReceive(old.packet)
	if !bytes.Equal(received, payload) {
		t.Fatal("compressed peer compatibility failed")
	}
	plain.Receive(nil)
	inner.CallReceive(old.packet)
	inner.SetConnected(true)
	if plain.(interface{ ConnectionGeneration() uint64 }).ConnectionGeneration() != inner.ConnectionGeneration() {
		t.Fatal("wrapper lost session generation")
	}
}
