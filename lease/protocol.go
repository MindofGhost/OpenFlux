package lease

import (
	"bytes"
	"encoding/json"
	"sync"
	"time"

	"universal-bypass-tool/transport"
)

const controlMagic = "OFL1"

type message struct {
	Kind    string    `json:"kind"`
	Request string    `json:"request"`
	Client  string    `json:"client"`
	Server  string    `json:"server,omitempty"`
	URL     string    `json:"url,omitempty"`
	Token   string    `json:"token,omitempty"`
	Expires time.Time `json:"expires,omitempty"`
	Clients int       `json:"clients,omitempty"`
}

type event struct {
	url string
	msg message
}

// mux consumes control frames before the TCP bridge decoder. The bounded event
// queue never blocks TCP delivery; control requests are retried after loss.
type mux struct {
	transport.Transport
	mu       sync.RWMutex
	callback func([]byte)
}

func newMux(tr transport.Transport, url string, events chan<- event) *mux {
	m := &mux{Transport: tr}
	tr.Receive(func(data []byte) {
		if bytes.HasPrefix(data, []byte(controlMagic)) {
			if len(data) > 4096 {
				return
			}
			var msg message
			if json.Unmarshal(data[len(controlMagic):], &msg) != nil || !validID(msg.Client) || !validID(msg.Request) {
				return
			}
			select {
			case events <- event{url, msg}:
			default:
			}
			return
		}
		m.mu.RLock()
		cb := m.callback
		m.mu.RUnlock()
		if cb != nil {
			cb(data)
		}
	})
	return m
}

func (m *mux) Receive(cb func([]byte)) {
	m.mu.Lock()
	m.callback = cb
	m.mu.Unlock()
}

func (m *mux) ConnectionGeneration() uint64 {
	if tr, ok := m.Transport.(interface{ ConnectionGeneration() uint64 }); ok {
		return tr.ConnectionGeneration()
	}
	return 0
}

func (m *mux) sendControl(msg message) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return m.Send(append([]byte(controlMagic), b...))
}
