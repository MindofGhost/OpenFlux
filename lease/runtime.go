package lease

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"universal-bypass-tool/tcpbridge"
	"universal-bypass-tool/transport"
)

type Config struct {
	Bootstrap []string
	Pool      []string // Server only; disjoint from Bootstrap.
	StateFile string
	Selection string // "first" or "least-clients".
	TTL       time.Duration
	Renew     time.Duration
	Drain     time.Duration
	Discover  time.Duration
	Timeout   time.Duration
	Heartbeat time.Duration
	Presence  time.Duration
	Retry     time.Duration
}

func DefaultConfig() Config {
	return Config{Selection: "first", TTL: 30 * time.Minute, Renew: 5 * time.Minute,
		Drain: 30 * time.Second, Discover: 3 * time.Second, Timeout: 15 * time.Second,
		Heartbeat: 30 * time.Second, Presence: 90 * time.Second, Retry: time.Second}
}

type Factory func(string) (transport.Transport, error)

type document struct {
	tr      *mux
	lane    int
	drainAt time.Time
}

type Runtime struct {
	config  Config
	factory Factory
	bridge  *tcpbridge.Bridge
	docs    map[string]*document
	events  chan event
	lock    *os.File
	server  serverState
	client  clientState
	seen    map[string]time.Time
}

func validURL(s string) bool {
	u, err := url.Parse(s)
	return len(s) <= 2048 && err == nil && u.Scheme == "https" && u.User == nil && u.Port() == "" &&
		(u.Host == "disk.yandex.ru" || u.Host == "disk.yandex.com") && u.Path != ""
}

// New owns the bridge and all transports it creates. Run has exactly one caller;
// Close must follow Run's return. State files are exclusively locked per process.
func New(c Config, bc tcpbridge.Config, factory Factory) (_ *Runtime, err error) {
	if factory == nil || c.StateFile == "" || len(c.Bootstrap) == 0 {
		return nil, fmt.Errorf("lease mode requires a factory, state file and bootstrap documents")
	}
	if c.Selection != "first" && c.Selection != "least-clients" {
		return nil, fmt.Errorf("lease selection must be first or least-clients")
	}
	if c.TTL <= 0 || c.Renew <= 0 || c.Drain <= 0 || c.Discover <= 0 || c.Timeout <= c.Discover || c.Heartbeat <= 0 || c.Presence <= c.Heartbeat || c.Retry <= 0 {
		return nil, fmt.Errorf("invalid lease durations (timeout must exceed discovery, presence must exceed heartbeat)")
	}
	server := bc.Target != ""
	if server != (len(c.Pool) > 0) {
		return nil, fmt.Errorf("lease server requires a pool; lease client must not specify a pool")
	}
	unique := make(map[string]bool)
	for _, group := range [][]string{c.Bootstrap, c.Pool} {
		for _, u := range group {
			if !validURL(u) || unique[u] {
				return nil, fmt.Errorf("lease documents must be distinct HTTPS disk.yandex.ru/com links; bootstrap and lease pools must not overlap")
			}
			unique[u] = true
		}
	}
	if err := os.MkdirAll(filepath.Dir(c.StateFile), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(c.StateFile+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := lockFile(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("lease state is already in use or cannot be locked: %w", err)
	}
	r := &Runtime{config: c, factory: factory, lock: f, docs: make(map[string]*document),
		events: make(chan event, 256), seen: make(map[string]time.Time)}
	defer func() {
		if err != nil {
			r.Close()
		}
	}()
	initial := append([]string(nil), c.Bootstrap...)
	if server {
		found, e := readState(c.StateFile, &r.server)
		if e != nil {
			return nil, e
		}
		if !found {
			r.server = serverState{Version: 1, ID: newID()}
		}
		if r.server.Version != 1 || !validID(r.server.ID) {
			return nil, fmt.Errorf("invalid server lease state")
		}
		currents := make(map[string]bool)
		for _, a := range r.server.Leases {
			if !validID(a.Client) || !validID(a.Token) || !validURL(a.URL) || a.Expires.IsZero() || (a.Current && currents[a.Client]) {
				return nil, fmt.Errorf("invalid server lease allocation")
			}
			if a.Current {
				currents[a.Client] = true
			}
		}
		if e := saveState(c.StateFile, r.server); e != nil {
			return nil, e
		}
		initial = append(initial, c.Pool...)
	} else {
		found, e := readState(c.StateFile, &r.client)
		if e != nil {
			return nil, e
		}
		if !found {
			r.client = clientState{Version: 1, ID: newID()}
		}
		if r.client.Version != 1 || !validID(r.client.ID) || (r.client.URL != "" && (!validURL(r.client.URL) || !validID(r.client.Server) || !validID(r.client.Token) || r.client.Expires.IsZero())) {
			return nil, fmt.Errorf("invalid client lease state")
		}
		if e := saveState(c.StateFile, r.client); e != nil {
			return nil, e
		}
		if time.Now().Before(r.client.Expires) && r.client.URL != "" {
			initial = []string{r.client.URL}
		}
	}
	var peers []transport.Transport
	for _, u := range initial {
		tr, e := factory(u)
		if e != nil {
			return nil, e
		}
		m := newMux(tr, u, r.events)
		r.docs[u] = &document{tr: m, lane: len(peers)}
		peers = append(peers, m)
	}
	r.bridge, err = tcpbridge.NewPool(peers, bc)
	if err != nil {
		return nil, err
	}
	for _, d := range r.docs {
		if e := d.tr.Start(); e != nil {
			return nil, e
		}
	}
	return r, nil
}

func (r *Runtime) Bridge() *tcpbridge.Bridge { return r.bridge }

func (r *Runtime) Close() {
	if r.bridge != nil {
		r.bridge.Close()
	}
	for _, d := range r.docs {
		d.tr.Stop()
	}
	if r.lock != nil {
		r.lock.Close()
		r.lock = nil
	}
}

func (r *Runtime) ensure(u string) error {
	if r.docs[u] != nil {
		return nil
	}
	tr, err := r.factory(u)
	if err != nil {
		return err
	}
	m := newMux(tr, u, r.events)
	lane, err := r.bridge.AddTransport(m, false)
	if err != nil {
		tr.Stop()
		return err
	}
	if err := m.Start(); err != nil {
		r.bridge.RetireTransport(lane)
		m.Stop()
		return err
	}
	r.docs[u] = &document{tr: m, lane: lane}
	return nil
}

func (r *Runtime) route(urls []string) {
	keep := make(map[string]bool)
	var lanes []int
	for _, u := range urls {
		keep[u] = true
		lanes = append(lanes, r.docs[u].lane)
	}
	r.bridge.SelectTransports(lanes...)
	for u, d := range r.docs {
		if keep[u] {
			d.drainAt = time.Time{}
		} else if d.drainAt.IsZero() {
			d.drainAt = time.Now().Add(r.config.Drain)
		}
	}
}

func (r *Runtime) drain(now time.Time) {
	for u, d := range r.docs {
		if !d.drainAt.IsZero() && (!now.Before(d.drainAt) || r.bridge.LaneConnections(d.lane) == 0) {
			r.bridge.RetireTransport(d.lane)
			d.tr.Stop()
			delete(r.docs, u)
		}
	}
}

func contains(urls []string, u string) bool {
	for _, v := range urls {
		if v == u {
			return true
		}
	}
	return false
}

func (r *Runtime) Run(ctx context.Context) error {
	if r.server.ID != "" {
		return r.runServer(ctx)
	}
	return r.runClient(ctx)
}

func (r *Runtime) runServer(ctx context.Context) error {
	tick := time.NewTicker(r.config.Heartbeat)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-tick.C:
			for id, seen := range r.seen {
				if now.Sub(seen) >= r.config.Presence {
					delete(r.seen, id)
				}
			}
		case e := <-r.events:
			if err := r.serverMessage(e, time.Now()); err != nil {
				return err
			}
		}
	}
}

func (r *Runtime) serverMessage(e event, now time.Time) error {
	m := e.msg
	if (m.Server != "" && m.Server != r.server.ID) || r.docs[e.url] == nil {
		return nil
	}
	bootstrap := contains(r.config.Bootstrap, e.url)
	authorized := r.server.authorized(m.Client, m.Token, e.url, now)
	if m.Kind != "discover" && m.Kind != "acquire" && m.Kind != "attach" && m.Kind != "heartbeat" {
		return nil
	}
	if !bootstrap && !authorized {
		return nil
	}
	if m.Kind != "discover" && m.Server != r.server.ID {
		return nil
	}
	if !validID(m.Client) || !validID(m.Request) {
		return nil
	}
	r.seen[m.Client] = now
	reply := message{Request: m.Request, Client: m.Client, Server: r.server.ID}
	for _, seen := range r.seen {
		if now.Sub(seen) < r.config.Presence {
			reply.Clients++
		}
	}
	switch m.Kind {
	case "discover", "acquire":
		var usable []string
		for _, u := range r.config.Pool {
			if r.docs[u].tr.IsConnected() {
				usable = append(usable, u)
			}
		}
		if len(usable) == 0 {
			return nil // Retry once a pool document reconnects.
		}
		next, a := r.server.allocate(m.Client, usable, now, r.config.TTL, r.config.Drain+2*r.config.Timeout)
		reply.URL, reply.Expires = a.URL, a.Expires
		if m.Kind == "discover" {
			reply.Kind = "offer"
		} else {
			if err := saveState(r.config.StateFile, next); err != nil {
				return fmt.Errorf("persist lease before grant: %w", err)
			}
			r.server = next
			reply.Kind, reply.Token = "grant", a.Token
		}
	case "attach", "heartbeat":
		if !authorized {
			return nil
		}
		reply.Kind = "ready"
	}
	// Lost replies are retried with the same request ID. Assignments are sticky,
	// so a retry cannot reserve another document or create another client.
	_ = r.docs[e.url].tr.sendControl(reply)
	return nil
}

// exchange keeps TCP streams and document draining active while control traffic
// is retried. A fresh request ID rejects late responses from previous rounds.
func (r *Runtime) exchange(ctx context.Context, urls []string, request message, kind string, least bool) (event, error) {
	request.Request, request.Client = newID(), r.client.ID
	deadline := time.Now().Add(r.config.Timeout)
	var collectUntil, nextSend time.Time
	var best event
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		now := time.Now()
		if !collectUntil.IsZero() && !now.Before(collectUntil) {
			return best, nil
		}
		if !now.Before(deadline) {
			if best.msg.Server != "" {
				return best, nil
			}
			return event{}, fmt.Errorf("lease %s timed out", request.Kind)
		}
		if !now.Before(nextSend) {
			for _, u := range urls {
				if d := r.docs[u]; d != nil && d.tr.IsConnected() {
					_ = d.tr.sendControl(request)
				}
			}
			nextSend = now.Add(r.config.Retry)
		}
		select {
		case <-ctx.Done():
			return event{}, ctx.Err()
		case now := <-tick.C:
			r.drain(now)
		case e := <-r.events:
			m := e.msg
			if m.Request != request.Request || m.Client != request.Client || m.Kind != kind || !validID(m.Server) || !contains(urls, e.url) || (request.Server != "" && m.Server != request.Server) {
				continue
			}
			if (kind == "grant" || kind == "offer") && (!validURL(m.URL) || contains(r.config.Bootstrap, m.URL) || !now.Before(m.Expires) || m.Clients < 0) {
				continue
			}
			if kind == "grant" && !validID(m.Token) {
				continue
			}
			if !least {
				return e, nil
			}
			if best.msg.Server == "" {
				collectUntil = now.Add(r.config.Discover)
				if collectUntil.After(deadline) {
					deadline = collectUntil
				}
				best = e
			} else if m.Clients < best.msg.Clients {
				best = e
			}
		}
	}
}

func (r *Runtime) activate(ctx context.Context, grant message) error {
	if err := r.ensure(grant.URL); err != nil {
		return err
	}
	// Cancel retirement if a renewed grant returns to an earlier document.
	r.docs[grant.URL].drainAt = time.Time{}
	_, err := r.exchange(ctx, []string{grant.URL}, message{Kind: "attach", Server: grant.Server, Token: grant.Token}, "ready", false)
	if err != nil {
		if grant.URL != r.client.URL {
			r.docs[grant.URL].drainAt = time.Now().Add(r.config.Drain)
		}
		return err
	}
	next := clientState{Version: 1, ID: r.client.ID, Server: grant.Server, URL: grant.URL, Token: grant.Token, Expires: grant.Expires}
	if err := saveState(r.config.StateFile, next); err != nil {
		return fmt.Errorf("persist client lease: %w", err)
	}
	moved := r.client.URL != next.URL
	r.client = next
	r.route([]string{next.URL})
	log.Printf("[LEASE] lease active, server=%s, expires=%s, moved=%t", next.Server, next.Expires.Format(time.RFC3339), moved)
	return nil
}

func (r *Runtime) discover(ctx context.Context) error {
	for _, u := range r.config.Bootstrap {
		if err := r.ensure(u); err != nil {
			return err
		}
	}
	r.route(r.config.Bootstrap)
	offer, err := r.exchange(ctx, r.config.Bootstrap, message{Kind: "discover"}, "offer", r.config.Selection == "least-clients")
	if err != nil {
		return err
	}
	log.Printf("[LEASE] selected server=%s, active clients=%d, policy=%s", offer.msg.Server, offer.msg.Clients, r.config.Selection)
	grant, err := r.exchange(ctx, []string{offer.url}, message{Kind: "acquire", Server: offer.msg.Server}, "grant", false)
	if err != nil {
		return err
	}
	return r.activate(ctx, grant.msg)
}

func (r *Runtime) pause(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		case now := <-tick.C:
			r.drain(now)
		case <-r.events: // No outstanding request: stale replies cannot change state.
		}
	}
}

func (r *Runtime) runClient(ctx context.Context) error {
	active := false
	if r.client.URL != "" && time.Now().Before(r.client.Expires) {
		err := r.activate(ctx, message{Server: r.client.Server, URL: r.client.URL, Token: r.client.Token, Expires: r.client.Expires})
		active = err == nil
		if err != nil && ctx.Err() == nil {
			log.Printf("[LEASE] cached lease unavailable; using bootstrap: %v", err)
		}
	}
	var renewAt time.Time
	for ctx.Err() == nil {
		if !active {
			if err := r.discover(ctx); err != nil {
				if ctx.Err() == nil {
					log.Printf("[LEASE] acquisition will retry: %v", err)
				}
				r.pause(ctx, r.config.Retry)
				continue
			}
			active = true
			renewAt = time.Time{}
		}
		if renewAt.IsZero() {
			renewAt = time.Now().Add(min(r.config.Renew, time.Until(r.client.Expires)/2))
		}
		wait := min(r.config.Heartbeat, time.Until(renewAt))
		if wait > 0 {
			r.pause(ctx, wait)
		}
		if ctx.Err() != nil {
			break
		}
		if !time.Now().Before(r.client.Expires) {
			active = false
			continue
		}
		renew := !time.Now().Before(renewAt)
		request := message{Kind: "heartbeat", Server: r.client.Server, Token: r.client.Token}
		kind := "ready"
		if renew {
			request.Kind, kind = "acquire", "grant"
		}
		reply, err := r.exchange(ctx, []string{r.client.URL}, request, kind, false)
		if err == nil && renew {
			err = r.activate(ctx, reply.msg)
		}
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("[LEASE] lease unavailable; using bootstrap: %v", err)
			}
			active = false
		} else if renew {
			renewAt = time.Time{}
		}
	}
	return nil
}
