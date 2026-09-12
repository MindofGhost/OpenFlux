// Package lease assigns document transports to clients and preserves leases
// across restarts. Control messages share the existing message transport.
package lease

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func validID(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 16
}

type allocation struct {
	Client  string    `json:"client"`
	URL     string    `json:"url"`
	Token   string    `json:"token"`
	Expires time.Time `json:"expires"`
	Current bool      `json:"current"`
}

type serverState struct {
	Version int          `json:"version"`
	ID      string       `json:"server_id"`
	Leases  []allocation `json:"leases"`
}

type clientState struct {
	Version int       `json:"version"`
	ID      string    `json:"client_id"`
	Server  string    `json:"server_id,omitempty"`
	URL     string    `json:"url,omitempty"`
	Token   string    `json:"token,omitempty"`
	Expires time.Time `json:"expires,omitempty"`
}

// saveState replaces the file only after the complete new contents are synced.
// A failed save must never be followed by a successful lease grant.
func saveState(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".openflux-state-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(append(data, '\n')); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func readState(path string, v any) (bool, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return false, fmt.Errorf("invalid lease state %s: %w", path, err)
	}
	return true, nil
}

func (s *serverState) authorized(client, token, url string, now time.Time) bool {
	for _, a := range s.Leases {
		if a.Client == client && a.Token == token && a.URL == url && now.Before(a.Expires) {
			return true
		}
	}
	return false
}

// allocate keeps disconnected clients' reservations until expiry. Sharing is
// allowed only after every usable document has a live reservation. Renewing a
// shared assignment moves it to an exclusive document when one becomes free.
func (s serverState) allocate(client string, pool []string, now time.Time, ttl, hold time.Duration) (serverState, allocation) {
	next := serverState{Version: s.Version, ID: s.ID}
	owners := make(map[string]map[string]bool)
	available := make(map[string]bool)
	for _, url := range pool {
		available[url] = true
		owners[url] = make(map[string]bool)
	}
	var current allocation
	for _, a := range s.Leases {
		if !now.Before(a.Expires) {
			continue
		}
		next.Leases = append(next.Leases, a)
		if available[a.URL] {
			owners[a.URL][a.Client] = true
		}
		if a.Client == client && a.Current {
			current = a
		}
	}
	chosen := current.URL
	if !available[chosen] {
		chosen = pool[0]
		for _, url := range pool {
			if len(owners[url]) < len(owners[chosen]) {
				chosen = url
			}
		}
	} else if len(owners[chosen]) > 1 {
		for _, url := range pool {
			if len(owners[url]) == 0 {
				chosen = url
				break
			}
		}
	}
	if chosen == current.URL {
		for i := range next.Leases {
			if next.Leases[i].Client == client && next.Leases[i].Current {
				next.Leases[i].Expires = now.Add(ttl)
				return next, next.Leases[i]
			}
		}
	}
	for i := range next.Leases {
		if next.Leases[i].Client == client && next.Leases[i].Current {
			next.Leases[i].Current = false
			if until := now.Add(hold); next.Leases[i].Expires.Before(until) {
				next.Leases[i].Expires = until
			}
		}
	}
	a := allocation{Client: client, URL: chosen, Token: newID(), Expires: now.Add(ttl), Current: true}
	next.Leases = append(next.Leases, a)
	return next, a
}
