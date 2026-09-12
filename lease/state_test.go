package lease

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestReservationsSurviveRestartAndSharingEndsOnRenewal(t *testing.T) {
	now := time.Now()
	pool := []string{"https://disk.yandex.ru/i/a", "https://disk.yandex.ru/i/b"}
	s := serverState{Version: 1, ID: newID()}
	a, b, c := newID(), newID(), newID()
	s, first := s.allocate(a, pool, now, time.Hour, time.Minute)
	s, second := s.allocate(b, pool, now, time.Minute, time.Minute)
	if first.URL == second.URL {
		t.Fatal("shared a document while another was free")
	}
	path := filepath.Join(t.TempDir(), "server.json")
	if err := saveState(path, s); err != nil {
		t.Fatal(err)
	}
	var restored serverState
	if _, err := readState(path, &restored); err != nil {
		t.Fatal(err)
	}
	// Presence is deliberately not restored: offline reservations still count.
	restored, third := restored.allocate(c, pool, now.Add(time.Second), time.Hour, time.Minute)
	if third.URL != first.URL {
		t.Fatal("exhausted pool did not share the least occupied document")
	}
	if !restored.authorized(a, first.Token, first.URL, now.Add(30*time.Second)) {
		t.Fatal("offline client's reservation was lost")
	}
	// The second document becomes free. The shared client moves on renewal.
	restored, moved := restored.allocate(c, pool, now.Add(2*time.Minute), time.Hour, time.Minute)
	if moved.URL != second.URL || moved.Token == third.Token {
		t.Fatal("renewal did not resolve a shared document")
	}
	if !restored.authorized(c, third.Token, third.URL, now.Add(3*time.Minute)) {
		t.Fatal("old token must survive a lost grant or a restart during migration")
	}
	// Repeated requests cannot allocate additional documents or change tokens.
	n := len(restored.Leases)
	restored, retry := restored.allocate(c, pool, now.Add(3*time.Minute), time.Hour, time.Minute)
	if retry.URL != moved.URL || retry.Token != moved.Token || len(restored.Leases) != n {
		t.Fatal("retry changed an exclusive assignment")
	}
	if !s.authorized(a, first.Token, first.URL, now.Add(30*time.Second)) || s.authorized(a, first.Token, second.URL, now) {
		t.Fatal("token is not bound to its document")
	}
}

func TestStateFilePermissionsAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "client.json")
	c := clientState{Version: 1, ID: newID()}
	if err := saveState(path, c); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || (runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		t.Fatalf("state permissions: %v, %v", info, err)
	}
	if err := os.WriteFile(path, []byte("{broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readState(path, &c); err == nil {
		t.Fatal("corrupted state was silently accepted")
	}
}
