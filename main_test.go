package main

import "testing"

func TestModeValidation(t *testing.T) {
	for _, tc := range []struct {
		name, mode           string
		client, server, exit bool
		target               string
		valid                bool
	}{
		{"legacy client", "socks5", true, false, false, "", true},
		{"legacy exit", "socks5", false, false, true, "", true},
		{"tcp client", "tcp", true, false, false, "", true},
		{"tcp server", "tcp", false, true, false, "localhost:443", true},
		{"no role", "tcp", false, false, false, "", false},
		{"two roles", "tcp", true, true, false, "localhost:443", false},
		{"missing target", "tcp", false, true, false, "", false},
		{"client target", "tcp", true, false, false, "localhost:443", false},
		{"raw exit in tcp", "tcp", false, false, true, "", false},
		{"server in legacy", "socks5", false, true, false, "localhost:443", false},
		{"unknown mode", "udp", true, false, false, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMode(tc.mode, tc.client, tc.server, tc.exit, tc.target)
			if (err == nil) != tc.valid {
				t.Fatalf("validation = %v, valid=%t", err, tc.valid)
			}
		})
	}
}
