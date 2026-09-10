# Testing and diagnostics

This document and the test files live on `main-test`, based on `main-transport`.
Use [README.md](README.md) for normal build and forwarding commands.

A live smoke test on 2026-09-10 passed with a shared `.docx` document, one server,
two clients and four concurrent TLS connections, each echoing 9728 bytes exactly.
This verifies TLS forwarding; the throughput measurement below exercises raw
TCP streams without AnyTLS/VLESS or TLS overhead.

## Local verification

```bash
go test -race ./...
```

Tests use local TCP/TLS servers and a simulated broadcast document, including
multiple clients, concurrent connections, binary payloads, half-close, reordered
and duplicated frames, message/ACK loss, reconnects and unavailable targets.
Yandex tests also cover batched cursor events, ping/pong and session shutdown.

To repeat the live TLS test against your own document (it sends synthetic cursor
messages, and is skipped unless the environment variable is set):

```bash
OPENFLUX_YANDEX_TEST_URL="YOUR_YANDEX_DOC_URL" \
  go test -v ./transport/yandex -run '^TestLiveYandexTCPBridge$' -timeout 240s
```

To measure raw TCP throughput over 12 simultaneous connections (two clients,
six streams each), separately in each direction:

```bash
OPENFLUX_YANDEX_TEST_URL="YOUR_YANDEX_DOC_URL" \
  OPENFLUX_YANDEX_SPEED_TEST=1 \
  OPENFLUX_YANDEX_SPEED_REPORT=/tmp/openflux-speed.json \
  go test -v ./transport/yandex -run '^TestLiveYandexTCPSpeed$' -timeout 300s
```

The default send interval is 20 seconds per direction, configurable using
`OPENFLUX_YANDEX_SPEED_DURATION` (1s–1m). Each stream is capped at 32 MiB per
direction. Reported throughput counts application payload only, excludes
document login/TCP setup, and includes draining queued data and receiver SHA-256
verification. Both bridge ends run locally, but all measured payload traverses
the real Yandex document; this is not a measurement between two physical devices
on different access networks. Tests are skipped unless explicitly enabled.

## Recorded 12-connection measurement

Measured on 2026-09-10 Moscow time (2026-09-09 22:44:46 UTC), through a real
shared `.docx` document with one server and two clients, six streams per client.
The bridge retained its default 1024-byte chunks and 16-frame per-stream window.
The payload comprised randomly generated 64 KiB blocks, repeated within each
stream, with SHA-256 and byte-count verification at the receiving end.

| Direction | Payload received | End-to-end elapsed | Aggregate throughput |
|---|---:|---:|---:|
| Clients → server | 41.6875 MiB | 47.761 s | 7.322 Mbit/s (0.915 MB/s) |
| Server → clients | 46.5000 MiB | 57.745 s | 6.755 Mbit/s (0.844 MB/s) |

All 12 streams passed in both directions. These are aggregate rates, not rates
per connection. The sender ran for approximately 20 seconds in each direction;
TCP socket buffers held additional data, so the measured elapsed time includes
waiting for all bytes to reach the receiver and checking the digest. The result
must not be calculated by dividing the byte count by 20 seconds.

The server's first document-page request timed out before the timed phases;
its retry succeeded. There were no transport reconnects during data transfer.
Both bridge ends ran on the same machine and accessed the real Yandex backend.
This is one run on that network path, not a guarantee of speed from other
networks or a measurement of AnyTLS/VLESS performance.

The [JSON report](test-results/yandex-tcp-12-streams-2026-09-10.json) includes
per-stream timings, byte counts, hashes and backend counters. It contains no
document URL, cookies or access tokens.
