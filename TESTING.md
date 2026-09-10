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
Pool tests simulate independent broadcast documents and verify round-robin
distribution, clients using subsets, replies through the correct document,
rejection of another document's frames for an existing stream, isolation of
document failures, recovery for new streams, and the shared connection limit
on both client and server. CLI tests cover repeated URLs, duplicate suppression
and rejection of pools in unsupported modes. These tests do not measure live
throughput across multiple Yandex documents.
Yandex tests also cover batched cursor events, ping/pong, co-editing authentication
acknowledgement and session shutdown.

To repeat the live TLS test against your own document (it sends synthetic cursor
messages, and is skipped unless the environment variable is set):

```bash
OPENFLUX_YANDEX_TEST_URL="YOUR_YANDEX_DOC_URL" \
  go test -v ./transport/yandex -run '^TestLiveYandexTCPBridge$' -timeout 240s
```

To measure raw TCP throughput over 24 simultaneous connections (two clients,
twelve streams each), separately in each direction:

```bash
OPENFLUX_YANDEX_TEST_URL="YOUR_YANDEX_DOC_URL" \
  OPENFLUX_YANDEX_SPEED_TEST=1 \
  OPENFLUX_YANDEX_SPEED_CONNECTIONS=24 \
  OPENFLUX_YANDEX_SPEED_REPORT=/tmp/openflux-speed.json \
  go test -v ./transport/yandex -run '^TestLiveYandexTCPSpeed$' -timeout 300s
```

`OPENFLUX_YANDEX_SPEED_CONNECTIONS` defaults to 12 and accepts even numbers
from 2 to 64, divided equally between two bridge clients. The benchmark uses
the same LZ4 transport wrapper as the CLI. The test's sending TCP sockets request
a 64 KiB write buffer to reduce queued data after the sending interval; production
bridge socket settings are unchanged. Run throughput tests without `-race`.

To measure 12 streams over two documents, supply their URLs as a JSON array:

```bash
OPENFLUX_YANDEX_TEST_URLS='["DOCUMENT_A_URL","DOCUMENT_B_URL"]' \
  OPENFLUX_YANDEX_SPEED_TEST=1 \
  OPENFLUX_YANDEX_SPEED_CONNECTIONS=12 \
  OPENFLUX_YANDEX_SPEED_REPORT=/tmp/openflux-speed-two-docs.json \
  go test -v -count=1 ./transport/yandex -run '^TestLiveYandexTCPSpeed$' -timeout 420s
```

`OPENFLUX_YANDEX_TEST_URLS` overrides the single-document variable. Both clients
and the server use the entire pool through `tcpbridge.NewPool`. With two
documents and 12 streams, each client opens three streams per document, giving
six streams per document and six WebSocket sessions in total. All configured
sessions authenticate before that bridge starts opening streams. Backend
counters in the report are ordered by server, client 1, client 2, and then by
document within each group. The report records the document count, not URLs.

The default send interval is 20 seconds per direction, configurable using
`OPENFLUX_YANDEX_SPEED_DURATION` (1s–1m). Each stream is capped at 32 MiB per
direction. Reported throughput counts application payload only, excludes
document login/TCP setup, and includes draining queued data and receiver SHA-256
verification. Both bridge ends run locally, but all measured payload traverses
the real Yandex document; this is not a measurement between two physical devices
on different access networks. Tests are skipped unless explicitly enabled.

## 12 connections across two documents

Measured on 2026-09-10 with two distinct shared documents, one server and two
clients. Every bridge subscribed to both documents, creating six WebSocket
sessions. Each client opened six TCP streams through the pool, alternating
between documents: three per document per client, six per document in total.
Settings matched the LZ4 benchmark above (20s send interval, 64 KiB requested
test socket write buffers, 1024-byte frames, 16-frame window per stream).

| Direction | Verified payload | Elapsed including drain | Aggregate throughput |
|---|---:|---:|---:|
| Clients → server | 22.0625 MiB | 23.083 s | 8.018 Mbit/s |
| Server → clients | 32.3125 MiB | 25.035 s | 10.827 Mbit/s |

All 12 streams passed byte-count and SHA-256 verification in both directions.
All six backend sessions remained connected with zero reconnects. This is one
run through the actual Yandex backend with both bridge ends on the same machine;
no simultaneous single-document control run was performed. Earlier results
below were measured at another time and do not establish a controlled speedup.

The [JSON report](test-results/yandex-tcp-lz4-12-streams-2-documents-2026-09-10.json)
contains per-stream timings, hashes and counters for all six sessions, without
document URLs or credentials.

## 12 versus 24 connections with LZ4

Measured on 2026-09-10 using the same document, two bridge clients and one
server, in the order 12 → 24 → 24 → 12. Each run sent for 20 seconds per
direction, followed by draining queued data and checking byte counts and SHA-256.
The bridge used its default 1024-byte chunks and 16-frame per-stream window.
Payloads were random 64 KiB blocks repeated within each stream; the LZ4 wrapper
was enabled, as in the CLI. These are TCP streams multiplexed over two client
document sessions, not 12 or 24 separate document sessions.

| TCP connections | Run | Clients → server, Mbit/s | Server → clients, Mbit/s |
|---|---:|---:|---:|
| 12 | 1 | 7.170 | 7.174 |
| 24 | 1 | 9.821 | 8.783 |
| 24 | 2 | 7.975 | 7.784 |
| 12 | 2 | 5.810 | 7.536 |

Combining the two runs by dividing total verified payload bits by total elapsed
phase time gives 6.492 → 8.825 Mbit/s upload (**+35.9%**) and
7.345 → 8.269 Mbit/s download (**+12.6%**). Increasing to 24 streams improved
aggregate throughput in these runs, with substantial run-to-run variation.
Two runs per setting do not establish a stable backend limit or guarantee the
same improvement on another network. More simultaneous streams also increase
the total outstanding data window; these results do not mean a single TCP
connection becomes faster or that doubling streams doubles throughput.

All streams passed byte-count and SHA-256 checks in both directions. Some
document-page, WebSocket or authentication attempts timed out during setup and
succeeded on retry; no sessions reconnected during the timed transfers.
An initial attempt before the co-editing fix failed authentication/session
stability and was excluded. The fix acknowledges `connectState.waitAuth` to
release our editor's authentication lock without saving document edits. Its
implementation is on `main-transport`; its regression test is on `main-test`.

The four reports contain per-stream results and backend counters, with no
document URL or credentials:

- [12 streams, run 1](test-results/yandex-tcp-lz4-12-streams-2026-09-10-run1.json)
- [24 streams, run 1](test-results/yandex-tcp-lz4-24-streams-2026-09-10-run1.json)
- [24 streams, run 2](test-results/yandex-tcp-lz4-24-streams-2026-09-10-run2.json)
- [12 streams, run 2](test-results/yandex-tcp-lz4-12-streams-2026-09-10-run2.json)

## Historical 12-connection measurement (before LZ4)

This earlier run used the bare Yandex transport and default TCP send buffers.
It is not a controlled comparison with the current benchmark, which enables
the CLI's LZ4 wrapper and requests smaller test socket write buffers.

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
