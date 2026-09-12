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
  OPENFLUX_YANDEX_SPEED_CLIENTS=2 \
  OPENFLUX_YANDEX_SPEED_CONNECTIONS=24 \
  OPENFLUX_YANDEX_SPEED_REPORT=/tmp/openflux-speed.json \
  go test -v ./transport/yandex -run '^TestLiveYandexTCPSpeed$' -timeout 300s
```

`OPENFLUX_YANDEX_SPEED_CONNECTIONS` defaults to 12 and accepts integers
from 1 to 64. `OPENFLUX_YANDEX_SPEED_CLIENTS` defaults to **1**; set it to 2
to reproduce the earlier two-client topology. Streams are divided equally
between clients, so the connection count must be divisible by the client count.
There is always one bridge server. Like the CLI, the current benchmark skips
LZ4 compression on send and retains the uncompressed wire marker. Its default
window is 16 frames (16 KiB) per stream/direction. Use
`OPENFLUX_YANDEX_SPEED_WINDOW=16` explicitly, or choose
any integer from 1 to 256. The setting applies to both clients and the server.
Reports include `compression: "none"` and `window_frames` so that new results
can be distinguished from earlier LZ4/16-frame runs.
`OPENFLUX_YANDEX_SPEED_BATCH` defaults to 6 messages (range 1–64), with a 1ms
partial-batch flush. The value applies to every bridge in the test. Set it to
`1` to disable batching. ACKs are sent after each incoming frame is processed;
ACK aggregation and its CLI/environment options have been removed.
Reports include `batch_size` and `batch_stats` counters distinguishing payload
messages from actual cursor events sent (excluding keep-alives and authentication).
The test's sending TCP sockets request
a 64 KiB write buffer to reduce queued data after the sending interval; production
bridge socket settings are unchanged. Run throughput tests without `-race`.

To measure 12 streams over two documents with one client and one server,
supply their URLs as a JSON array:

```bash
OPENFLUX_YANDEX_TEST_URLS='["DOCUMENT_A_URL","DOCUMENT_B_URL"]' \
  OPENFLUX_YANDEX_SPEED_TEST=1 \
  OPENFLUX_YANDEX_SPEED_CLIENTS=1 \
  OPENFLUX_YANDEX_SPEED_CONNECTIONS=12 \
  OPENFLUX_YANDEX_SPEED_WINDOW=16 \
  OPENFLUX_YANDEX_SPEED_BATCH=6 \
  OPENFLUX_YANDEX_SPEED_REPORT=/tmp/openflux-speed-two-docs.json \
  go test -v -count=1 ./transport/yandex -run '^TestLiveYandexTCPSpeed$' -timeout 420s
```

`OPENFLUX_YANDEX_TEST_URLS` overrides the single-document variable. Each client
and the server use the entire pool through `tcpbridge.NewPool`. With one client,
two documents and 12 streams, the client opens six streams per document, giving
four WebSocket sessions in total: two from the client and two from the server.
Generally, session count is `(clients + 1) × documents`. All configured
sessions authenticate before that bridge starts opening streams. Backend
counters in the report are ordered by server, then clients, and then by
document within each group. The report records the document count, not URLs.

For a controlled comparison, keep clients, documents, streams, duration and
window identical and test these settings in separate runs:

| Variant | `OPENFLUX_YANDEX_SPEED_BATCH` |
|---|---:|
| Without batching | 1 |
| Current default | 6 |
| Earlier batching experiment | 8 |

Local regression tests cover batch ordering, malformed envelopes without
partial delivery, byte limits, cancellation and partial-batch timers at the
current default batch size. Existing tests also exercise ACK loss, half-close,
window limits, document isolation and reconnects.

The default send interval is 20 seconds per direction, configurable using
`OPENFLUX_YANDEX_SPEED_DURATION` (1s–1m). Each stream is capped at 32 MiB per
direction. Reported throughput counts application payload only, excludes
document login/TCP setup, and includes draining queued data and receiver SHA-256
verification. Both bridge ends run locally, but all measured payload traverses
the real Yandex document; this is not a measurement between two physical devices
on different access networks. Tests are skipped unless explicitly enabled.

## Document lease regression tests

Run the local lease and TCP migration checks without document URLs:

```bash
go test -race ./lease ./tcpbridge
```

Lease tests use an in-memory broadcast transport and real local TCP sockets.
They check data forwarding before discovery, first-response and least-client
selection, reservation only at the selected server, lost-grant retries, readiness
before switching, draining and forced retirement of old streams, shared-document
renewal onto a free document, client/server state restoration, expired and
unavailable cached documents, exclusive file locks, corrupt files and persistence
failure before a grant. Allocation tests also verify that disconnected clients'
reservations survive and that old tokens work during a migration retry.

The existing throughput test exercises the static pool; it does not measure
lease discovery or migration. No new Yandex throughput measurement accompanies
the lease change. Live lease validation requires a bootstrap document and at
least one distinct allocation document per server; use the CLI examples in the
README. Watch `[LEASE]` messages for the selected server and confirmed move.

## Historical batching and cumulative ACK comparison, 2026-09-11

Three sequential runs used one client, one server, two documents (four sessions),
24 TCP streams, a 16-frame window and no compression. Each direction had a 40s
send interval; elapsed time includes draining and SHA-256 verification. All
streams passed in both directions. Both bridge ends ran on the same machine
through the actual Yandex backend. The current implementation retains only batching,
with a default of 6. ACK aggregation was discarded; the following reports
preserve the earlier experimental settings and results.

| Variant | Batch | ACK every | Upload, Mbit/s | Download, Mbit/s |
|---|---:|---:|---:|---:|
| Previous behavior | 1 | 1 | 15.637 | 16.234 |
| Batching only | 8 | 1 | 29.738 | 30.111 |
| Batching + cumulative ACK | 8 | 8 | 28.173 | 23.353 |

Batching alone improved throughput by approximately 90% upload and 85% download
in these runs. The batching-only counters averaged 7.80 payload messages per
cursor event. Cumulative ACKs did not provide an additional speed improvement
in this comparison. These are single runs per setting in a changing network;
they do not establish a stable limit or isolate why ACK aggregation was slower.
The batching-only run retried one WebSocket connection during setup; no sessions
reconnected during timed transfers in any run. All modes retain bounded queues
and abort streams on overflow; backpressure on a full queue is not implemented
by this experiment.

Reports, including logical message and actual cursor-event counters:

- [Previous behavior](test-results/yandex-batching-baseline-2026-09-11.json)
- [Batching only](test-results/yandex-batching-only-2026-09-11.json)
- [Batching and cumulative ACK](test-results/yandex-batching-ack-2026-09-11.json)

## Historical two-client run: 12 connections across two documents

Measured on 2026-09-10 with two distinct shared documents, one server and two
clients. Every bridge subscribed to both documents, creating six WebSocket
sessions. Each client opened six TCP streams through the pool, alternating
between documents: three per document per client, six per document in total.
This historical run used LZ4 (20s send interval, 64 KiB requested
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
It is not a controlled comparison with the current benchmark, which retains
the wire marker and requests smaller test socket write buffers.

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
