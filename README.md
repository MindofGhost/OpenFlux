# OpenFlux

**English** | [Русский](README.ru.md)

Network stack research tool. TCP tunnel with pluggable transports.

## Overview
```
Client (SOCKS5) --> Transport --> Exit Node --> Internet
```

## Requirements
1. Golang v. 1.26.3+ - is required for building desktop client / exit node binary (universal-bypass-tool);
2. Android Native Development Kit (NDK) v.27.0.12077973+ - is required for building Android client binary;
3. XCode v. 26.6+ - is required for building iOS client binary;
4. Linux VPS / VDS exit node.

## Overview

TCP packets are sent via Transport. Currently, there are two transports available:
1. Yandex - sends packets via Yandex Docs cursor messages;
2. Max - sends packets via WebRTC DataChannel.

Client side runs a SOCKS5 proxy, exit node decapsulates and forwards packets to destination point.

The optional `--mode tcp` forwards TCP **byte streams** to a fixed server-side
destination. It can carry an existing AnyTLS or VLESS TCP+TLS connection without
terminating TLS, encapsulating IP packets, or using the SOCKS5 server.

## Structure

```
universal-bypass-tool/
├── main.go
├── transport/
│   ├── transport.go      # Transport interface
│   └── yandex/           # Yandex Docs backend
│   └── oneme/            # MAX Messenger backend
├── tunnel/
│   ├── tunnel.go         # TCP tunnel core
│   ├── endpoint.go       # Virtual NIC
│   └── rawsocket.go      # Raw socket (exit node)
├── socks5/               # SOCKS5 server
├── network/              # Checksums, packet parsing
└── utils/                # Debug logging
```

## Build (desktop client / exit-node binary)

```bash
go mod tidy
go build -o universal-bypass-tool .
```

## Build for Android (client binary)
```bash
export ANDROID_NDK_HOME=<your Android NDK path>
./build_android.sh
```

## Build for iOS (client binary)
```bash
export XCODE_PATH="<your Xcode.app path>" # optional, defaults to /Applications/Xcode.app
./build_ios.sh
```

## Usage

### 1. Setting up exit node
1. You must have root access on exit node machine;
2. Only legacy Yandex document editor is supported (you can toggle this setting from the interface).

Setup commands for exit node:
```bash
sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -j DROP
sudo ./universal-bypass-tool --exit-node --url "YOUR_YANDEX_DOC_URL" --debug
```

### 1. Setting up desktop client:

Setup commands for desktop client:
```bash
./universal-bypass-tool --client --url "YOUR_YANDEX_DOC_URL" --socks5 :1080 --debug
```

Then set up SOCKS5 proxy in your browser at localhost:1080.

## TCP forwarding through one document

Build the updated binary on both ends:

```bash
go build -o openflux .
```

On the machine running your AnyTLS/VLESS server (or another TCP service):

```bash
./openflux --mode tcp --server --transport yandex \
  --url "YOUR_YANDEX_DOC_URL" --target 127.0.0.1:443
```

On each client device:

```bash
./openflux --mode tcp --client --transport yandex \
  --url "YOUR_YANDEX_DOC_URL" --listen 127.0.0.1:15000
```

Point the AnyTLS/VLESS client at `127.0.0.1:15000`, keeping the real TLS server
name/SNI and certificate verification settings. Each accepted local connection
opens a separate TCP connection to `--target` on the server. The target may also
be another host reachable from the server. These are ordinary TCP sockets; this
mode does not require root, raw sockets, or the iptables rule above.

Several devices may run the same client command against **one document and one
bridge server**. There is no `client-id` flag: each TCP connection gets a random
128-bit ID generated with `crypto/rand`. Frames include direction and connection
ID, so clients ignore requests from other clients and responses for unknown IDs.
The ID provides routing, not authentication or private delivery inside the
document. TLS remains end-to-end between the protocol client and server.

Use a separate document for the legacy SOCKS5/IP tunnel. Multi-client support
applies to the new TCP mode; the legacy tunnel still has a fixed client IP.
The shared bridge uses the `Transport` interface and can also be selected with
`--transport oneme` and its existing connection flags, but this does not change
MAX's existing call topology into a multi-client server. The documented
multi-client setup is for Yandex Docs.

### A pool of documents

Repeat `--url` to open one Yandex session per document. The server subscribes
to the whole pool; each client may use any subset, in any order:

```bash
# Server: accept streams from both documents and forward to one TCP service.
./openflux --mode tcp --server --transport yandex \
  --url "DOCUMENT_A_URL" --url "DOCUMENT_B_URL" --target 127.0.0.1:443

# Client: distribute new local TCP connections across both documents.
./openflux --mode tcp --client --transport yandex \
  --url "DOCUMENT_A_URL" --url "DOCUMENT_B_URL" --listen 127.0.0.1:15000

# Another client can use just document B.
./openflux --mode tcp --client --transport yandex \
  --url "DOCUMENT_B_URL" --listen 127.0.0.1:15000
```

New TCP streams use authenticated document sessions in round-robin order,
skipping disconnected sessions. Each stream stays on its chosen document;
responses return through that document. A document failure closes its streams,
while streams on other documents continue. Reconnected sessions become eligible
for new streams. Established streams are not moved between documents, and a
single TCP stream does not combine their bandwidth. Session availability does
not guarantee that the bridge server is present; opening still has a timeout.

Configure only documents served by the same bridge server, with one server
subscription per document. Repeating the exact same URL has no effect; avoid
using different links to the same document. The pool is supported in Yandex TCP
mode; legacy SOCKS5/IP mode accepts a single document. Clients subscribed only
to document A do not receive cursor traffic from document B.

### Stream behavior and limits

- Opening, data, half-close, reset, acknowledgements and heartbeat messages are
  framed separately. A FIN closes only the receiving TCP socket's write side,
  allowing the application to finish sending its response.
- Data is split into 1024-byte chunks. A stream defaults to at most 16 unacknowledged
  chunks (16 KiB per direction); acknowledgements follow writes to the receiving TCP socket. A bounded
  reorder buffer restores order and discards duplicate sequence numbers.
  `--tcp-window` accepts 1–256 chunks; set the same value on client and server.
  The default is `--tcp-window 16`. Larger windows increase
  buffering and the load on the shared transport queue, without guaranteeing
  higher throughput.
- In Yandex TCP mode, `--yandex-batch` defaults to 6 messages per cursor event.
  A partial batch waits at most 1ms, and a batch is capped at 64 KiB before
  Base64 encoding. This does not increase the per-stream TCP window. Packet
  boundaries, stream IDs and sequence numbers are preserved. Set
  `--yandex-batch 1` to disable batching. Update all peers before enabling it:
  older binaries cannot decode the new batch envelope. Legacy SOCKS5/IP mode
  continues to send single messages.
- Lost messages are **not retransmitted** in this first version. Missing
  acknowledgements or peer heartbeats close the affected stream after the
  timeout, rather than delivering bytes after a gap. Queue overflow and socket
  errors also close the stream.
- Yandex transport disconnects invalidate that document's active streams, including brief
  disconnect/reconnect cycles. Old outbound queues are discarded. Applications
  must open new TCP connections after the backend reconnects; existing TLS
  sessions are not resumed by the bridge.
- `--tcp-timeout` defaults to `30s` (minimum `1s`) for opening, socket writes,
  peer liveness and acknowledgements; target dialing uses half that duration.
  `--tcp-max-connections` defaults to `1024` per bridge process, including
  connections still opening, across the entire document pool. Clients accepted while all backends are disconnected
  are closed; an absent/full bridge server results in an opening timeout.
- Streams on the same document share its backend bandwidth and outer TCP connection. This
  mode does not provide UDP-like latency or independent loss recovery per stream.

Yandex sends messages without attempting LZ4 compression. The one-byte
uncompressed marker and receive-side LZ4 decoder remain for compatibility with
compression-framed peers. Update both ends and use matching TCP windows when
comparing performance. MAX continues using its existing LZ4 wrapper.

The backend still targets the old Yandex editor described above. It waits for
Engine.IO, Socket.IO and document authentication before reporting connected.

## Document leases and server selection

`--lease` enables leases in Yandex TCP mode. Configure each server with its own
bootstrap documents (`--url`) and a separate allocation pool (`--lease-url`).
Create the documents beforehand. All documents must be distinct across servers
and roles, including different links pointing to the same document.

Server A:

```bash
./universal-bypass-tool --mode tcp --server --transport yandex --lease \
  --target 127.0.0.1:443 \
  --url 'https://disk.yandex.ru/i/ENTRY_A' \
  --lease-url 'https://disk.yandex.ru/i/WORK_A1' \
  --lease-url 'https://disk.yandex.ru/i/WORK_A2' \
  --lease-state .openflux-state/server-a.json
```

Run server B with its own entry document, allocation documents, state file and
target. Give the client both entry documents:

```bash
./universal-bypass-tool --mode tcp --client --transport yandex --lease \
  --listen 127.0.0.1:15000 \
  --url 'https://disk.yandex.ru/i/ENTRY_A' \
  --url 'https://disk.yandex.ru/i/ENTRY_B' \
  --lease-select least-clients \
  --lease-state .openflux-state/client.json
```

Replace these example links with your documents. Lease mode accepts HTTPS
`disk.yandex.ru` and `disk.yandex.com` links. TCP forwarding starts on available
bootstrap documents while discovery runs. `first` chooses the first response;
`least-clients` collects responses for another 3 seconds and chooses the lowest
reported client count, preserving arrival order on ties. Offers do not reserve
documents: only the selected server persists a grant.

The client connects to the granted document and waits for the server's reply
there before saving the lease and routing all new streams to it. Existing
streams stay on old documents until completion or the 30-second drain deadline.
Remaining streams are then aborted and the old document sessions are stopped.
Existing TCP/TLS connections cannot move between documents; applications must
reconnect after forced retirement. Renewal that changes the document uses the
same procedure.

Reservations last 30 minutes, including offline time. Only when all usable
documents are reserved may a document be shared; the server chooses the one with
the fewest distinct lease holders. A shared lease moves to a free document on
renewal when possible. Old reservations survive until their expiry, extended
when needed to cover draining and control retries. Clients renew every 5 minutes
or after half the remaining lifetime, whichever is earlier.

A client restart uses an unexpired saved document directly. Expired leases or
no response on a saved document within 15 seconds cause bootstrap discovery.
Control requests are retried; an exclusive assignment remains sticky across
retries and server restarts. Data-frame retransmission behavior is unchanged.

Client counts estimate unique clients heard from in the past 90 seconds;
clients send a heartbeat every 30 seconds. This includes bootstrap clients and
is independent of TCP stream counts. Presence is rebuilt after a server restart;
offline reservations persist independently of presence.

Each client state file contains an automatically generated persistent ID; no
manual `client-id` is needed. Use a different file per device and do not copy
client state between devices. State is atomically replaced and exclusively
locked, and corrupt state causes a startup error instead of silently forgetting
leases. Preserve the server file across restarts. Leases distribute load; they
do not provide document privacy or replace target-service authentication.

The server opens one session for every bootstrap and allocation document,
including unused allocation documents. After migration a client normally has
one session, with extra sessions during discovery and draining. Without
`--lease`, the static document pool works as before. Batching remains 6 messages
and the default TCP window remains 16 KiB per stream.

| Lease flag | Default | Description |
|---|---|---|
| `--lease` | `false` | Enable leases on client and server |
| `--lease-url` | | Server allocation document; repeat to provide a pool |
| `--lease-state` | | Required state file path, separate per process |
| `--lease-select` | `first` | Client policy: `first` or `least-clients` |
| `--lease-ttl` | `30m` | Server reservation lifetime |
| `--lease-renew` | `5m` | Client renewal interval |
| `--lease-drain` | `30s` | Client stream drain deadline; also used for retaining old server reservations |
| `--lease-discovery` | `3s` | Offer collection time for `least-clients`; must be less than 15s |

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--client` | | Run as client |
| `--exit-node` | | Run as exit node |
| `--socks5` | `:1080` | SOCKS5 listen addr |
| `--url` | | Required Yandex document URL; repeat for a pool in TCP mode |
| `--maxToken` | `` | Auth token (Max) |
| `--maxUid` | `` | User ID (Max) |
| `--debug` | `false` | Verbose logging |
| `--transport` | `yandex` | Transport backend |
| `--mode` | `socks5` | Legacy SOCKS5/IP tunnel or `tcp` byte-stream forwarding |
| `--server` | | Server role for `--mode tcp`; use instead of `--exit-node` |
| `--listen` | `127.0.0.1:15000` | Local listen address for the TCP bridge client |
| `--target` | | Required destination `host:port` for the TCP bridge server |
| `--tcp-timeout` | `30s` | TCP bridge opening, write, peer and acknowledgement timeout |
| `--tcp-max-connections` | `1024` | Maximum simultaneous streams per TCP bridge process |
| `--tcp-window` | `16` | Unacknowledged 1024-byte frames per stream/direction (1–256); same value on both ends |
| `--yandex-batch` | `6` | Messages per Yandex TCP cursor event (1–64), with a 1ms flush timer; 1 disables batching |

## Implementing custom transports

You are free to implement the `Transport` interface from `transport/transport.go` and register your custom transport in main.go switch block.

## License

This project is licensed under the **GNU General Public License v3.0 or later**.
See [LICENSE](LICENSE) for the full text.

Third-party licenses are listed in [NOTICE](NOTICE).

## Disclaimer

Educational use only. Test on your own machines and networks.
