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

### Stream behavior and limits

- Opening, data, half-close, reset, acknowledgements and heartbeat messages are
  framed separately. A FIN closes only the receiving TCP socket's write side,
  allowing the application to finish sending its response.
- Data is split into 1024-byte chunks. A stream can have at most 16 unacknowledged
  chunks; acknowledgements follow writes to the receiving TCP socket. A bounded
  reorder buffer restores order and discards duplicate sequence numbers.
- Lost messages are **not retransmitted** in this first version. Missing
  acknowledgements or peer heartbeats close the affected stream after the
  timeout, rather than delivering bytes after a gap. Queue overflow and socket
  errors also close the stream.
- Yandex transport disconnects invalidate active streams, including brief
  disconnect/reconnect cycles. Old outbound queues are discarded. Applications
  must open new TCP connections after the backend reconnects; existing TLS
  sessions are not resumed by the bridge.
- `--tcp-timeout` defaults to `30s` (minimum `1s`) for opening, socket writes,
  peer liveness and acknowledgements; target dialing uses half that duration.
  `--tcp-max-connections` defaults to `1024` per bridge process, including
  connections still opening. Clients accepted while the backend is disconnected
  are closed; an absent/full bridge server results in an opening timeout.
- All streams share the backend's bandwidth and outer TCP connection. This
  mode does not provide UDP-like latency or independent loss recovery per stream.

Both ends should run the updated build: the transport now wraps messages with
LZ4 compression (or a marker for uncompressed messages).

The backend still targets the old Yandex editor described above. It waits for
Engine.IO, Socket.IO and document authentication before reporting connected.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--client` | | Run as client |
| `--exit-node` | | Run as exit node |
| `--socks5` | `:1080` | SOCKS5 listen addr |
| `--url` | `https://localhost` | Document URL (Yandex Docs) |
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

## Implementing custom transports

You are free to implement the `Transport` interface from `transport/transport.go` and register your custom transport in main.go switch block.

## Testing

See [TESTING.md](TESTING.md) for local checks, live document tests, the
12-connection speed test and recorded measurements on the `main-test` branch.

## License

This project is licensed under the **GNU General Public License v3.0 or later**.
See [LICENSE](LICENSE) for the full text.

Third-party licenses are listed in [NOTICE](NOTICE).

## Disclaimer

Educational use only. Test on your own machines and networks.
