# OpenFlux - Universal Bypass Tool

Network stack research tool. TCP tunnel with pluggable transports.

```
Client (SOCKS5) --> Transport --> Exit Node --> Internet
```

## What it does

Sends TCP packets through Yandex Docs cursor messages (or webRTC datachannel if MAX transport selected). Client side runs a SOCKS5 proxy, exit node decapsulates and forwards to real internet.

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

## Build

```bash
go mod tidy
go build -o universal-bypass-tool .
```

## Build for Android
```bash
export ANDROID_NDK_HOME=<YOUR ANDROID NDK PATH>
./build_android.sh
```

## Build for iOS
```bash
XCODE_PATH="/Applications/Xcode.app"  # or Xcode-beta path
SDK_PATH="$XCODE_PATH/Contents/Developer/Platforms/iPhoneOS.platform/Developer/SDKs/iPhoneOS.sdk"
./build_ios.sh
```

## Usage

Exit node (needs root):

Please use the old document editor. At the moment, the application crashes if you use the new one. I will fix this problem as soon as possible.

```bash
sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -j DROP
sudo ./universal-bypass-tool --exit-node --url "YOUR_YANDEX_DOC_URL" --debug
```

Client:
```bash
./universal-bypass-tool --client --url "YOUR_YANDEX_DOC_URL" --socks5 :1080 --debug
```

Then point your browser to SOCKS5 proxy at localhost:1080.

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

The backend still targets the old Yandex editor described above. It waits for
Engine.IO, Socket.IO and document authentication before reporting connected.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--client` | | Run as client |
| `--exit-node` | | Run as exit node |
| `--socks5` | `:1080` | SOCKS5 listen addr |
| `--url` | `https://localhost` | Document URL (Yandex Docs) |
| `--maxToken` | `` | Token (Max) |
| `--maxUid` | `` | User ID (Max) |
| `--debug` | `false` | Verbose logging |
| `--transport` | `yandex` | Transport backend |
| `--mode` | `socks5` | Legacy SOCKS5/IP tunnel or `tcp` byte-stream forwarding |
| `--server` | | Server role for `--mode tcp`; use instead of `--exit-node` |
| `--listen` | `127.0.0.1:15000` | Local listen address for the TCP bridge client |
| `--target` | | Required destination `host:port` for the TCP bridge server |
| `--tcp-timeout` | `30s` | TCP bridge opening, write, peer and acknowledgement timeout |
| `--tcp-max-connections` | `1024` | Maximum simultaneous streams per TCP bridge process |

## Adding new transports

Implement the `Transport` interface from `transport/transport.go`, add your package, register in main.go switch.

## Testing

See [TESTING.md](TESTING.md) for local checks, live document tests, the
12-connection speed test and recorded measurements on the `main-test` branch.

## License

Educational use only. Test on your own machines and networks.
