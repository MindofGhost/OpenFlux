package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	_ "github.com/wlynxg/anet"
	"universal-bypass-tool/socks5"
	"universal-bypass-tool/tcpbridge"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/oneme"
	"universal-bypass-tool/transport/yandex"
	"universal-bypass-tool/tunnel"
	"universal-bypass-tool/utils"
)

var (
	maxToken string
	maxUid   string
)

func main() {
	//os.Setenv("GODEBUG", "netdns=go")
	fmt.Print("written by p1neappleXpress\n")

	exitNode := flag.Bool("exit-node", false, "Run as exit node (needs root)")
	client := flag.Bool("client", false, "Run as client")
	server := flag.Bool("server", false, "Run TCP bridge server (use with --mode tcp)")
	mode := flag.String("mode", "socks5", "Mode: socks5 (legacy IP tunnel) or tcp (byte-stream forwarding)")
	listenAddr := flag.String("listen", "127.0.0.1:15000", "Local TCP bridge listen address")
	targetAddr := flag.String("target", "", "TCP bridge server destination, e.g. 127.0.0.1:443")
	bridgeTimeout := flag.Duration("tcp-timeout", 30*time.Second, "TCP bridge open, write, peer and acknowledgement timeout")
	maxConnections := flag.Int("tcp-max-connections", 1024, "Maximum simultaneous TCP bridge connections")
	windowSize := flag.Int("tcp-window", tcpbridge.DefaultWindowSize, "TCP window in 1024-byte frames (1-256); use the same value at both ends")
	batchSize := flag.Int("yandex-batch", yandex.DefaultBatchSize, "Maximum messages per Yandex TCP batch (1-64); 1 disables batching")
	debug := flag.Bool("debug", false, "Enable verbose debug logging")
	socksAddr := flag.String("socks5", ":1080", "SOCKS5 address")
	transportType := flag.String("transport", "yandex", "Transport type (yandex, oneme)")
	var docURLs documentURLs
	flag.Var(&docURLs, "url", "Yandex document URL; repeat for a document pool in TCP mode")
	flag.StringVar(&maxToken, "maxToken", "", "MAX call user id. If u use MAX transport")
	flag.StringVar(&maxUid, "maxUid", "", "MAX Web token. If u use MAX transport")
	flag.Parse()

	if err := validateMode(*mode, *client, *server, *exitNode, *targetAddr); err != nil {
		log.Fatal(err)
	}

	if err := validateDocumentPool(*transportType, *mode, docURLs); err != nil {
		log.Fatal(err)
	}
	if *mode == "tcp" && (*windowSize < 1 || *windowSize > tcpbridge.MaxWindowSize) {
		log.Fatalf("--tcp-window must be between 1 and %d", tcpbridge.MaxWindowSize)
	}

	if *debug {
		utils.EnableDebug()
	}

	log.Printf("=== Universal Bypass Tool ===")
	log.Printf("Mode: %s, server=%t", *mode, *exitNode || *server)
	log.Printf("Transport: %s", *transportType)

	config := transport.DefaultConfig()
	var transports []transport.Transport

	switch *transportType {
	case "yandex":
		batchLimit := 1
		if *mode == "tcp" {
			batchLimit = *batchSize
		}
		for _, docURL := range docURLs {
			transports = append(transports, transport.NewUncompressedTransport(yandex.NewYandexDocsTransportWithBatch(docURL, config, batchLimit)))
		}
	case "oneme":
		uidint, _ := strconv.ParseInt(maxUid, 10, 64)
		transports = append(transports, transport.NewCompressedTransport(oneme.NewOneMeTransport(*exitNode || *server, maxToken, uidint, config)))
	default:
		log.Fatalf("Unknown transport type: %s", *transportType)
	}

	if *mode == "tcp" {
		if err := runTCPBridge(transports, *listenAddr, tcpbridge.Config{
			Target: *targetAddr, Timeout: *bridgeTimeout, MaxConnections: *maxConnections,
			WindowSize: *windowSize,
		}); err != nil {
			log.Fatal(err)
		}
		return
	}

	trans := transports[0]
	if err := trans.Start(); err != nil {
		log.Fatalf("Failed to start transport: %v", err)
	}

	tun := tunnel.NewTCPTunnel(trans, *exitNode)

	if *exitNode {
		log.Printf("Running as EXIT NODE (needs root for raw socket)")
		log.Printf("! Run: sudo iptables -A OUTPUT -p tcp --tcp-flags RST RST -j DROP")
		select {}
	} else {
		log.Printf("Running as CLIENT (SOCKS5 on %s)", *socksAddr)
		socks5Server := socks5.NewSOCKS5Server(*socksAddr, tun)
		log.Fatal(socks5Server.Start())
	}
}

func validateMode(mode string, client, server, exitNode bool, target string) error {
	switch mode {
	case "socks5":
		if server || client == exitNode || target != "" {
			return fmt.Errorf("socks5 mode requires exactly one of --client or --exit-node; --server/--target require --mode tcp")
		}
	case "tcp":
		if exitNode || client == server {
			return fmt.Errorf("tcp mode requires exactly one of --client or --server; do not use --exit-node")
		}
		if server && target == "" {
			return fmt.Errorf("tcp server requires --target host:port")
		}
		if client && target != "" {
			return fmt.Errorf("--target is configured on the TCP bridge server only")
		}
	default:
		return fmt.Errorf("unknown mode %q (use socks5 or tcp)", mode)
	}
	return nil
}

func runTCPBridge(transports []transport.Transport, listen string, config tcpbridge.Config) error {
	bridge, err := tcpbridge.NewPool(transports, config)
	if err != nil {
		return err
	}
	defer bridge.Close()
	var listener net.Listener
	if config.Target == "" {
		listener, err = net.Listen("tcp", listen)
		if err != nil {
			return err
		}
		defer listener.Close()
	}
	for i, trans := range transports {
		if err := trans.Start(); err != nil {
			_ = trans.Stop()
			return fmt.Errorf("start transport %d: %w", i+1, err)
		}
		defer trans.Stop()
	}
	log.Printf("TCP bridge document/transport sessions: %d", len(transports))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if listener == nil {
		log.Printf("TCP bridge server -> %s", config.Target)
		<-ctx.Done()
		return bridge.Close()
	}
	log.Printf("TCP bridge listening on %s", listener.Addr())
	errors := make(chan error, 1)
	go func() { errors <- bridge.Serve(listener) }()
	select {
	case err := <-errors:
		return err
	case <-ctx.Done():
		return bridge.Close()
	}
}
