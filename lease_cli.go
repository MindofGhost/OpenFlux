package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"universal-bypass-tool/lease"
	"universal-bypass-tool/tcpbridge"
	"universal-bypass-tool/transport"
	"universal-bypass-tool/transport/yandex"
)

func runLeasedTCPBridge(config lease.Config, listen string, batch int, bridgeConfig tcpbridge.Config) error {
	r, err := lease.New(config, bridgeConfig, func(url string) (transport.Transport, error) {
		return transport.NewUncompressedTransport(yandex.NewYandexDocsTransportWithBatch(url, transport.DefaultConfig(), batch)), nil
	})
	if err != nil {
		return err
	}
	defer r.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var listener net.Listener
	if bridgeConfig.Target == "" {
		listener, err = net.Listen("tcp", listen)
		if err != nil {
			return err
		}
		defer listener.Close()
		log.Printf("TCP bridge with leases listening on %s", listener.Addr())
	} else {
		log.Printf("TCP lease server -> %s; bootstrap documents=%d, lease documents=%d", bridgeConfig.Target, len(config.Bootstrap), len(config.Pool))
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var serve <-chan error
	if listener != nil {
		ch := make(chan error, 1)
		serve = ch
		go func() { ch <- r.Bridge().Serve(listener) }()
	}
	select {
	case err = <-done:
		stop()
	case err = <-serve:
		stop()
		<-done
	case <-ctx.Done():
		err = <-done
	}
	return err
}
