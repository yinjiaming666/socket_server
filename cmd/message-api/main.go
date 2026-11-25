package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/yinjiaming666/wskit"

	"socket_server/internal/config"
	"socket_server/internal/host"
	"socket_server/internal/message"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := config.Load("")
	if err != nil {
		return err
	}
	messageAPIConfig, err := config.SelectEndpoint("message-api", cfg.MessageAPIs, cfg.FindMessageAPI)
	if err != nil {
		return err
	}

	backend := host.NewBackend(cfg.Redis)
	defer backend.Close()
	sender, err := wskit.NewSender(backend)
	if err != nil {
		return err
	}
	defer sender.Close()

	notices := message.NewNoticeStore(cfg.Redis)
	defer notices.Close()
	service := message.NewService(sender, notices)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var workers sync.WaitGroup
	workers.Go(func() { sender.RunAckTimeouts(ctx) })

	addr := ":" + strconv.Itoa(messageAPIConfig.Port)
	log.Printf("message api %s listening on %s", messageAPIConfig.EndpointID, addr)
	err = host.ServeHTTP(ctx, addr, message.NewMux(service, backend.Ready))
	stop()
	workers.Wait()
	return err
}
