package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/yinjiaming666/wskit"

	"socket_server/internal/config"
	"socket_server/internal/gateway"
	"socket_server/internal/host"
	"socket_server/internal/websocket"
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
	nodeConfig, err := config.SelectEndpoint("websocket-node", cfg.Nodes, cfg.FindNode)
	if err != nil {
		return err
	}

	backend := host.NewBackend(cfg.Redis)
	defer backend.Close()
	node, err := wskit.CreateNode(wskit.NodeConfig{
		EndpointID:        nodeConfig.EndpointID,
		WSPath:            nodeConfig.WSPath,
		WSAddr:            nodeConfig.WSAddr,
		GRPCAddr:          nodeConfig.GRPCAddr,
		MaxConn:           nodeConfig.MaxConn,
		SendBuffer:        nodeConfig.SendBuffer,
		HeartbeatInterval: nodeConfig.NodeHeartbeatInterval(),
		OnMessage:         websocket.HandleClientFrame,
	}, backend)
	if err != nil {
		return err
	}
	tokenManager := gateway.NewRouteTokenManager(cfg.RouteToken.Secret, cfg.RouteTokenTTL())
	mux := websocket.NewMux(node, tokenManager, nodeConfig.EndpointID, backend.Ready)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	drainSignals := make(chan os.Signal, 1)
	signal.Notify(drainSignals, syscall.SIGUSR1)
	defer signal.Stop(drainSignals)
	var workers sync.WaitGroup
	workers.Go(func() { watchDrainSignal(ctx, node, drainSignals) })
	var nodeErr error
	workers.Go(func() {
		nodeErr = node.Run(ctx)
		stop()
	})

	addr := ":" + strconv.Itoa(nodeConfig.Port)
	log.Printf("websocket node %s listening on %s", nodeConfig.EndpointID, addr)
	httpErr := host.ServeHTTP(ctx, addr, mux)
	stop()
	workers.Wait()
	return errors.Join(httpErr, nodeErr)
}

func watchDrainSignal(ctx context.Context, node *wskit.Node, signals <-chan os.Signal) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-signals:
			drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			if err := node.BeginDrain(drainCtx); err != nil && ctx.Err() == nil {
				log.Printf("websocket node drain error: %v", err)
			}
			cancel()
		}
	}
}
