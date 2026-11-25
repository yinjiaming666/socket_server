package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/yinjiaming666/wskit"

	"socket_server/internal/config"
	"socket_server/internal/gateway"
	"socket_server/internal/host"
)

// main 启动 Gateway 服务。
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

	gatewayConfig, err := config.SelectEndpoint("gateway", cfg.Gateways, cfg.FindGateway)
	if err != nil {
		return err
	}

	backend := host.NewBackend(cfg.Redis)
	defer backend.Close()
	router, err := wskit.CreateGateway(wskit.GatewayConfig{
		PublicURL:        "wss://" + gatewayConfig.PublicHost,
		HeartbeatTimeout: gatewayConfig.HeartbeatTimeout(),
		LastEndpointTTL:  gatewayConfig.LastEndpointTTL(),
	}, backend)
	if err != nil {
		return err
	}
	tokenManager := gateway.NewRouteTokenManager(cfg.RouteToken.Secret, cfg.RouteTokenTTL())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	addr := ":" + strconv.Itoa(gatewayConfig.Port)
	log.Printf("gateway %s listening on %s", gatewayConfig.EndpointID, addr)
	return host.ServeHTTP(ctx, addr, gateway.NewMux(router, tokenManager, backend.Ready))
}
