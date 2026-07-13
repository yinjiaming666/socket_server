package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"socket_server/internal/config"
	"socket_server/internal/gateway"
	"socket_server/internal/liveroute"
	appruntime "socket_server/internal/runtime"
	"socket_server/internal/websocket"
)

// main 启动 WebSocket Node 服务。
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load("")
	if err != nil {
		log.Fatal(err)
	}

	nodeConfig, err := resolveNodeConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	stores := appruntime.NewStores(cfg)
	defer stores.Close()

	tokenManager := gateway.NewRouteTokenManager(cfg.RouteToken.Secret, cfg.RouteTokenTTL())

	config := websocket.NodeConfig{
		EndpointID:        nodeConfig.EndpointID,
		WSPath:            nodeConfig.WSPath,
		WSAddr:            nodeConfig.WSAddr,
		GRPCAddr:          nodeConfig.GRPCAddr,
		MaxConn:           nodeConfig.MaxConn,
		SendBuffer:        nodeConfig.SendBuffer,
		HeartbeatInterval: nodeConfig.NodeHeartbeatInterval(),
	}

	node := websocket.NewNode(config, stores.EndpointRepo, stores.RouteResolver, stores.AckStore, tokenManager)
	if appruntime.UseRedis(cfg) {
		// Redis 模式注册直播房间节点短租约，使消息只扇出到实际承载房间的节点。
		node.SetLiveRoomRegistry(liveroute.NewRedisRegistry(stores.Redis))
	}

	// SIGUSR1 仅能由主机进程权限触发，用于滚动发布前安全进入 draining。
	drainSignals := make(chan os.Signal, 1)
	signal.Notify(drainSignals, syscall.SIGUSR1)
	defer signal.Stop(drainSignals)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-drainSignals:
				drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := node.BeginDrain(drainCtx); err != nil {
					log.Printf("websocket node drain error: %v", err)
				}
				cancel()
			}
		}
	}()

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := node.Stop(stopCtx); err != nil {
			log.Printf("websocket node stop error: %v", err)
		}
	}()

	if err := node.Start(ctx); err != nil {
		log.Fatal(err)
	}
	if err := node.StartGRPCServer(ctx); err != nil {
		log.Fatal(err)
	}
	if appruntime.UseRedis(cfg) {
		node.StartRedisLiveConsumers(ctx, stores.Redis)
	}

	handler := websocket.NewHandler(node)
	// readiness 同时验证节点排空状态和 Redis 共享状态。
	handler.SetReadinessCheck(stores.Ready)
	mux := websocket.NewMux(handler)

	addr := ":" + strconv.Itoa(nodeConfig.Port)
	log.Printf("websocket node %s redis=%s storage=%s", config.EndpointID, cfg.Redis.Addr, cfg.Storage)
	if err := appruntime.ServeHTTP(appruntime.HTTPServerConfig{Addr: addr, Handler: mux, Name: "websocket-node " + config.EndpointID}); err != nil {
		log.Fatal(err)
	}
}

// resolveNodeConfig 根据 ENDPOINT_ID 环境变量选择节点配置。
func resolveNodeConfig(cfg *config.AppConfig) (*config.WebSocketNodeConfig, error) {
	endpointID := os.Getenv("ENDPOINT_ID")
	if endpointID == "" && len(cfg.Nodes) > 0 {
		endpointID = cfg.Nodes[0].EndpointID
	}

	nodeConfig, ok := cfg.FindNode(endpointID)
	if !ok {
		return nil, os.ErrNotExist
	}
	return nodeConfig, nil
}
