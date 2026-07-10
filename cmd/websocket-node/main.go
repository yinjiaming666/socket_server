package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"socket_server/internal/config"
	"socket_server/internal/gateway"
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
	defer node.Stop(context.Background())

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
	mux := websocket.NewMux(handler)

	addr := ":" + strconv.Itoa(nodeConfig.Port)
	log.Printf("websocket node %s redis=%s storage=%s", config.EndpointID, cfg.Redis.Addr, cfg.Storage)
	log.Fatal(appruntime.ServeHTTP(appruntime.HTTPServerConfig{Addr: addr, Handler: mux, Name: "websocket-node " + config.EndpointID}))
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
