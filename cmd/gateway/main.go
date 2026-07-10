package main

import (
	"log"
	"os"
	"strconv"

	"socket_server/internal/config"
	"socket_server/internal/gateway"
	appruntime "socket_server/internal/runtime"
)

// main 启动 Gateway 服务。
func main() {
	cfg, err := config.Load("")
	if err != nil {
		log.Fatal(err)
	}

	gatewayConfig, err := selectGatewayConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	stores := appruntime.NewStores(cfg)
	defer stores.Close()

	selector := gateway.NewDefaultEndpointSelector(stores.EndpointRepo, stores.RouteResolver, gatewayConfig.HeartbeatTimeout(), gatewayConfig.LastEndpointTTL())
	tokenManager := gateway.NewRouteTokenManager(cfg.RouteToken.Secret, cfg.RouteTokenTTL())
	handler := gateway.NewHandler(selector, tokenManager, gatewayConfig.PublicHost, int64(cfg.RouteToken.TTLSeconds))

	mux := gateway.NewMux(handler)

	addr := ":" + strconv.Itoa(gatewayConfig.Port)
	log.Printf("gateway %s redis=%s storage=%s", gatewayConfig.EndpointID, cfg.Redis.Addr, cfg.Storage)
	log.Fatal(appruntime.ServeHTTP(appruntime.HTTPServerConfig{Addr: addr, Handler: mux, Name: "gateway " + gatewayConfig.EndpointID}))
}

// selectGatewayConfig 根据 ENDPOINT_ID 选择 Gateway 实例配置。
func selectGatewayConfig(cfg *config.AppConfig) (*config.GatewayConfig, error) {
	endpointID := os.Getenv("ENDPOINT_ID")
	if endpointID == "" {
		gatewayConfig, ok := cfg.DefaultGateway()
		if !ok {
			return nil, os.ErrNotExist
		}
		return gatewayConfig, nil
	}

	gatewayConfig, ok := cfg.FindGateway(endpointID)
	if !ok {
		return nil, os.ErrNotExist
	}
	return gatewayConfig, nil
}
