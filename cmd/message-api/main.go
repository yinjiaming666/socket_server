package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"socket_server/internal/ack"
	"socket_server/internal/bus"
	"socket_server/internal/config"
	"socket_server/internal/liveroute"
	"socket_server/internal/message"
	"socket_server/internal/push"
	appruntime "socket_server/internal/runtime"
	"socket_server/internal/sender"
)

// main 启动 Message API 服务。
func main() {
	cfg, err := config.Load("")
	if err != nil {
		log.Fatal(err)
	}

	messageAPIConfig, err := selectMessageAPIConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}

	stores := appruntime.NewStores(cfg)
	defer stores.Close()

	ack.StartTimeoutWorker(context.Background(), stores.AckStore, ack.TimeoutWorkerConfig{Timeout: 30 * time.Second, Interval: 5 * time.Second, BatchSize: 100})

	pubSubClient := sender.PubSubClient(bus.NewMemoryPubSub())
	streamClient := sender.StreamClient(bus.NewMemoryStream())
	var roomNodes sender.RoomNodeResolver
	if appruntime.UseRedis(cfg) {
		// Message API 与 WebSocket Node 共用直播间节点租约，避免向无关节点广播。
		liveRegistry := liveroute.NewRedisRegistry(stores.Redis)
		roomNodes = liveRegistry
		pubSubClient = bus.NewRedisPublisher(stores.Redis)
		streamClient = bus.NewRoutedRedisStreamWriter(stores.Redis, liveRegistry)
	}

	nodeRegistry := push.NewRegistry()
	nodeClient := senderNodeClient(push.NewInMemoryClient(nodeRegistry))
	if appruntime.UseRedis(cfg) {
		nodeClient = push.NewGRPCClient(stores.EndpointRepo)
	}
	defer closeNodeClient(nodeClient)

	directSender := sender.NewGRPCSender(stores.RouteResolver, nodeClient)
	roomSender := sender.NewRedisPubSubSender(pubSubClient)
	if roomNodes != nil {
		roomSender = sender.NewRoutedRedisPubSubSender(pubSubClient, roomNodes)
	}
	importantRoomSender := sender.NewRedisStreamSender(streamClient)
	selector := sender.NewDefaultSenderSelector(directSender, roomSender, importantRoomSender, nil)

	service := message.NewService(message.NewEmptyStore(), stores.RouteResolver, stores.AckStore, selector)
	handler := message.NewHandler(service)
	// readiness 同时验证 Redis 共享状态，避免发布门禁只看到进程存活。
	handler.SetReadinessCheck(stores.Ready)
	mux := message.NewMux(handler)

	addr := ":" + strconv.Itoa(messageAPIConfig.Port)
	log.Printf("message api %s redis=%s storage=%s", messageAPIConfig.EndpointID, cfg.Redis.Addr, cfg.Storage)
	if err := appruntime.ServeHTTP(appruntime.HTTPServerConfig{Addr: addr, Handler: mux, Name: "message-api " + messageAPIConfig.EndpointID}); err != nil {
		log.Fatal(err)
	}
}

// senderNodeClient 保留类型推断清晰度。
func senderNodeClient(client push.Client) push.Client {
	return client
}

// closeNodeClient 尝试关闭节点客户端。
func closeNodeClient(client push.Client) {
	closer, ok := client.(interface{ Close() error })
	if ok {
		_ = closer.Close()
	}
}

// selectMessageAPIConfig 根据 ENDPOINT_ID 选择 Message API 实例配置。
func selectMessageAPIConfig(cfg *config.AppConfig) (*config.MessageAPIConfig, error) {
	endpointID := os.Getenv("ENDPOINT_ID")
	if endpointID == "" {
		messageAPIConfig, ok := cfg.DefaultMessageAPI()
		if !ok {
			return nil, os.ErrNotExist
		}
		return messageAPIConfig, nil
	}

	messageAPIConfig, ok := cfg.FindMessageAPI(endpointID)
	if !ok {
		return nil, os.ErrNotExist
	}
	return messageAPIConfig, nil
}
