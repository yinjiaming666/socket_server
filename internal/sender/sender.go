package sender

import (
	"context"
	"errors"

	"socket_server/internal/domain"
	"socket_server/internal/push"
	"socket_server/internal/rediskey"
	"socket_server/internal/route"
)

// MessageSender 定义统一消息发送接口。
type MessageSender interface {
	// Send 发送一条内部消息。
	Send(ctx context.Context, msg *domain.Message) (*domain.SendResult, error)
}

// PubSubClient 定义 Redis Pub/Sub 发送能力接口。
type PubSubClient interface {
	// Publish 向指定频道发布消息。
	Publish(ctx context.Context, channel string, payload []byte) error
}

// BatchPubSubClient 定义把同一消息批量发布到多个频道的可选能力。
type BatchPubSubClient interface {
	// PublishMany 使用一次批处理发布多个节点频道。
	PublishMany(ctx context.Context, channels []string, payload []byte) error
}

// RoomNodeResolver 定义直播间承载节点查询能力。
type RoomNodeResolver interface {
	// ListRoomNodes 返回当前仍持有房间租约的节点编号。
	ListRoomNodes(ctx context.Context, roomID string) ([]string, error)
}

// StreamClient 定义 Redis Stream 写入能力接口。
type StreamClient interface {
	// XAdd 向指定 Stream 写入消息。
	XAdd(ctx context.Context, stream string, values map[string]any) error
}

// SenderSelector 定义消息发送器选择接口。
type SenderSelector interface {
	// Select 根据消息业务类型选择具体发送器。
	Select(ctx context.Context, msg *domain.Message) MessageSender
}

// GRPCSender 使用内部推送客户端向指定 WebSocket Node 精准推送。
type GRPCSender struct {
	// router 表示连接路由解析器。
	router route.Resolver
	// client 表示内部推送客户端。
	client push.Client
}

// NewGRPCSender 创建 gRPC 精准推送发送器。
func NewGRPCSender(router route.Resolver, client push.Client) *GRPCSender {
	return &GRPCSender{router: router, client: client}
}

// groupMessageRoutes 优先复用消息服务已经解析的完整路由，避免发送阶段再次查询 Redis。
// 兼容仅设置 ConnIDs 的旧调用方时，才回退到 Resolver 分组。
func groupMessageRoutes(ctx context.Context, router route.Resolver, msg *domain.Message) (route.EndpointGroup, error) {
	if len(msg.DeliveryRoutes) == 0 {
		if router == nil {
			if len(msg.ConnIDs) == 0 {
				return make(route.EndpointGroup), nil
			}
			return nil, errors.New("route resolver is nil")
		}
		return router.GroupConnsByEndpoint(ctx, msg.ConnIDs)
	}

	groups := make(route.EndpointGroup)
	seen := make(map[string]struct{}, len(msg.DeliveryRoutes))
	for _, info := range msg.DeliveryRoutes {
		if info == nil {
			continue
		}
		if info.ConnID == "" || info.EndpointID == "" {
			return nil, errors.New("delivery route is incomplete")
		}
		if _, ok := seen[info.ConnID]; ok {
			continue
		}
		seen[info.ConnID] = struct{}{}
		groups[info.EndpointID] = append(groups[info.EndpointID], info.ConnID)
	}
	return groups, nil
}

// Send 按 endpoint_id 分组后批量推送连接消息。
func (s *GRPCSender) Send(ctx context.Context, msg *domain.Message) (*domain.SendResult, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}

	groups, err := groupMessageRoutes(ctx, s.router, msg)
	if err != nil {
		return nil, err
	}

	type outcome struct {
		connIDs []string
		resp    *domain.PushResponse
		err     error
	}
	outcomes := make(chan outcome, len(groups))
	sem := make(chan struct{}, 8)
	for endpointID, connIDs := range groups {
		endpointID := endpointID
		connIDs := append([]string(nil), connIDs...)
		go func() {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				outcomes <- outcome{connIDs: connIDs, err: ctx.Err()}
				return
			}
			resp, err := s.client.PushToConns(ctx, endpointID, connIDs, msg)
			outcomes <- outcome{connIDs: connIDs, resp: resp, err: err}
		}()
	}

	result := &domain.SendResult{Accepted: true}
	for range groups {
		current := <-outcomes
		if current.err != nil {
			result.Accepted = false
			result.FailedConnIDs = append(result.FailedConnIDs, current.connIDs...)
			result.Reason = current.err.Error()
			continue
		}
		// 防御异常客户端实现返回 nil 响应，避免聚合阶段触发空指针崩溃。
		if current.resp == nil {
			result.Accepted = false
			result.FailedConnIDs = append(result.FailedConnIDs, current.connIDs...)
			result.Reason = "push client returned nil response"
			continue
		}
		result.AcceptedConnIDs = append(result.AcceptedConnIDs, current.resp.AcceptedConnIDs...)
		result.FailedConnIDs = append(result.FailedConnIDs, current.resp.FailedConnIDs...)
		if !current.resp.Accepted {
			result.Accepted = false
			result.Reason = current.resp.Reason
		}
	}
	return result, nil
}

// RedisPubSubSender 使用 Redis Pub/Sub 发布直播间高频低价值消息。
type RedisPubSubSender struct {
	// client 表示 Pub/Sub 发布客户端。
	client PubSubClient
	// roomNodes 非空时只向实际承载房间的节点频道发布。
	roomNodes RoomNodeResolver
}

// NewRedisPubSubSender 创建兼容全房间频道的 Pub/Sub 发送器。
func NewRedisPubSubSender(client PubSubClient) *RedisPubSubSender {
	return &RedisPubSubSender{client: client}
}

// NewRoutedRedisPubSubSender 创建按房间节点租约精准扇出的 Pub/Sub 发送器。
func NewRoutedRedisPubSubSender(client PubSubClient, roomNodes RoomNodeResolver) *RedisPubSubSender {
	return &RedisPubSubSender{client: client, roomNodes: roomNodes}
}

// Send 将消息发布到直播间 Pub/Sub 频道。
func (s *RedisPubSubSender) Send(ctx context.Context, msg *domain.Message) (*domain.SendResult, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}

	body, err := domain.MarshalClientMessage(msg)
	if err != nil {
		return nil, err
	}

	if s.roomNodes == nil {
		if err := s.client.Publish(ctx, rediskey.LiveRoomChannel(msg.RoomID), body); err != nil {
			return nil, err
		}
		return &domain.SendResult{Accepted: true}, nil
	}

	nodes, err := s.roomNodes.ListRoomNodes(ctx, msg.RoomID)
	if err != nil {
		return nil, err
	}
	channels := make([]string, 0, len(nodes))
	for _, endpointID := range nodes {
		channels = append(channels, rediskey.LiveNodeRoomChannel(endpointID, msg.RoomID))
	}

	// Redis 发布器支持批处理时，一次 pipeline 完成所有目标节点扇出。
	if batchClient, ok := s.client.(BatchPubSubClient); ok {
		if err := batchClient.PublishMany(ctx, channels, body); err != nil {
			return nil, err
		}
		return &domain.SendResult{Accepted: true}, nil
	}
	for _, channel := range channels {
		if err := s.client.Publish(ctx, channel, body); err != nil {
			return nil, err
		}
	}
	return &domain.SendResult{Accepted: true}, nil
}

// RedisStreamSender 使用 Redis Stream 写入直播间重要消息。
type RedisStreamSender struct {
	// client 表示 Stream 写入客户端。
	client StreamClient
}

// NewRedisStreamSender 创建 Redis Stream 发送器。
func NewRedisStreamSender(client StreamClient) *RedisStreamSender {
	return &RedisStreamSender{client: client}
}

// Send 将消息写入直播间 Stream。
func (s *RedisStreamSender) Send(ctx context.Context, msg *domain.Message) (*domain.SendResult, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}

	body, err := domain.MarshalClientMessage(msg)
	if err != nil {
		return nil, err
	}

	values := map[string]any{
		"message_id": msg.MessageID,
		"biz_type":   msg.BizType,
		"payload":    string(body),
		"created_at": msg.CreatedAt,
	}
	if err := s.client.XAdd(ctx, rediskey.LiveRoomStream(msg.RoomID), values); err != nil {
		return nil, err
	}
	return &domain.SendResult{Accepted: true}, nil
}

// KafkaSender 是后续扩展 Kafka 时使用的发送器占位实现。
type KafkaSender struct {
	// producer 表示后续注入的 Kafka 生产者。
	producer MessageSender
}

// NewKafkaSender 创建 Kafka 发送器占位实现。
func NewKafkaSender(producer MessageSender) *KafkaSender {
	return &KafkaSender{producer: producer}
}

// Send 将消息交给 Kafka 生产者占位实现处理。
func (s *KafkaSender) Send(ctx context.Context, msg *domain.Message) (*domain.SendResult, error) {
	if s.producer == nil {
		return nil, errors.New("kafka producer is not configured")
	}
	return s.producer.Send(ctx, msg)
}

// DefaultSenderSelector 是按照 biz_type 选择发送器的默认实现。
type DefaultSenderSelector struct {
	// grpcSender 表示单聊、中小群、指定用户系统通知发送器。
	grpcSender MessageSender
	// redisPubSubSender 表示直播间高频低价值消息发送器。
	redisPubSubSender MessageSender
	// redisStreamSender 表示直播间重要消息发送器。
	redisStreamSender MessageSender
	// kafkaSender 表示后续大规模广播扩展发送器。
	kafkaSender MessageSender
}

// NewDefaultSenderSelector 创建默认消息发送器选择器。
func NewDefaultSenderSelector(grpcSender MessageSender, redisPubSubSender MessageSender, redisStreamSender MessageSender, kafkaSender MessageSender) *DefaultSenderSelector {
	return &DefaultSenderSelector{
		grpcSender:        grpcSender,
		redisPubSubSender: redisPubSubSender,
		redisStreamSender: redisStreamSender,
		kafkaSender:       kafkaSender,
	}
}

// Select 根据消息业务类型选择具体发送器。
func (s *DefaultSenderSelector) Select(ctx context.Context, msg *domain.Message) MessageSender {
	switch msg.BizType {
	case "live_danmu", "live_like", "live_join":
		return s.redisPubSubSender
	case "live_notice", "live_status", "system_broadcast":
		return s.redisStreamSender
	case "live_huge_broadcast":
		if s.kafkaSender != nil {
			return s.kafkaSender
		}
		return s.redisPubSubSender
	default:
		return s.grpcSender
	}
}
