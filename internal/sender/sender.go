package sender

import (
	"context"
	"encoding/json"
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

// Send 按 endpoint_id 分组后批量推送连接消息。
func (s *GRPCSender) Send(ctx context.Context, msg *domain.Message) (*domain.SendResult, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}

	groups, err := s.router.GroupConnsByEndpoint(ctx, msg.ConnIDs)
	if err != nil {
		return nil, err
	}

	result := &domain.SendResult{Accepted: true}
	for endpointID, connIDs := range groups {
		resp, err := s.client.PushToConns(ctx, endpointID, connIDs, msg)
		if err != nil {
			result.Accepted = false
			result.FailedConnIDs = append(result.FailedConnIDs, connIDs...)
			result.Reason = err.Error()
			continue
		}
		result.AcceptedConnIDs = append(result.AcceptedConnIDs, resp.AcceptedConnIDs...)
		result.FailedConnIDs = append(result.FailedConnIDs, resp.FailedConnIDs...)
		if !resp.Accepted {
			result.Accepted = false
			result.Reason = resp.Reason
		}
	}
	return result, nil
}

// RedisPubSubSender 使用 Redis Pub/Sub 发布直播间高频低价值消息。
type RedisPubSubSender struct {
	// client 表示 Pub/Sub 发布客户端。
	client PubSubClient
}

// NewRedisPubSubSender 创建 Redis Pub/Sub 发送器。
func NewRedisPubSubSender(client PubSubClient) *RedisPubSubSender {
	return &RedisPubSubSender{client: client}
}

// Send 将消息发布到直播间 Pub/Sub 频道。
func (s *RedisPubSubSender) Send(ctx context.Context, msg *domain.Message) (*domain.SendResult, error) {
	if msg == nil {
		return nil, errors.New("message is nil")
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	if err := s.client.Publish(ctx, rediskey.LiveRoomChannel(msg.RoomID), body); err != nil {
		return nil, err
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

	body, err := json.Marshal(msg)
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
