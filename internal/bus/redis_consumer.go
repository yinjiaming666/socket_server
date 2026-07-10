package bus

import (
	"context"

	"socket_server/internal/redisclient"
)

// RedisMessageHandler 表示 Redis 消息处理函数。
type RedisMessageHandler func(channel string, payload []byte)

// RedisConsumer 是基于 Redis Pub/Sub pattern 的消费器。
type RedisConsumer struct {
	client *redisclient.Client
}

// NewRedisConsumer 创建 Redis 消费器。
func NewRedisConsumer(client *redisclient.Client) *RedisConsumer {
	return &RedisConsumer{client: client}
}

// SubscribePattern 订阅频道模式。
func (c *RedisConsumer) SubscribePattern(ctx context.Context, pattern string, handler RedisMessageHandler) error {
	return c.client.SubscribePattern(ctx, pattern, handler)
}
