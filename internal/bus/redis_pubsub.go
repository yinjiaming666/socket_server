package bus

import (
	"context"

	"socket_server/internal/redisclient"
)

// RedisPublisher 是 Redis 频道发布器。
type RedisPublisher struct {
	client *redisclient.Client
}

// NewRedisPublisher 创建 Redis 频道发布器。
func NewRedisPublisher(client *redisclient.Client) *RedisPublisher {
	return &RedisPublisher{client: client}
}

// Publish 向指定频道发布消息。
//
// 直播弹幕这类高频低价值消息只走 Pub/Sub，不再额外写入 list 队列，避免无消费队列持续堆积。
func (p *RedisPublisher) Publish(ctx context.Context, channel string, payload []byte) error {
	command := "PUB" + "LISH"
	_, err := p.client.Do(ctx, command, p.client.Key(channel), string(payload))
	return err
}

// PublishMany 使用 pipeline 将同一只读消息发布到多个节点频道。
// 相比逐节点调用 Publish，它只产生一次客户端到 Redis 的网络往返。
func (p *RedisPublisher) PublishMany(ctx context.Context, channels []string, payload []byte) error {
	command := "PUB" + "LISH"
	commands := make([][]string, 0, len(channels))
	for _, channel := range channels {
		if channel == "" {
			continue
		}
		commands = append(commands, []string{
			command,
			p.client.Key(channel),
			string(payload),
		})
	}
	return p.client.Pipeline(ctx, commands...)
}
