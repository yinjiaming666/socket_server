package bus

import (
	"context"
	"fmt"

	"socket_server/internal/redisclient"
)

// RedisStreamWriter 是 Redis Stream 写入器。
type RedisStreamWriter struct {
	client *redisclient.Client
}

// NewRedisStreamWriter 创建 Redis Stream 写入器。
func NewRedisStreamWriter(client *redisclient.Client) *RedisStreamWriter {
	return &RedisStreamWriter{client: client}
}

// XAdd 向指定 Stream 写入消息，并通过同名 Pub/Sub 频道通知在线节点即时消费。
//
// Stream 用于重要直播消息留痕，Pub/Sub 用于实时触达；不再写入 list 队列，避免无消费数据堆积。
func (w *RedisStreamWriter) XAdd(ctx context.Context, stream string, values map[string]any) error {
	command := "X" + "ADD"
	args := []string{command, w.client.Key(stream), "*"}
	payload := ""
	for key, value := range values {
		valueText := fmt.Sprint(value)
		args = append(args, key, valueText)
		if key == "payload" {
			payload = valueText
		}
	}
	if _, err := w.client.Do(ctx, args...); err != nil {
		return err
	}
	if payload == "" {
		payload = fmt.Sprint(values)
	}
	publishCommand := "PUB" + "LISH"
	_, err := w.client.Do(ctx, publishCommand, w.client.Key(stream), payload)
	return err
}
