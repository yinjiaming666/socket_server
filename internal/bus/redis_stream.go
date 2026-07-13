package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"socket_server/internal/redisclient"
	"socket_server/internal/rediskey"
)

// StreamRoomNodeResolver 定义重要直播消息所需的房间节点查询能力。
type StreamRoomNodeResolver interface {
	// ListRoomNodes 返回当前仍持有房间租约的节点编号。
	ListRoomNodes(ctx context.Context, roomID string) ([]string, error)
}

// RedisStreamWriter 是 Redis Stream 写入器。
type RedisStreamWriter struct {
	client *redisclient.Client
	// roomNodes 非空时将实时通知精准发布到承载房间的节点。
	roomNodes StreamRoomNodeResolver
}

// NewRedisStreamWriter 创建兼容全房间通知频道的 Redis Stream 写入器。
func NewRedisStreamWriter(client *redisclient.Client) *RedisStreamWriter {
	return &RedisStreamWriter{client: client}
}

// NewRoutedRedisStreamWriter 创建按房间节点租约精准通知的 Redis Stream 写入器。
func NewRoutedRedisStreamWriter(client *redisclient.Client, roomNodes StreamRoomNodeResolver) *RedisStreamWriter {
	return &RedisStreamWriter{client: client, roomNodes: roomNodes}
}

// XAdd 向指定 Stream 写入消息，并通过 Pub/Sub 通知在线节点即时消费。
//
// Stream 用于重要直播消息留痕，Pub/Sub 用于实时触达；配置房间路由后只通知实际承载节点。
func (w *RedisStreamWriter) XAdd(ctx context.Context, stream string, values map[string]any) error {
	command := "X" + "ADD"
	args := []string{command, w.client.Key(stream), "MAXLEN", "~", "10000", "*"}
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
	if w.roomNodes == nil {
		_, err := w.client.Do(ctx, publishCommand, w.client.Key(stream), payload)
		return err
	}

	const liveStreamPrefix = "stream:live:room:"
	if !strings.HasPrefix(stream, liveStreamPrefix) {
		return errors.New("live stream name is invalid")
	}
	roomID := strings.TrimPrefix(stream, liveStreamPrefix)
	if roomID == "" {
		return errors.New("live stream room is empty")
	}
	nodes, err := w.roomNodes.ListRoomNodes(ctx, roomID)
	if err != nil {
		return err
	}
	commands := make([][]string, 0, len(nodes))
	for _, endpointID := range nodes {
		commands = append(commands, []string{
			publishCommand,
			w.client.Key(rediskey.LiveNodeRoomStreamChannel(endpointID, roomID)),
			payload,
		})
	}
	// 节点通知通过 pipeline 批量发布，Stream 持久化仍只执行一次。
	return w.client.Pipeline(ctx, commands...)
}
