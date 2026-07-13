package websocket

import (
	"context"
	"log"
	"strings"
	"time"

	"socket_server/internal/redisclient"
	"socket_server/internal/rediskey"
)

// StartRedisLiveConsumers 启动 Redis 直播间广播消费，并在订阅中断后自动重连。
func (n *Node) StartRedisLiveConsumers(ctx context.Context, client *redisclient.Client) {
	if client == nil {
		return
	}
	patterns := []string{
		rediskey.LiveNodeRoomChannelPattern(n.config.EndpointID),
		rediskey.LiveNodeRoomStreamChannelPattern(n.config.EndpointID),
	}
	for _, pattern := range patterns {
		go n.runRedisPatternConsumer(ctx, client, pattern)
	}
}

func (n *Node) runRedisPatternConsumer(ctx context.Context, client *redisclient.Client, pattern string) {
	backoff := time.Second
	for {
		err := client.SubscribePattern(ctx, pattern, func(channel string, payload []byte) {
			roomID := roomIDFromRedisChannel(channel, n.config.EndpointID)
			if roomID == "" || len(n.roomHub.ListConns(roomID)) == 0 {
				return
			}
			if _, err := n.BroadcastRoom(ctx, roomID, payload); err != nil && ctx.Err() == nil {
				log.Printf("redis live broadcast error pattern=%s room=%s error=%v", pattern, roomID, err)
			}
		})
		if ctx.Err() != nil {
			return
		}
		log.Printf("redis live consumer reconnect pattern=%s error=%v backoff=%s", pattern, err, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

// roomIDFromRedisChannel 从频道名解析 room_id，并校验消息确实发往当前节点。
func roomIDFromRedisChannel(channel string, endpointID string) string {
	targetPrefixes := []string{
		strings.TrimSuffix(rediskey.LiveNodeRoomChannelPattern(endpointID), "*"),
		strings.TrimSuffix(rediskey.LiveNodeRoomStreamChannelPattern(endpointID), "*"),
	}
	for _, prefix := range targetPrefixes {
		if strings.HasPrefix(channel, prefix) {
			return strings.TrimPrefix(channel, prefix)
		}
	}

	// 保留旧频道解析仅用于平滑兼容；新消费者不会再订阅全房间模式。
	if strings.HasPrefix(channel, "stream:live:room:") {
		return strings.TrimPrefix(channel, "stream:live:room:")
	}
	if strings.HasPrefix(channel, "live:room:") {
		suffix := strings.TrimPrefix(channel, "live:room:")
		return strings.TrimSuffix(suffix, ":nodes")
	}
	return ""
}
