package websocket

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"socket_server/internal/domain"
	"socket_server/internal/redisclient"
)

// StartRedisLiveConsumers 启动 Redis 直播间广播消费。
func (n *Node) StartRedisLiveConsumers(ctx context.Context, client *redisclient.Client) {
	if client == nil {
		return
	}
	patterns := []string{"live:room:*", "stream:live:room:*"}
	for _, pattern := range patterns {
		currentPattern := pattern
		go func() {
			if err := client.SubscribePattern(ctx, currentPattern, func(channel string, payload []byte) {
				roomID := roomIDFromRedisChannel(channel)
				if roomID == "" {
					return
				}
				if len(n.roomHub.ListConns(roomID)) == 0 {
					return
				}
				broadcastPayload := payload
				var msg domain.Message
				if err := json.Unmarshal(payload, &msg); err == nil && msg.Payload != nil {
					broadcastPayload = payload
				}
				count, err := n.BroadcastRoom(ctx, roomID, broadcastPayload)
				if err != nil {
					log.Printf("redis live broadcast error: %v", err)
					return
				}
				if count > 0 {
					log.Printf("redis live broadcast room=%s count=%d", roomID, count)
				}
			}); err != nil && ctx.Err() == nil {
				log.Printf("redis live consumer stopped pattern=%s error=%v", currentPattern, err)
			}
		}()
	}
}

// roomIDFromRedisChannel 从频道名解析 room_id。
func roomIDFromRedisChannel(channel string) string {
	if strings.HasPrefix(channel, "stream:live:room:") {
		return strings.TrimPrefix(channel, "stream:live:room:")
	}
	if strings.HasPrefix(channel, "live:room:") {
		suffix := strings.TrimPrefix(channel, "live:room:")
		suffix = strings.TrimSuffix(suffix, ":nodes")
		return suffix
	}
	return ""
}
