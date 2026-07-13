package rediskey

import "fmt"

// EndpointsKey 返回 WebSocket endpoint 列表 Key。
func EndpointsKey() string {
	return "ws:endpoints"
}

// UserConnsKey 返回用户多端连接集合 Key。
func UserConnsKey(userID string) string {
	return fmt.Sprintf("ws:user:%s:conns", userID)
}

// ConnKey 返回单个连接详情 Key。
func ConnKey(connID string) string {
	return fmt.Sprintf("ws:conn:%s", connID)
}

// EndpointKey 返回 endpoint 详情 Key。
func EndpointKey(endpointID string) string {
	return fmt.Sprintf("ws:endpoint:%s", endpointID)
}

// EndpointConnsKey 返回节点连接索引 Key。
func EndpointConnsKey(endpointID string) string {
	return fmt.Sprintf("ws:endpoint:%s:conns", endpointID)
}

// LastEndpointKey 返回用户最近连接节点 Key。
func LastEndpointKey(userID string) string {
	return fmt.Sprintf("route:last_endpoint:%s", userID)
}

// AckPendingKey 返回消息待 ACK 连接集合 Key。
func AckPendingKey(messageID string) string {
	return fmt.Sprintf("ack:msg:%s:pending", messageID)
}

// AckDoneKey 返回消息已 ACK 连接集合 Key。
func AckDoneKey(messageID string) string {
	return fmt.Sprintf("ack:msg:%s:done", messageID)
}

// AckFailedKey 返回消息失败 ACK 连接集合 Key。
func AckFailedKey(messageID string) string {
	return fmt.Sprintf("ack:msg:%s:failed", messageID)
}

// LiveRoomChannel 返回直播间 Redis Pub/Sub 频道。
func LiveRoomChannel(roomID string) string {
	return fmt.Sprintf("live:room:%s", roomID)
}

// LiveRoomStream 返回直播间 Redis Stream Key。
func LiveRoomStream(roomID string) string {
	return fmt.Sprintf("stream:live:room:%s", roomID)
}

// LiveRoomNodesKey 返回直播间承载节点租约有序集合 Key。
func LiveRoomNodesKey(roomID string) string {
	return fmt.Sprintf("live:room:%s:nodes", roomID)
}

// LiveNodeRoomsKey 返回节点承载直播间反向索引集合 Key。
func LiveNodeRoomsKey(endpointID string) string {
	return fmt.Sprintf("live:node:%s:rooms", endpointID)
}

// LiveNodeRoomChannel 返回仅由指定节点消费的直播间 Pub/Sub 频道。
func LiveNodeRoomChannel(endpointID string, roomID string) string {
	return fmt.Sprintf("live:node:%s:room:%s", endpointID, roomID)
}

// LiveNodeRoomChannelPattern 返回指定节点的直播间 Pub/Sub 订阅模式。
func LiveNodeRoomChannelPattern(endpointID string) string {
	return fmt.Sprintf("live:node:%s:room:*", endpointID)
}

// LiveNodeRoomStreamChannel 返回仅由指定节点消费的重要直播消息通知频道。
func LiveNodeRoomStreamChannel(endpointID string, roomID string) string {
	return fmt.Sprintf("stream:live:node:%s:room:%s", endpointID, roomID)
}

// LiveNodeRoomStreamChannelPattern 返回指定节点的重要直播消息订阅模式。
func LiveNodeRoomStreamChannelPattern(endpointID string) string {
	return fmt.Sprintf("stream:live:node:%s:room:*", endpointID)
}
