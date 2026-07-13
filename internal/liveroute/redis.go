package liveroute

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"time"

	"socket_server/internal/redisclient"
	"socket_server/internal/rediskey"
)

// RedisRegistry 使用带过期分数的 ZSET 保存房间节点租约。
// 分数按节点独立过期，避免一个活跃节点刷新整个集合后让故障节点永久残留。
type RedisRegistry struct {
	client *redisclient.Client
	ttl    time.Duration
}

// NewRedisRegistry 创建使用默认租约时长的 Redis 直播路由表。
func NewRedisRegistry(client *redisclient.Client) *RedisRegistry {
	return &RedisRegistry{client: client, ttl: DefaultLeaseTTL}
}

// RegisterNode 注册一条房间到节点的短租约，并维护节点反向房间索引。
func (r *RedisRegistry) RegisterNode(ctx context.Context, roomID string, endpointID string) error {
	if r.client == nil {
		return errors.New("live route redis client is nil")
	}
	if roomID == "" || endpointID == "" {
		return errors.New("live route is incomplete")
	}
	expiresAt := strconv.FormatInt(time.Now().Add(r.ttl).Unix(), 10)
	keyTTL := strconv.Itoa(int((r.ttl * 3).Seconds()))
	roomKey := r.client.Key(rediskey.LiveRoomNodesKey(roomID))
	nodeKey := r.client.Key(rediskey.LiveNodeRoomsKey(endpointID))
	return r.client.Pipeline(ctx,
		[]string{"ZADD", roomKey, expiresAt, endpointID},
		[]string{"EXPIRE", roomKey, keyTTL},
		[]string{"SADD", nodeKey, roomID},
		[]string{"EXPIRE", nodeKey, keyTTL},
	)
}

// UnregisterNode 删除一条房间节点租约和对应反向索引。
func (r *RedisRegistry) UnregisterNode(ctx context.Context, roomID string, endpointID string) error {
	if r.client == nil {
		return errors.New("live route redis client is nil")
	}
	if roomID == "" || endpointID == "" {
		return nil
	}
	return r.client.Pipeline(ctx,
		[]string{"ZREM", r.client.Key(rediskey.LiveRoomNodesKey(roomID)), endpointID},
		[]string{"SREM", r.client.Key(rediskey.LiveNodeRoomsKey(endpointID)), roomID},
	)
}

// RefreshNodeRooms 用节点当前本地房间快照刷新每个房间的独立租约。
// Redis 中存在但本地已不存在的房间会在同一 pipeline 内移除。
func (r *RedisRegistry) RefreshNodeRooms(ctx context.Context, endpointID string, roomIDs []string) error {
	if r.client == nil {
		return errors.New("live route redis client is nil")
	}
	if endpointID == "" {
		return errors.New("live route endpoint is empty")
	}
	nodeKey := r.client.Key(rediskey.LiveNodeRoomsKey(endpointID))
	stored, err := r.client.Strings(ctx, "SMEMBERS", nodeKey)
	if err != nil {
		return err
	}

	current := uniqueSorted(roomIDs)
	currentSet := make(map[string]struct{}, len(current))
	for _, roomID := range current {
		currentSet[roomID] = struct{}{}
	}
	stale := make([]string, 0)
	for _, roomID := range stored {
		if _, ok := currentSet[roomID]; !ok && roomID != "" {
			stale = append(stale, roomID)
		}
	}
	sort.Strings(stale)

	expiresAt := strconv.FormatInt(time.Now().Add(r.ttl).Unix(), 10)
	keyTTL := strconv.Itoa(int((r.ttl * 3).Seconds()))
	commands := make([][]string, 0, len(current)*2+len(stale)+3)
	for _, roomID := range current {
		roomKey := r.client.Key(rediskey.LiveRoomNodesKey(roomID))
		commands = append(commands,
			[]string{"ZADD", roomKey, expiresAt, endpointID},
			[]string{"EXPIRE", roomKey, keyTTL},
		)
	}
	for _, roomID := range stale {
		commands = append(commands, []string{
			"ZREM",
			r.client.Key(rediskey.LiveRoomNodesKey(roomID)),
			endpointID,
		})
	}

	// 重建反向索引，确保异常中断留下的旧房间不会被后续停止流程继续使用。
	commands = append(commands, []string{"DEL", nodeKey})
	if len(current) > 0 {
		addRooms := []string{"SADD", nodeKey}
		addRooms = append(addRooms, current...)
		commands = append(commands, addRooms, []string{"EXPIRE", nodeKey, keyTTL})
	}
	return r.client.Pipeline(ctx, commands...)
}

// RemoveNode 合并本地快照与 Redis 反向索引，尽可能完整地清理节点租约。
func (r *RedisRegistry) RemoveNode(ctx context.Context, endpointID string, roomIDs []string) error {
	if r.client == nil {
		return errors.New("live route redis client is nil")
	}
	if endpointID == "" {
		return nil
	}
	nodeKey := r.client.Key(rediskey.LiveNodeRoomsKey(endpointID))
	stored, err := r.client.Strings(ctx, "SMEMBERS", nodeKey)
	if err != nil {
		return err
	}
	allRooms := uniqueSorted(append(append([]string(nil), roomIDs...), stored...))
	commands := make([][]string, 0, len(allRooms)+1)
	for _, roomID := range allRooms {
		commands = append(commands, []string{
			"ZREM",
			r.client.Key(rediskey.LiveRoomNodesKey(roomID)),
			endpointID,
		})
	}
	commands = append(commands, []string{"DEL", nodeKey})
	return r.client.Pipeline(ctx, commands...)
}

// ListRoomNodes 清理已过期分数并返回当前仍有效的房间承载节点。
func (r *RedisRegistry) ListRoomNodes(ctx context.Context, roomID string) ([]string, error) {
	if r.client == nil {
		return nil, errors.New("live route redis client is nil")
	}
	if roomID == "" {
		return nil, nil
	}
	roomKey := r.client.Key(rediskey.LiveRoomNodesKey(roomID))
	now := strconv.FormatInt(time.Now().Unix(), 10)
	results, err := r.client.StringLists(ctx,
		[]string{"ZREMRANGEBYSCORE", roomKey, "-inf", now},
		[]string{"ZRANGEBYSCORE", roomKey, "(" + now, "+inf"},
	)
	if err != nil {
		return nil, err
	}
	if len(results) < 2 {
		return nil, errors.New("live route query result is incomplete")
	}
	nodes := uniqueSorted(results[1])
	return nodes, nil
}
