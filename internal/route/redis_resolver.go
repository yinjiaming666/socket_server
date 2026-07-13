package route

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"socket_server/internal/domain"
	"socket_server/internal/redisclient"
	"socket_server/internal/rediskey"
)

const (
	// defaultConnRouteTTL 限制异常节点连接绑定最多残留 30 秒。
	defaultConnRouteTTL = 30 * time.Second
	// redisRouteBatchSize 限制单条 MGET 的 Key 数，避免超大回复长时间占用 Redis 事件循环。
	redisRouteBatchSize = 512
)

// ErrConnNotFound 表示连接路由确实不存在，调用方不得把其它 Redis 错误当成过期连接。
var ErrConnNotFound = errors.New("conn not found")

// RedisResolver 是基于 Redis 的用户连接路由解析器。
type RedisResolver struct {
	client *redisclient.Client
}

// NewRedisResolver 创建 Redis 路由解析器。
func NewRedisResolver(client *redisclient.Client) *RedisResolver {
	return &RedisResolver{client: client}
}

// RegisterConn 原子注册一个新的用户连接路由和索引。
func (r *RedisResolver) RegisterConn(ctx context.Context, info *domain.ConnInfo) error {
	if info == nil || info.ConnID == "" || info.UserID == "" || info.EndpointID == "" {
		return errors.New("conn info is incomplete")
	}
	copied := *info
	now := time.Now().Unix()
	if copied.CreatedAt == 0 {
		copied.CreatedAt = now
	}
	if copied.UpdatedAt == 0 {
		copied.UpdatedAt = now
	}
	body, err := json.Marshal(&copied)
	if err != nil {
		return err
	}
	return r.client.Transaction(ctx,
		[]string{"SET", r.client.Key(rediskey.ConnKey(copied.ConnID)), string(body), "EX", strconv.Itoa(int(defaultConnRouteTTL.Seconds()))},
		[]string{"SADD", r.client.Key(rediskey.UserConnsKey(copied.UserID)), copied.ConnID},
		[]string{"SADD", r.client.Key(rediskey.EndpointConnsKey(copied.EndpointID)), copied.ConnID},
	)
}

// RemoveConn 删除一个用户连接路由。
func (r *RedisResolver) RemoveConn(ctx context.Context, connID string) error {
	info, err := r.GetConnInfo(ctx, connID)
	if errors.Is(err, ErrConnNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return r.client.Transaction(ctx,
		[]string{"DEL", r.client.Key(rediskey.ConnKey(connID))},
		[]string{"SREM", r.client.Key(rediskey.UserConnsKey(info.UserID)), connID},
		[]string{"SREM", r.client.Key(rediskey.EndpointConnsKey(info.EndpointID)), connID},
	)
}

// GetUserConnIDs 查询指定用户的全部在线连接编号，并只清理确定已过期的连接。
func (r *RedisResolver) GetUserConnIDs(ctx context.Context, userID string) ([]string, error) {
	return r.GetUsersConnIDs(ctx, []string{userID})
}

// GetUsersConnIDs 查询多个用户的全部在线连接编号。
func (r *RedisResolver) GetUsersConnIDs(ctx context.Context, userIDs []string) ([]string, error) {
	routes, err := r.ResolveUsers(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	connIDs := make([]string, 0, len(routes))
	for _, info := range routes {
		connIDs = append(connIDs, info.ConnID)
	}
	return connIDs, nil
}

// ResolveUsers 通过 pipeline 批量读取用户连接集合，再分批 MGET 路由详情。
// 该方法同时校验用户归属，防止残留索引把连接错误投递给其它用户。
func (r *RedisResolver) ResolveUsers(ctx context.Context, userIDs []string) ([]*domain.ConnInfo, error) {
	userCommands := make([][]string, 0, len(userIDs))
	queriedUsers := make([]string, 0, len(userIDs))
	seenUsers := make(map[string]struct{}, len(userIDs))
	for _, userID := range userIDs {
		if userID == "" {
			continue
		}
		if _, ok := seenUsers[userID]; ok {
			continue
		}
		seenUsers[userID] = struct{}{}
		queriedUsers = append(queriedUsers, userID)
		userCommands = append(userCommands, []string{
			"SMEMBERS",
			r.client.Key(rediskey.UserConnsKey(userID)),
		})
	}

	userResults, err := r.client.StringLists(ctx, userCommands...)
	if err != nil {
		return nil, err
	}

	// connOwners 保存连接来自哪些用户索引，便于识别并清理交叉污染的旧索引。
	connOwners := make(map[string]map[string]struct{})
	for index, connIDs := range userResults {
		userID := queriedUsers[index]
		for _, connID := range connIDs {
			if connID == "" {
				continue
			}
			if _, ok := connOwners[connID]; !ok {
				connOwners[connID] = make(map[string]struct{})
			}
			connOwners[connID][userID] = struct{}{}
		}
	}

	connIDs := make([]string, 0, len(connOwners))
	for connID := range connOwners {
		connIDs = append(connIDs, connID)
	}
	sort.Strings(connIDs)

	infos, err := r.getConnInfos(ctx, connIDs)
	if err != nil {
		return nil, err
	}

	staleByUser := make(map[string][]string)
	routesByConn := make(map[string]*domain.ConnInfo, len(infos))
	for _, connID := range connIDs {
		info, exists := infos[connID]
		owners := connOwners[connID]
		for userID := range owners {
			if !exists || info.UserID != userID {
				staleByUser[userID] = append(staleByUser[userID], connID)
			}
		}
		if !exists {
			continue
		}
		if _, matchesOwner := owners[info.UserID]; !matchesOwner {
			continue
		}
		if info.EndpointID == "" {
			return nil, fmt.Errorf("conn %s endpoint is empty", connID)
		}
		routesByConn[connID] = info
	}

	// 一名用户的多个陈旧连接合并为一条 SREM，并用 pipeline 一次提交。
	cleanupCommands := make([][]string, 0, len(staleByUser))
	cleanupUsers := make([]string, 0, len(staleByUser))
	for userID := range staleByUser {
		cleanupUsers = append(cleanupUsers, userID)
	}
	sort.Strings(cleanupUsers)
	for _, userID := range cleanupUsers {
		staleConnIDs := staleByUser[userID]
		sort.Strings(staleConnIDs)
		args := []string{"SREM", r.client.Key(rediskey.UserConnsKey(userID))}
		args = append(args, staleConnIDs...)
		cleanupCommands = append(cleanupCommands, args)
	}
	if err := r.client.Pipeline(ctx, cleanupCommands...); err != nil {
		return nil, err
	}

	routes := make([]*domain.ConnInfo, 0, len(routesByConn))
	for _, connID := range connIDs {
		if info, ok := routesByConn[connID]; ok {
			routes = append(routes, info)
		}
	}
	return routes, nil
}

// getConnInfos 使用 pipeline 执行分批 MGET，并返回仍然有效的连接路由。
// 缺失 Key 会被忽略；Redis 或 JSON 错误会原样返回，避免把基础设施故障误判为连接下线。
func (r *RedisResolver) getConnInfos(ctx context.Context, connIDs []string) (map[string]*domain.ConnInfo, error) {
	normalized := uniqueSortedStrings(connIDs)
	infos := make(map[string]*domain.ConnInfo, len(normalized))
	if len(normalized) == 0 {
		return infos, nil
	}

	commands := make([][]string, 0, (len(normalized)+redisRouteBatchSize-1)/redisRouteBatchSize)
	batches := make([][]string, 0, cap(commands))
	for start := 0; start < len(normalized); start += redisRouteBatchSize {
		end := start + redisRouteBatchSize
		if end > len(normalized) {
			end = len(normalized)
		}
		batch := normalized[start:end]
		args := make([]string, 1, len(batch)+1)
		args[0] = "MGET"
		for _, connID := range batch {
			args = append(args, r.client.Key(rediskey.ConnKey(connID)))
		}
		commands = append(commands, args)
		batches = append(batches, batch)
	}

	results, err := r.client.StringLists(ctx, commands...)
	if err != nil {
		return nil, err
	}
	for batchIndex, bodies := range results {
		batch := batches[batchIndex]
		if len(bodies) != len(batch) {
			return nil, fmt.Errorf("redis MGET returned %d values for %d keys", len(bodies), len(batch))
		}
		for index, body := range bodies {
			if body == "" {
				continue
			}
			var info domain.ConnInfo
			if err := json.Unmarshal([]byte(body), &info); err != nil {
				return nil, fmt.Errorf("decode conn %s route: %w", batch[index], err)
			}
			if info.ConnID == "" {
				info.ConnID = batch[index]
			}
			if info.ConnID != batch[index] {
				return nil, fmt.Errorf("conn route key %s contains id %s", batch[index], info.ConnID)
			}
			infos[info.ConnID] = &info
		}
	}
	return infos, nil
}

// uniqueSortedStrings 去重并排序字符串，保证批量读取和发送结果稳定。
func uniqueSortedStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for value := range seen {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

// GetConnInfo 查询指定连接的路由详情。
func (r *RedisResolver) GetConnInfo(ctx context.Context, connID string) (*domain.ConnInfo, error) {
	body, err := r.client.String(ctx, "GET", r.client.Key(rediskey.ConnKey(connID)))
	if err != nil {
		return nil, err
	}
	if body == "" {
		return nil, ErrConnNotFound
	}
	var info domain.ConnInfo
	if err := json.Unmarshal([]byte(body), &info); err != nil {
		return nil, err
	}
	return &info, nil
}

// GetConnEndpoint 查询指定连接所在的 endpoint_id。
func (r *RedisResolver) GetConnEndpoint(ctx context.Context, connID string) (string, error) {
	info, err := r.GetConnInfo(ctx, connID)
	if err != nil {
		return "", err
	}
	return info.EndpointID, nil
}

// GroupConnsByEndpoint 将连接编号按 endpoint_id 分组，只跳过确定已过期的连接。
// 路由详情通过批量 MGET 获取，避免按连接逐条访问 Redis。
func (r *RedisResolver) GroupConnsByEndpoint(ctx context.Context, connIDs []string) (EndpointGroup, error) {
	normalized := uniqueSortedStrings(connIDs)
	infos, err := r.getConnInfos(ctx, normalized)
	if err != nil {
		return nil, err
	}

	groups := make(EndpointGroup)
	for _, connID := range normalized {
		info, ok := infos[connID]
		if !ok {
			continue
		}
		if info.EndpointID == "" {
			return nil, fmt.Errorf("conn %s endpoint is empty", connID)
		}
		groups[info.EndpointID] = append(groups[info.EndpointID], connID)
	}
	return groups, nil
}

// GetLastEndpoint 获取用户最近连接过的 endpoint_id。
func (r *RedisResolver) GetLastEndpoint(ctx context.Context, userID string) (string, error) {
	return r.client.String(ctx, "GET", r.client.Key(rediskey.LastEndpointKey(userID)))
}

// SetLastEndpoint 设置用户最近连接过的 endpoint_id。
func (r *RedisResolver) SetLastEndpoint(ctx context.Context, userID string, endpointID string, ttl time.Duration) error {
	args := []string{"SET", r.client.Key(rediskey.LastEndpointKey(userID)), endpointID}
	if ttl > 0 {
		args = append(args, "EX", strconv.Itoa(int(ttl.Seconds())))
	}
	_, err := r.client.Do(ctx, args...)
	return err
}

// ListEndpointConns 获取指定节点上的连接编号列表，并只过滤确定已过期的连接。
// 批量读取还能识别已迁移到其它节点的残留 endpoint 索引。
func (r *RedisResolver) ListEndpointConns(ctx context.Context, endpointID string) ([]string, error) {
	connIDs, err := r.client.Strings(ctx, "SMEMBERS", r.client.Key(rediskey.EndpointConnsKey(endpointID)))
	if err != nil {
		return nil, err
	}
	normalized := uniqueSortedStrings(connIDs)
	infos, err := r.getConnInfos(ctx, normalized)
	if err != nil {
		return nil, err
	}

	result := make([]string, 0, len(infos))
	stale := make([]string, 0)
	for _, connID := range normalized {
		info, ok := infos[connID]
		if !ok || info.EndpointID != endpointID {
			stale = append(stale, connID)
			continue
		}
		result = append(result, connID)
	}
	if len(stale) > 0 {
		args := []string{"SREM", r.client.Key(rediskey.EndpointConnsKey(endpointID))}
		args = append(args, stale...)
		if _, err := r.client.Do(ctx, args...); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// RemoveEndpointConns 清理指定节点上的全部连接路由。
func (r *RedisResolver) RemoveEndpointConns(ctx context.Context, endpointID string) error {
	connIDs, err := r.ListEndpointConns(ctx, endpointID)
	if err != nil {
		return err
	}
	for _, connID := range connIDs {
		if err := r.RemoveConn(ctx, connID); err != nil {
			return err
		}
	}
	_, err = r.client.Do(ctx, "DEL", r.client.Key(rediskey.EndpointConnsKey(endpointID)))
	return err
}

// RefreshEndpointConns 使用 pipeline 刷新指定节点连接路由 TTL。
func (r *RedisResolver) RefreshEndpointConns(ctx context.Context, endpointID string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = defaultConnRouteTTL
	}
	connIDs, err := r.client.Strings(ctx, "SMEMBERS", r.client.Key(rediskey.EndpointConnsKey(endpointID)))
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(connIDs))
	for _, connID := range connIDs {
		keys = append(keys, r.client.Key(rediskey.ConnKey(connID)))
	}
	return r.client.ExpireKeys(ctx, keys, ttl)
}
