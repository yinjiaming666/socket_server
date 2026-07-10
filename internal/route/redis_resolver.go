package route

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"socket_server/internal/domain"
	"socket_server/internal/redisclient"
	"socket_server/internal/rediskey"
)

const defaultConnRouteTTL = 180 * time.Second

// RedisResolver 是基于 Redis 的用户连接路由解析器。
type RedisResolver struct {
	client *redisclient.Client
}

// NewRedisResolver 创建 Redis 路由解析器。
func NewRedisResolver(client *redisclient.Client) *RedisResolver {
	return &RedisResolver{client: client}
}

// RegisterConn 注册一个新的用户连接路由。
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
	if _, err := r.client.Do(ctx, "SET", r.client.Key(rediskey.ConnKey(copied.ConnID)), string(body), "EX", strconv.Itoa(int(defaultConnRouteTTL.Seconds()))); err != nil {
		return err
	}
	if _, err := r.client.Do(ctx, "SADD", r.client.Key(rediskey.UserConnsKey(copied.UserID)), copied.ConnID); err != nil {
		return err
	}
	_, err = r.client.Do(ctx, "SADD", r.client.Key(rediskey.EndpointConnsKey(copied.EndpointID)), copied.ConnID)
	return err
}

// RemoveConn 删除一个用户连接路由。
func (r *RedisResolver) RemoveConn(ctx context.Context, connID string) error {
	info, err := r.GetConnInfo(ctx, connID)
	if err != nil {
		return nil
	}
	if _, err := r.client.Do(ctx, "DEL", r.client.Key(rediskey.ConnKey(connID))); err != nil {
		return err
	}
	if _, err := r.client.Do(ctx, "SREM", r.client.Key(rediskey.UserConnsKey(info.UserID)), connID); err != nil {
		return err
	}
	_, err = r.client.Do(ctx, "SREM", r.client.Key(rediskey.EndpointConnsKey(info.EndpointID)), connID)
	return err
}

// GetUserConnIDs 查询指定用户的全部在线连接编号，并顺手过滤过期连接。
func (r *RedisResolver) GetUserConnIDs(ctx context.Context, userID string) ([]string, error) {
	connIDs, err := r.client.Strings(ctx, "SMEMBERS", r.client.Key(rediskey.UserConnsKey(userID)))
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(connIDs))
	for _, connID := range connIDs {
		if _, err := r.GetConnInfo(ctx, connID); err != nil {
			_, _ = r.client.Do(ctx, "SREM", r.client.Key(rediskey.UserConnsKey(userID)), connID)
			continue
		}
		result = append(result, connID)
	}
	sort.Strings(result)
	return result, nil
}

// GetUsersConnIDs 查询多个用户的全部在线连接编号。
func (r *RedisResolver) GetUsersConnIDs(ctx context.Context, userIDs []string) ([]string, error) {
	seen := make(map[string]struct{})
	for _, userID := range userIDs {
		connIDs, err := r.GetUserConnIDs(ctx, userID)
		if err != nil {
			return nil, err
		}
		for _, connID := range connIDs {
			seen[connID] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for connID := range seen {
		result = append(result, connID)
	}
	sort.Strings(result)
	return result, nil
}

// GetConnInfo 查询指定连接的路由详情。
func (r *RedisResolver) GetConnInfo(ctx context.Context, connID string) (*domain.ConnInfo, error) {
	body, err := r.client.String(ctx, "GET", r.client.Key(rediskey.ConnKey(connID)))
	if err != nil {
		return nil, err
	}
	if body == "" {
		return nil, errors.New("conn not found")
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

// GroupConnsByEndpoint 将连接编号按 endpoint_id 分组，并跳过过期连接。
func (r *RedisResolver) GroupConnsByEndpoint(ctx context.Context, connIDs []string) (EndpointGroup, error) {
	groups := make(EndpointGroup)
	for _, connID := range connIDs {
		info, err := r.GetConnInfo(ctx, connID)
		if err != nil || info.EndpointID == "" {
			continue
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

// ListEndpointConns 获取指定节点上的连接编号列表，并过滤过期连接。
func (r *RedisResolver) ListEndpointConns(ctx context.Context, endpointID string) ([]string, error) {
	connIDs, err := r.client.Strings(ctx, "SMEMBERS", r.client.Key(rediskey.EndpointConnsKey(endpointID)))
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(connIDs))
	for _, connID := range connIDs {
		if _, err := r.GetConnInfo(ctx, connID); err != nil {
			_, _ = r.client.Do(ctx, "SREM", r.client.Key(rediskey.EndpointConnsKey(endpointID)), connID)
			continue
		}
		result = append(result, connID)
	}
	sort.Strings(result)
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
	return nil
}

// RefreshEndpointConns 刷新指定节点连接路由 TTL。
func (r *RedisResolver) RefreshEndpointConns(ctx context.Context, endpointID string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = defaultConnRouteTTL
	}
	connIDs, err := r.client.Strings(ctx, "SMEMBERS", r.client.Key(rediskey.EndpointConnsKey(endpointID)))
	if err != nil {
		return err
	}
	seconds := strconv.Itoa(int(ttl.Seconds()))
	for _, connID := range connIDs {
		if _, err := r.client.Do(ctx, "EXPIRE", r.client.Key(rediskey.ConnKey(connID)), seconds); err != nil {
			return err
		}
	}
	return nil
}
