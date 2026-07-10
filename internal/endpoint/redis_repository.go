package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"socket_server/internal/domain"
	"socket_server/internal/redisclient"
	"socket_server/internal/rediskey"
)

// RedisRepository 是基于 Redis 的 WebSocket 节点仓储。
type RedisRepository struct {
	// client 表示 Redis 客户端。
	client *redisclient.Client
}

// NewRedisRepository 创建 Redis 节点仓储。
func NewRedisRepository(client *redisclient.Client) *RedisRepository {
	return &RedisRepository{client: client}
}

// UpsertEndpoint 新增或更新 WebSocket 节点信息。
func (r *RedisRepository) UpsertEndpoint(ctx context.Context, ep *domain.Endpoint) error {
	if ep == nil || ep.ID == "" {
		return errors.New("endpoint is empty")
	}
	copied := *ep
	if copied.UpdatedAt == 0 {
		copied.UpdatedAt = time.Now().Unix()
	}
	body, err := json.Marshal(&copied)
	if err != nil {
		return err
	}
	if _, err := r.client.Do(ctx, "SET", r.client.Key(rediskey.EndpointKey(copied.ID)), string(body)); err != nil {
		return err
	}
	_, err = r.client.Do(ctx, "SADD", r.client.Key(rediskey.EndpointsKey()), copied.ID)
	return err
}

// GetEndpoint 根据节点编号获取 WebSocket 节点信息。
func (r *RedisRepository) GetEndpoint(ctx context.Context, endpointID string) (*domain.Endpoint, error) {
	body, err := r.client.String(ctx, "GET", r.client.Key(rediskey.EndpointKey(endpointID)))
	if err != nil {
		return nil, err
	}
	if body == "" {
		return nil, errors.New("endpoint not found")
	}
	var ep domain.Endpoint
	if err := json.Unmarshal([]byte(body), &ep); err != nil {
		return nil, err
	}
	return &ep, nil
}

// ListEndpoints 获取全部 WebSocket 节点信息。
func (r *RedisRepository) ListEndpoints(ctx context.Context) ([]*domain.Endpoint, error) {
	ids, err := r.client.Strings(ctx, "SMEMBERS", r.client.Key(rediskey.EndpointsKey()))
	if err != nil {
		return nil, err
	}
	sort.Strings(ids)
	endpoints := make([]*domain.Endpoint, 0, len(ids))
	for _, id := range ids {
		ep, err := r.GetEndpoint(ctx, id)
		if err != nil {
			continue
		}
		endpoints = append(endpoints, ep)
	}
	return endpoints, nil
}

// DeleteEndpoint 删除指定 WebSocket 节点信息。
func (r *RedisRepository) DeleteEndpoint(ctx context.Context, endpointID string) error {
	if _, err := r.client.Do(ctx, "DEL", r.client.Key(rediskey.EndpointKey(endpointID))); err != nil {
		return err
	}
	_, err := r.client.Do(ctx, "SREM", r.client.Key(rediskey.EndpointsKey()), endpointID)
	return err
}

// MarkDraining 将指定 WebSocket 节点标记为下线迁移状态。
func (r *RedisRepository) MarkDraining(ctx context.Context, endpointID string) error {
	ep, err := r.GetEndpoint(ctx, endpointID)
	if err != nil {
		return err
	}
	ep.Status = "draining"
	ep.UpdatedAt = time.Now().Unix()
	return r.UpsertEndpoint(ctx, ep)
}
