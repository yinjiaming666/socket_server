package endpoint

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"socket_server/internal/domain"
)

// Repository 定义 WebSocket 节点信息仓储接口。
type Repository interface {
	// UpsertEndpoint 新增或更新 WebSocket 节点信息。
	UpsertEndpoint(ctx context.Context, ep *domain.Endpoint) error
	// GetEndpoint 根据节点编号获取 WebSocket 节点信息。
	GetEndpoint(ctx context.Context, endpointID string) (*domain.Endpoint, error)
	// ListEndpoints 获取全部 WebSocket 节点信息。
	ListEndpoints(ctx context.Context) ([]*domain.Endpoint, error)
	// DeleteEndpoint 删除指定 WebSocket 节点信息。
	DeleteEndpoint(ctx context.Context, endpointID string) error
	// MarkDraining 将指定 WebSocket 节点标记为下线迁移状态。
	MarkDraining(ctx context.Context, endpointID string) error
}

// MemoryRepository 是用于本地开发和单元测试的内存节点仓储。
type MemoryRepository struct {
	// mu 保护 endpoints 并发读写。
	mu sync.RWMutex
	// endpoints 保存 endpoint_id 到节点详情的映射。
	endpoints map[string]*domain.Endpoint
}

// NewMemoryRepository 创建内存节点仓储。
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		endpoints: make(map[string]*domain.Endpoint),
	}
}

// UpsertEndpoint 新增或更新 WebSocket 节点信息。
func (r *MemoryRepository) UpsertEndpoint(ctx context.Context, ep *domain.Endpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ep == nil || ep.ID == "" {
		return errors.New("endpoint is empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	copied := *ep
	if copied.UpdatedAt == 0 {
		copied.UpdatedAt = time.Now().Unix()
	}
	r.endpoints[copied.ID] = &copied
	return nil
}

// GetEndpoint 根据节点编号获取 WebSocket 节点信息。
func (r *MemoryRepository) GetEndpoint(ctx context.Context, endpointID string) (*domain.Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	ep, ok := r.endpoints[endpointID]
	if !ok {
		return nil, errors.New("endpoint not found")
	}

	copied := *ep
	return &copied, nil
}

// ListEndpoints 获取全部 WebSocket 节点信息。
func (r *MemoryRepository) ListEndpoints(ctx context.Context) ([]*domain.Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	ids := make([]string, 0, len(r.endpoints))
	for id := range r.endpoints {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	endpoints := make([]*domain.Endpoint, 0, len(ids))
	for _, id := range ids {
		copied := *r.endpoints[id]
		endpoints = append(endpoints, &copied)
	}

	return endpoints, nil
}

// DeleteEndpoint 删除指定 WebSocket 节点信息。
func (r *MemoryRepository) DeleteEndpoint(ctx context.Context, endpointID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.endpoints, endpointID)
	return nil
}

// MarkDraining 将指定 WebSocket 节点标记为下线迁移状态。
func (r *MemoryRepository) MarkDraining(ctx context.Context, endpointID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	ep, ok := r.endpoints[endpointID]
	if !ok {
		return errors.New("endpoint not found")
	}
	ep.Status = "draining"
	ep.UpdatedAt = time.Now().Unix()
	return nil
}
