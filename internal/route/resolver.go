package route

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"socket_server/internal/domain"
)

// EndpointGroup 表示按 endpoint_id 分组后的连接编号列表。
type EndpointGroup map[string][]string

// Resolver 定义用户连接路由查询和维护接口。
type Resolver interface {
	RegisterConn(ctx context.Context, info *domain.ConnInfo) error
	RemoveConn(ctx context.Context, connID string) error
	GetUserConnIDs(ctx context.Context, userID string) ([]string, error)
	GetUsersConnIDs(ctx context.Context, userIDs []string) ([]string, error)
	GetConnInfo(ctx context.Context, connID string) (*domain.ConnInfo, error)
	GetConnEndpoint(ctx context.Context, connID string) (string, error)
	GroupConnsByEndpoint(ctx context.Context, connIDs []string) (EndpointGroup, error)
	GetLastEndpoint(ctx context.Context, userID string) (string, error)
	SetLastEndpoint(ctx context.Context, userID string, endpointID string, ttl time.Duration) error
	ListEndpointConns(ctx context.Context, endpointID string) ([]string, error)
	RemoveEndpointConns(ctx context.Context, endpointID string) error
	RefreshEndpointConns(ctx context.Context, endpointID string, ttl time.Duration) error
}

type lastEndpoint struct {
	endpointID string
	expiresAt  time.Time
}

// MemoryResolver 是用于本地开发和单元测试的内存路由解析器。
type MemoryResolver struct {
	mu            sync.RWMutex
	conns         map[string]*domain.ConnInfo
	userConns     map[string]map[string]struct{}
	endpointConns map[string]map[string]struct{}
	lastEndpoints map[string]lastEndpoint
}

// NewMemoryResolver 创建内存路由解析器。
func NewMemoryResolver() *MemoryResolver {
	return &MemoryResolver{
		conns:         make(map[string]*domain.ConnInfo),
		userConns:     make(map[string]map[string]struct{}),
		endpointConns: make(map[string]map[string]struct{}),
		lastEndpoints: make(map[string]lastEndpoint),
	}
}

// RegisterConn 注册一个新的用户连接路由。
func (r *MemoryResolver) RegisterConn(ctx context.Context, info *domain.ConnInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if info == nil || info.ConnID == "" || info.UserID == "" || info.EndpointID == "" {
		return errors.New("conn info is incomplete")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := *info
	now := time.Now().Unix()
	if copied.CreatedAt == 0 {
		copied.CreatedAt = now
	}
	if copied.UpdatedAt == 0 {
		copied.UpdatedAt = now
	}
	r.conns[copied.ConnID] = &copied
	if _, ok := r.userConns[copied.UserID]; !ok {
		r.userConns[copied.UserID] = make(map[string]struct{})
	}
	r.userConns[copied.UserID][copied.ConnID] = struct{}{}
	if _, ok := r.endpointConns[copied.EndpointID]; !ok {
		r.endpointConns[copied.EndpointID] = make(map[string]struct{})
	}
	r.endpointConns[copied.EndpointID][copied.ConnID] = struct{}{}
	return nil
}

// RemoveConn 删除一个用户连接路由。
func (r *MemoryResolver) RemoveConn(ctx context.Context, connID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	info, ok := r.conns[connID]
	if !ok {
		return nil
	}
	delete(r.conns, connID)
	delete(r.userConns[info.UserID], connID)
	delete(r.endpointConns[info.EndpointID], connID)
	if len(r.userConns[info.UserID]) == 0 {
		delete(r.userConns, info.UserID)
	}
	if len(r.endpointConns[info.EndpointID]) == 0 {
		delete(r.endpointConns, info.EndpointID)
	}
	return nil
}

// GetUserConnIDs 查询指定用户的全部在线连接编号。
func (r *MemoryResolver) GetUserConnIDs(ctx context.Context, userID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	connIDs := make([]string, 0, len(r.userConns[userID]))
	for connID := range r.userConns[userID] {
		connIDs = append(connIDs, connID)
	}
	sort.Strings(connIDs)
	return connIDs, nil
}

// GetUsersConnIDs 查询多个用户的全部在线连接编号。
func (r *MemoryResolver) GetUsersConnIDs(ctx context.Context, userIDs []string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	connMap := make(map[string]struct{})
	for _, userID := range userIDs {
		for connID := range r.userConns[userID] {
			connMap[connID] = struct{}{}
		}
	}
	connIDs := make([]string, 0, len(connMap))
	for connID := range connMap {
		connIDs = append(connIDs, connID)
	}
	sort.Strings(connIDs)
	return connIDs, nil
}

// GetConnInfo 查询指定连接的路由详情。
func (r *MemoryResolver) GetConnInfo(ctx context.Context, connID string) (*domain.ConnInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.conns[connID]
	if !ok {
		return nil, errors.New("conn not found")
	}
	copied := *info
	return &copied, nil
}

// GetConnEndpoint 查询指定连接所在的 endpoint_id。
func (r *MemoryResolver) GetConnEndpoint(ctx context.Context, connID string) (string, error) {
	info, err := r.GetConnInfo(ctx, connID)
	if err != nil {
		return "", err
	}
	return info.EndpointID, nil
}

// GroupConnsByEndpoint 将连接编号按 endpoint_id 分组。
func (r *MemoryResolver) GroupConnsByEndpoint(ctx context.Context, connIDs []string) (EndpointGroup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	groups := make(EndpointGroup)
	for _, connID := range connIDs {
		if info, ok := r.conns[connID]; ok {
			groups[info.EndpointID] = append(groups[info.EndpointID], connID)
		}
	}
	return groups, nil
}

// GetLastEndpoint 获取用户最近连接过的 endpoint_id。
func (r *MemoryResolver) GetLastEndpoint(ctx context.Context, userID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.mu.RLock()
	item, ok := r.lastEndpoints[userID]
	r.mu.RUnlock()
	if !ok || time.Now().After(item.expiresAt) {
		return "", nil
	}
	return item.endpointID, nil
}

// SetLastEndpoint 设置用户最近连接过的 endpoint_id。
func (r *MemoryResolver) SetLastEndpoint(ctx context.Context, userID string, endpointID string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lastEndpoints[userID] = lastEndpoint{endpointID: endpointID, expiresAt: time.Now().Add(ttl)}
	return nil
}

// ListEndpointConns 获取指定节点上的连接编号列表。
func (r *MemoryResolver) ListEndpointConns(ctx context.Context, endpointID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	connIDs := make([]string, 0, len(r.endpointConns[endpointID]))
	for connID := range r.endpointConns[endpointID] {
		connIDs = append(connIDs, connID)
	}
	sort.Strings(connIDs)
	return connIDs, nil
}

// RemoveEndpointConns 清理指定节点上的全部连接路由。
func (r *MemoryResolver) RemoveEndpointConns(ctx context.Context, endpointID string) error {
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

// RefreshEndpointConns 内存模式无 TTL，保持接口一致。
func (r *MemoryResolver) RefreshEndpointConns(ctx context.Context, endpointID string, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
