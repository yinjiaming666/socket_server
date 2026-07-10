package push

import (
	"context"
	"errors"
	"sync"

	"socket_server/internal/domain"
)

// Client 定义 Message API 调用 WebSocket Node 的内部推送客户端接口。
type Client interface {
	// PushToConns 将消息投递到指定节点上的连接集合。
	PushToConns(ctx context.Context, endpointID string, connIDs []string, msg *domain.Message) (*domain.PushResponse, error)
	// PushToUsers 将消息投递到指定节点上的用户集合。
	PushToUsers(ctx context.Context, endpointID string, userIDs []string, msg *domain.Message) (*domain.PushResponse, error)
}

// Node 定义可接收内部推送请求的 WebSocket 节点接口。
type Node interface {
	// PushToConns 将消息写入本节点指定连接的发送队列。
	PushToConns(ctx context.Context, connIDs []string, msg *domain.Message) (*domain.PushResponse, error)
	// PushToUsers 将消息写入本节点指定用户的发送队列。
	PushToUsers(ctx context.Context, userIDs []string, msg *domain.Message) (*domain.PushResponse, error)
}

// Registry 保存本进程内可调用的节点实例。
type Registry struct {
	// mu 保护 nodes 并发读写。
	mu sync.RWMutex
	// nodes 保存 endpoint_id 到节点实例的映射。
	nodes map[string]Node
}

// NewRegistry 创建节点注册表。
func NewRegistry() *Registry {
	return &Registry{nodes: make(map[string]Node)}
}

// Register 注册一个节点实例。
func (r *Registry) Register(endpointID string, node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[endpointID] = node
}

// Unregister 移除一个节点实例。
func (r *Registry) Unregister(endpointID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, endpointID)
}

// Get 获取一个节点实例。
func (r *Registry) Get(endpointID string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.nodes[endpointID]
	return node, ok
}

// InMemoryClient 通过本地注册表模拟内部推送调用。
type InMemoryClient struct {
	// registry 表示本地节点注册表。
	registry *Registry
}

// NewInMemoryClient 创建内存推送客户端。
func NewInMemoryClient(registry *Registry) *InMemoryClient {
	return &InMemoryClient{registry: registry}
}

// PushToConns 将消息投递到指定节点上的连接集合。
func (c *InMemoryClient) PushToConns(ctx context.Context, endpointID string, connIDs []string, msg *domain.Message) (*domain.PushResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	node, ok := c.registry.Get(endpointID)
	if !ok {
		return nil, errors.New("push node not found")
	}
	return node.PushToConns(ctx, connIDs, msg)
}

// PushToUsers 将消息投递到指定节点上的用户集合。
func (c *InMemoryClient) PushToUsers(ctx context.Context, endpointID string, userIDs []string, msg *domain.Message) (*domain.PushResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	node, ok := c.registry.Get(endpointID)
	if !ok {
		return nil, errors.New("push node not found")
	}
	return node.PushToUsers(ctx, userIDs, msg)
}
