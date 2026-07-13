package liveroute

import (
	"context"
	"errors"
	"sync"
)

// MemoryRegistry 是用于本地开发和单元测试的直播间节点路由表。
type MemoryRegistry struct {
	mu        sync.RWMutex
	roomNodes map[string]map[string]struct{}
	nodeRooms map[string]map[string]struct{}
}

// NewMemoryRegistry 创建内存直播路由表。
func NewMemoryRegistry() *MemoryRegistry {
	return &MemoryRegistry{
		roomNodes: make(map[string]map[string]struct{}),
		nodeRooms: make(map[string]map[string]struct{}),
	}
}

// RegisterNode 注册一条内存房间节点路由。
func (r *MemoryRegistry) RegisterNode(ctx context.Context, roomID string, endpointID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if roomID == "" || endpointID == "" {
		return errors.New("live route is incomplete")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.roomNodes[roomID]; !ok {
		r.roomNodes[roomID] = make(map[string]struct{})
	}
	if _, ok := r.nodeRooms[endpointID]; !ok {
		r.nodeRooms[endpointID] = make(map[string]struct{})
	}
	r.roomNodes[roomID][endpointID] = struct{}{}
	r.nodeRooms[endpointID][roomID] = struct{}{}
	return nil
}

// UnregisterNode 删除一条内存房间节点路由。
func (r *MemoryRegistry) UnregisterNode(ctx context.Context, roomID string, endpointID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.roomNodes[roomID], endpointID)
	delete(r.nodeRooms[endpointID], roomID)
	if len(r.roomNodes[roomID]) == 0 {
		delete(r.roomNodes, roomID)
	}
	if len(r.nodeRooms[endpointID]) == 0 {
		delete(r.nodeRooms, endpointID)
	}
	return nil
}

// RefreshNodeRooms 用节点当前房间快照重建内存路由。
func (r *MemoryRegistry) RefreshNodeRooms(ctx context.Context, endpointID string, roomIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if endpointID == "" {
		return errors.New("live route endpoint is empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	for roomID := range r.nodeRooms[endpointID] {
		delete(r.roomNodes[roomID], endpointID)
		if len(r.roomNodes[roomID]) == 0 {
			delete(r.roomNodes, roomID)
		}
	}
	delete(r.nodeRooms, endpointID)
	for _, roomID := range uniqueSorted(roomIDs) {
		if _, ok := r.roomNodes[roomID]; !ok {
			r.roomNodes[roomID] = make(map[string]struct{})
		}
		if _, ok := r.nodeRooms[endpointID]; !ok {
			r.nodeRooms[endpointID] = make(map[string]struct{})
		}
		r.roomNodes[roomID][endpointID] = struct{}{}
		r.nodeRooms[endpointID][roomID] = struct{}{}
	}
	return nil
}

// RemoveNode 清理指定节点的全部内存房间路由。
func (r *MemoryRegistry) RemoveNode(ctx context.Context, endpointID string, roomIDs []string) error {
	return r.RefreshNodeRooms(ctx, endpointID, nil)
}

// ListRoomNodes 返回房间当前承载节点的有序快照。
func (r *MemoryRegistry) ListRoomNodes(ctx context.Context, roomID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]string, 0, len(r.roomNodes[roomID]))
	for endpointID := range r.roomNodes[roomID] {
		nodes = append(nodes, endpointID)
	}
	return uniqueSorted(nodes), nil
}
