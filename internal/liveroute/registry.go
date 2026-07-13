// Package liveroute 维护直播间到 WebSocket 节点的短租约路由。
package liveroute

import (
	"context"
	"sort"
	"time"
)

// DefaultLeaseTTL 是节点房间租约的默认有效期；30 秒可缩短故障节点残留窗口。
const DefaultLeaseTTL = 30 * time.Second

// Registry 定义直播间节点路由的注册、刷新、清理与查询能力。
type Registry interface {
	// RegisterNode 在节点首次承载房间时创建短租约路由。
	RegisterNode(ctx context.Context, roomID string, endpointID string) error
	// UnregisterNode 在节点不再承载房间时删除路由。
	UnregisterNode(ctx context.Context, roomID string, endpointID string) error
	// RefreshNodeRooms 按节点本地房间快照刷新租约并清理过期关联。
	RefreshNodeRooms(ctx context.Context, endpointID string, roomIDs []string) error
	// RemoveNode 在节点优雅停止时清理它承载的全部房间路由。
	RemoveNode(ctx context.Context, endpointID string, roomIDs []string) error
	// ListRoomNodes 返回租约仍然有效的房间承载节点。
	ListRoomNodes(ctx context.Context, roomID string) ([]string, error)
}

// uniqueSorted 去重并排序非空标识，保证 Redis 命令和测试结果稳定。
func uniqueSorted(values []string) []string {
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
