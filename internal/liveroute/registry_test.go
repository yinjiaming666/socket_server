package liveroute

import (
	"context"
	"reflect"
	"testing"
)

// TestMemoryRegistryLifecycle 验证注册、快照刷新和节点清理会同步更新双向索引。
func TestMemoryRegistryLifecycle(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryRegistry()
	if err := registry.RegisterNode(ctx, "room-1", "node-b"); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterNode(ctx, "room-1", "node-a"); err != nil {
		t.Fatal(err)
	}

	nodes, err := registry.ListRoomNodes(ctx, "room-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nodes, []string{"node-a", "node-b"}) {
		t.Fatalf("unexpected room-1 nodes: %#v", nodes)
	}

	// node-a 的新快照不再包含 room-1，应迁移到 room-2。
	if err := registry.RefreshNodeRooms(ctx, "node-a", []string{"room-2", "room-2"}); err != nil {
		t.Fatal(err)
	}
	room1Nodes, err := registry.ListRoomNodes(ctx, "room-1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(room1Nodes, []string{"node-b"}) {
		t.Fatalf("stale node was not removed: %#v", room1Nodes)
	}
	room2Nodes, err := registry.ListRoomNodes(ctx, "room-2")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(room2Nodes, []string{"node-a"}) {
		t.Fatalf("refreshed room was not registered: %#v", room2Nodes)
	}

	if err := registry.RemoveNode(ctx, "node-b", []string{"room-1"}); err != nil {
		t.Fatal(err)
	}
	room1Nodes, err = registry.ListRoomNodes(ctx, "room-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(room1Nodes) != 0 {
		t.Fatalf("removed node still owns room-1: %#v", room1Nodes)
	}
}
