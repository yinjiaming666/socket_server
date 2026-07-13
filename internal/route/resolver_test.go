package route

import (
	"context"
	"testing"

	"socket_server/internal/domain"
)

// TestMemoryResolverResolveUsersReturnsUniqueCopies 验证批量解析会去重、排序且不会暴露内部可变对象。
func TestMemoryResolverResolveUsersReturnsUniqueCopies(t *testing.T) {
	ctx := context.Background()
	resolver := NewMemoryResolver()
	connections := []*domain.ConnInfo{
		{ConnID: "conn-b", UserID: "user-1", EndpointID: "node-2"},
		{ConnID: "conn-a", UserID: "user-1", EndpointID: "node-1"},
		{ConnID: "conn-c", UserID: "user-2", EndpointID: "node-2"},
	}
	for _, info := range connections {
		if err := resolver.RegisterConn(ctx, info); err != nil {
			t.Fatalf("register %s: %v", info.ConnID, err)
		}
	}

	routes, err := resolver.ResolveUsers(ctx, []string{"user-2", "user-1", "user-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 3 {
		t.Fatalf("expected 3 unique routes, got %d", len(routes))
	}
	for index, expected := range []string{"conn-a", "conn-b", "conn-c"} {
		if routes[index].ConnID != expected {
			t.Fatalf("route %d: expected %s, got %s", index, expected, routes[index].ConnID)
		}
	}

	// 修改返回对象后再次读取，确认解析器内部数据没有被调用方污染。
	routes[0].EndpointID = "mutated-node"
	stored, err := resolver.GetConnInfo(ctx, "conn-a")
	if err != nil {
		t.Fatal(err)
	}
	if stored.EndpointID != "node-1" {
		t.Fatalf("resolver state was mutated through returned route: %s", stored.EndpointID)
	}
}
