package websocket

import (
	"context"
	"reflect"
	"testing"
	"time"

	"socket_server/internal/ack"
	"socket_server/internal/endpoint"
	"socket_server/internal/gateway"
	"socket_server/internal/liveroute"
	"socket_server/internal/rediskey"
	"socket_server/internal/route"
)

// TestNodeMaintainsLiveRoomLeaseOnFirstJoinAndLastLeave 验证节点只在首个加入和最后离开时改变房间租约。
func TestNodeMaintainsLiveRoomLeaseOnFirstJoinAndLastLeave(t *testing.T) {
	ctx := context.Background()
	registry := liveroute.NewMemoryRegistry()
	resolver := route.NewMemoryResolver()
	tokenManager := gateway.NewRouteTokenManager("live-route-secret", time.Minute)
	node := NewNode(
		NodeConfig{EndpointID: "node-live", MaxConn: 10, SendBuffer: 8},
		endpoint.NewMemoryRepository(),
		resolver,
		ack.NewMemoryStore(),
		tokenManager,
	)
	node.SetLiveRoomRegistry(registry)

	token, err := tokenManager.Generate("user-live", "node-live")
	if err != nil {
		t.Fatal(err)
	}
	first, err := node.RegisterConnection(ctx, "conn-live-1", token, "device-1", "test")
	if err != nil {
		t.Fatal(err)
	}
	second, err := node.RegisterConnection(ctx, "conn-live-2", token, "device-2", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := node.JoinRoom(ctx, first.ConnID, "room-live"); err != nil {
		t.Fatal(err)
	}
	if err := node.JoinRoom(ctx, second.ConnID, "room-live"); err != nil {
		t.Fatal(err)
	}

	nodes, err := registry.ListRoomNodes(ctx, "room-live")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nodes, []string{"node-live"}) {
		t.Fatalf("room lease was not registered: %#v", nodes)
	}

	if err := node.LeaveRoom(ctx, first.ConnID, "room-live"); err != nil {
		t.Fatal(err)
	}
	nodes, err = registry.ListRoomNodes(ctx, "room-live")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nodes, []string{"node-live"}) {
		t.Fatalf("lease was removed before the last member left: %#v", nodes)
	}

	if err := node.RemoveClientConnection(ctx, second); err != nil {
		t.Fatal(err)
	}
	nodes, err = registry.ListRoomNodes(ctx, "room-live")
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 {
		t.Fatalf("lease remained after the last member left: %#v", nodes)
	}
	if err := node.RemoveClientConnection(ctx, first); err != nil {
		t.Fatal(err)
	}
}

// TestRoomIDFromTargetedRedisChannel 验证节点频道解析不会接受其它节点的消息。
func TestRoomIDFromTargetedRedisChannel(t *testing.T) {
	cases := []struct {
		name     string
		channel  string
		endpoint string
		expected string
	}{
		{
			name:     "pubsub",
			channel:  rediskey.LiveNodeRoomChannel("node-a", "room-1"),
			endpoint: "node-a",
			expected: "room-1",
		},
		{
			name:     "stream notification",
			channel:  rediskey.LiveNodeRoomStreamChannel("node-a", "room-2"),
			endpoint: "node-a",
			expected: "room-2",
		},
		{
			name:     "other endpoint",
			channel:  rediskey.LiveNodeRoomChannel("node-b", "room-3"),
			endpoint: "node-a",
			expected: "",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if actual := roomIDFromRedisChannel(testCase.channel, testCase.endpoint); actual != testCase.expected {
				t.Fatalf("expected %q, got %q", testCase.expected, actual)
			}
		})
	}
}
