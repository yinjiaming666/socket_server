package sender

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"testing"

	"socket_server/internal/domain"
)

// recordingPushClient 记录每个节点收到的连接集合，用于验证发送器路由分组。
type recordingPushClient struct {
	mu    sync.Mutex
	calls map[string][]string
}

// PushToConns 记录一次精准推送并模拟节点全部接收成功。
func (c *recordingPushClient) PushToConns(ctx context.Context, endpointID string, connIDs []string, msg *domain.Message) (*domain.PushResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.calls[endpointID] = append([]string(nil), connIDs...)
	c.mu.Unlock()
	return &domain.PushResponse{
		Accepted:        true,
		AcceptedConnIDs: append([]string(nil), connIDs...),
	}, nil
}

// PushToUsers 满足 push.Client 接口；当前测试只覆盖按连接推送。
func (c *recordingPushClient) PushToUsers(ctx context.Context, endpointID string, userIDs []string, msg *domain.Message) (*domain.PushResponse, error) {
	return &domain.PushResponse{Accepted: true}, ctx.Err()
}

// TestGRPCSenderReusesResolvedRoutes 验证完整路由存在时不会依赖 Resolver 再次查询。
func TestGRPCSenderReusesResolvedRoutes(t *testing.T) {
	client := &recordingPushClient{calls: make(map[string][]string)}
	directSender := NewGRPCSender(nil, client)
	msg := &domain.Message{
		ConnIDs: []string{"conn-a", "conn-b", "conn-c"},
		DeliveryRoutes: []*domain.ConnInfo{
			{ConnID: "conn-a", EndpointID: "node-1"},
			{ConnID: "conn-b", EndpointID: "node-2"},
			{ConnID: "conn-c", EndpointID: "node-1"},
			// 重复路由用于确认同一连接不会被重复投递。
			{ConnID: "conn-a", EndpointID: "node-1"},
		},
	}

	result, err := directSender.Send(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || len(result.AcceptedConnIDs) != 3 {
		t.Fatalf("unexpected send result: %#v", result)
	}

	client.mu.Lock()
	node1 := append([]string(nil), client.calls["node-1"]...)
	node2 := append([]string(nil), client.calls["node-2"]...)
	client.mu.Unlock()
	sort.Strings(node1)
	sort.Strings(node2)
	if !reflect.DeepEqual(node1, []string{"conn-a", "conn-c"}) {
		t.Fatalf("unexpected node-1 routes: %#v", node1)
	}
	if !reflect.DeepEqual(node2, []string{"conn-b"}) {
		t.Fatalf("unexpected node-2 routes: %#v", node2)
	}
}
