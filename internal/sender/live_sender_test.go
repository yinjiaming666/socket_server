package sender

import (
	"context"
	"testing"

	"socket_server/internal/bus"
	"socket_server/internal/domain"
	"socket_server/internal/liveroute"
	"socket_server/internal/rediskey"
)

// TestRoutedRedisPubSubSenderOnlyPublishesOwningNodes 验证直播消息不会广播到未承载房间的节点。
func TestRoutedRedisPubSubSenderOnlyPublishesOwningNodes(t *testing.T) {
	ctx := context.Background()
	registry := liveroute.NewMemoryRegistry()
	for _, endpointID := range []string{"node-a", "node-b"} {
		if err := registry.RegisterNode(ctx, "room-1", endpointID); err != nil {
			t.Fatal(err)
		}
	}

	pubSub := bus.NewMemoryPubSub()
	nodeA := pubSub.Subscribe(rediskey.LiveNodeRoomChannel("node-a", "room-1"), 1)
	nodeB := pubSub.Subscribe(rediskey.LiveNodeRoomChannel("node-b", "room-1"), 1)
	unrelated := pubSub.Subscribe(rediskey.LiveNodeRoomChannel("node-c", "room-1"), 1)
	defer nodeA.Close()
	defer nodeB.Close()
	defer unrelated.Close()

	roomSender := NewRoutedRedisPubSubSender(pubSub, registry)
	result, err := roomSender.Send(ctx, &domain.Message{
		MessageID: "live-1",
		RoomID:    "room-1",
		BizType:   "live_danmu",
		Payload:   []byte("{\"text\":\"hello\"}"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted {
		t.Fatalf("message was not accepted: %#v", result)
	}

	// PublishMany 是同步调用，返回后两个目标节点都应已有消息。
	for endpointID, subscription := range map[string]*bus.Subscription{
		"node-a": nodeA,
		"node-b": nodeB,
	} {
		select {
		case payload := <-subscription.C:
			if len(payload) == 0 {
				t.Fatalf("%s received an empty payload", endpointID)
			}
		default:
			t.Fatalf("%s did not receive the room message", endpointID)
		}
	}
	select {
	case payload := <-unrelated.C:
		t.Fatalf("unrelated node received payload: %s", payload)
	default:
	}
}
