package websocket

import (
	"context"
	"sync"
	"testing"
	"time"

	"socket_server/internal/ack"
	"socket_server/internal/domain"
	"socket_server/internal/endpoint"
	"socket_server/internal/gateway"
	"socket_server/internal/route"
)

func TestClientCloseAndEnqueueAreSafe(t *testing.T) {
	client := NewClient("conn-1", "user-1", "", "", 8)
	var group sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := 0; index < 1000; index++ {
				client.Enqueue([]byte("message"))
			}
		}()
	}
	client.Close()
	group.Wait()
	select {
	case <-client.Done():
	default:
		t.Fatal("client done channel was not closed")
	}
	if client.Enqueue([]byte("late")) {
		t.Fatal("closed client accepted a message")
	}
}

func TestOldConnectionCannotRemoveReplacement(t *testing.T) {
	ctx := context.Background()
	resolver := route.NewMemoryResolver()
	tokenManager := gateway.NewRouteTokenManager("test-secret", time.Minute)
	node := NewNode(
		NodeConfig{EndpointID: "node-1", MaxConn: 10, SendBuffer: 8},
		endpoint.NewMemoryRepository(),
		resolver,
		ack.NewMemoryStore(),
		tokenManager,
	)
	token, err := tokenManager.Generate("user-1", "node-1")
	if err != nil {
		t.Fatal(err)
	}
	first, err := node.RegisterConnection(ctx, "same-conn", token, "device-1", "test")
	if err != nil {
		t.Fatal(err)
	}
	second, err := node.RegisterConnection(ctx, "same-conn", token, "device-2", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := node.RemoveClientConnection(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.GetConnInfo(ctx, "same-conn"); err != nil {
		t.Fatalf("old connection removed replacement route: %v", err)
	}
	response, err := node.PushToConns(ctx, []string{"same-conn"}, &domain.Message{
		MessageID: "msg-1", BizType: "single", Payload: []byte(`{"ok":true}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !response.Accepted || len(response.AcceptedConnIDs) != 1 {
		t.Fatalf("replacement connection was not active: %#v", response)
	}
	if err := node.RemoveClientConnection(ctx, second); err != nil {
		t.Fatal(err)
	}
}
