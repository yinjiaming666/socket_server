package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	xwebsocket "golang.org/x/net/websocket"

	"socket_server/internal/ack"
	"socket_server/internal/bus"
	"socket_server/internal/endpoint"
	"socket_server/internal/gateway"
	"socket_server/internal/message"
	"socket_server/internal/push"
	"socket_server/internal/route"
	"socket_server/internal/sender"
)

func TestEndToEndRoutePushAndAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	endpointRepo := endpoint.NewMemoryRepository()
	resolver := route.NewMemoryResolver()
	ackStore := ack.NewMemoryStore()
	tokenManager := gateway.NewRouteTokenManager("e2e-secret", time.Minute)
	node := NewNode(NodeConfig{
		EndpointID: "node-e2e", WSPath: "/ws/node-e2e", WSAddr: "127.0.0.1",
		MaxConn: 100, SendBuffer: 16, HeartbeatInterval: time.Minute,
	}, endpointRepo, resolver, ackStore, tokenManager)
	if err := node.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer node.Stop(context.Background())

	wsServer := httptest.NewServer(NewMux(NewHandler(node)))
	defer wsServer.Close()
	publicHost := strings.TrimPrefix(wsServer.URL, "http://")

	gatewayHandler := gateway.NewHandler(
		gateway.NewDefaultEndpointSelector(endpointRepo, resolver, time.Minute, time.Minute),
		tokenManager, publicHost, 60,
	)
	gatewayServer := httptest.NewServer(gateway.NewMux(gatewayHandler))
	defer gatewayServer.Close()

	registry := push.NewRegistry()
	registry.Register("node-e2e", node)
	direct := sender.NewGRPCSender(resolver, push.NewInMemoryClient(registry))
	selector := sender.NewDefaultSenderSelector(
		direct,
		sender.NewRedisPubSubSender(bus.NewMemoryPubSub()),
		sender.NewRedisStreamSender(bus.NewMemoryStream()),
		nil,
	)
	messageService := message.NewService(message.NewEmptyStore(), resolver, ackStore, selector)
	messageServer := httptest.NewServer(message.NewMux(message.NewHandler(messageService)))
	defer messageServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/api/ws/route", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-User-ID", "receiver")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var routeEnvelope struct {
		Code int `json:"code"`
		Data struct {
			WSURL string `json:"ws_url"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&routeEnvelope); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if routeEnvelope.Code != 0 {
		t.Fatalf("route failed: %#v", routeEnvelope)
	}

	parsed, err := url.Parse(routeEnvelope.Data.WSURL)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Scheme = "ws"
	parsed.Host = strings.TrimPrefix(wsServer.URL, "http://")
	query := parsed.Query()
	query.Set("conn_id", "conn-e2e")
	parsed.RawQuery = query.Encode()
	config, err := xwebsocket.NewConfig(parsed.String(), "http://localhost")
	if err != nil {
		t.Fatal(err)
	}
	client, err := xwebsocket.DialConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_ = client.SetDeadline(time.Now().Add(3 * time.Second))

	messageBody := []byte(`{"from_user_id":"sender","to_user_id":"receiver","content":{"text":"hello"}}`)
	messageResp, err := http.Post(messageServer.URL+"/api/messages/single", "application/json", bytes.NewReader(messageBody))
	if err != nil {
		t.Fatal(err)
	}
	if messageResp.StatusCode >= 300 {
		t.Fatalf("message status: %s", messageResp.Status)
	}
	_ = messageResp.Body.Close()

	var raw string
	if err := xwebsocket.Message.Receive(client, &raw); err != nil {
		t.Fatal(err)
	}
	var pushedEnvelope struct {
		C    string          `json:"c"`
		M    string          `json:"m"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(raw), &pushedEnvelope); err != nil {
		t.Fatal(err)
	}
	if pushedEnvelope.C != "socket_server" || pushedEnvelope.M != "message" {
		t.Fatalf("unexpected pushed envelope: %s", raw)
	}

	var pushed struct {
		MessageID string `json:"message_id"`
		Payload   struct {
			Text string `json:"text"`
		} `json:"payload"`
		AckRequired bool     `json:"ack_required"`
		ConnIDs     []string `json:"conn_ids"`
	}
	if err := json.Unmarshal(pushedEnvelope.Data, &pushed); err != nil {
		t.Fatal(err)
	}
	if pushed.MessageID == "" || pushed.Payload.Text != "hello" || !pushed.AckRequired {
		t.Fatalf("unexpected pushed message: %s", raw)
	}
	if len(pushed.ConnIDs) != 0 || strings.Contains(raw, "conn-e2e") {
		t.Fatalf("internal route leaked to client: %s", raw)
	}
	ackBody, _ := json.Marshal(map[string]any{
		"c":    "socket_server",
		"m":    "ack",
		"data": map[string]string{"message_id": pushed.MessageID},
	})
	if err := xwebsocket.Message.Send(client, string(ackBody)); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		done, err := ackStore.GetDone(ctx, pushed.MessageID)
		if err != nil {
			t.Fatal(err)
		}
		if len(done) == 1 && done[0] == "conn-e2e" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("client ACK was not recorded")
}
