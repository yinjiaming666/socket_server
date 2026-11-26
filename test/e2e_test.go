package test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yinjiaming666/wskit"
	xwebsocket "golang.org/x/net/websocket"

	"socket_server/internal/ack"
	"socket_server/internal/config"
	"socket_server/internal/domain"
	"socket_server/internal/gateway"
	"socket_server/internal/host"
	"socket_server/internal/message"
	wsnode "socket_server/internal/websocket"
)

func TestEndToEndRoutePushAndAck(t *testing.T) {
	redisAddr := os.Getenv("WS_TEST_REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("set WS_TEST_REDIS_ADDR to run Redis/gRPC integration")
	}
	cfg := config.RedisConfig{Addr: redisAddr, KeyPrefix: "socket-server-test:" + rand.Text() + ":"}
	backend := host.NewBackend(cfg)
	defer backend.Close()
	sender, err := wskit.NewSender(backend)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	notices := message.NewNoticeStore(cfg)
	defer notices.Close()
	acks := ack.NewStore(cfg)
	defer acks.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer redisClient.Close()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		keys := redisClient.Scan(ctx, 0, cfg.KeyPrefix+"*", 100).Iterator()
		var testKeys []string
		for keys.Next(ctx) {
			testKeys = append(testKeys, keys.Val())
		}
		if err := keys.Err(); err != nil {
			t.Error(err)
		}
		if len(testKeys) != 0 {
			if err := redisClient.Del(ctx, testKeys...).Err(); err != nil {
				t.Error(err)
			}
		}
	}()

	const nodeID = "node-e2e"
	const wsPath = "/ws/" + nodeID
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	node, err := wsnode.NewNode(wskit.NodeConfig{
		EndpointID: nodeID, WSPath: wsPath, GRPCAddr: "127.0.0.1:0",
	}, backend, acks)
	if err != nil {
		t.Fatal(err)
	}
	nodeCtx, stop := context.WithCancel(t.Context())
	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		runErr = node.Run(nodeCtx)
	}()
	defer func() {
		stop()
		<-done
		if runErr != nil {
			t.Errorf("node run: %v", runErr)
		}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !node.Ready() {
		select {
		case <-done:
			t.Fatalf("node exited before becoming ready: %v", runErr)
		case <-ctx.Done():
			t.Fatal("node did not become ready")
		case <-ticker.C:
		}
	}

	tokens := gateway.NewRouteTokenManager("e2e-secret", time.Minute)
	wsServer := httptest.NewServer(wsnode.NewMux(node, tokens, nodeID, acks, backend.Ready))
	defer wsServer.Close()

	router, err := wskit.CreateGateway(wskit.GatewayConfig{
		PublicURL: "ws://" + strings.TrimPrefix(wsServer.URL, "http://"),
	}, backend)
	if err != nil {
		t.Fatal(err)
	}
	gatewayServer := httptest.NewServer(gateway.NewMux(router, tokens, backend.Ready))
	defer gatewayServer.Close()

	service := message.NewService(sender, notices)
	messageServer := httptest.NewServer(message.NewMux(service, backend.Ready))
	defer messageServer.Close()

	httpClient := &http.Client{Timeout: 3 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, gatewayServer.URL+"/api/ws/route", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-User-ID", "receiver")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var routeEnvelope struct {
		Code int                   `json:"code"`
		Data gateway.RouteResponse `json:"data"`
	}
	decodeErr := json.NewDecoder(resp.Body).Decode(&routeEnvelope)
	resp.Body.Close()
	if decodeErr != nil || resp.StatusCode != http.StatusOK || routeEnvelope.Code != 0 ||
		routeEnvelope.Data.EndpointID != nodeID || routeEnvelope.Data.RouteToken == "" || routeEnvelope.Data.ExpiresIn != 60 {
		t.Fatalf("route failed: status=%s envelope=%+v error=%v", resp.Status, routeEnvelope, decodeErr)
	}
	parsed, err := url.Parse(routeEnvelope.Data.WSURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	if query.Get("route_token") != routeEnvelope.Data.RouteToken {
		t.Fatal("WebSocket URL is missing the issued route token")
	}
	const connID = "conn-e2e"
	query.Set("conn_id", connID)
	parsed.RawQuery = query.Encode()
	wsConfig, err := xwebsocket.NewConfig(parsed.String(), "http://localhost")
	if err != nil {
		t.Fatal(err)
	}
	client, err := wsConfig.DialContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	post := func(server *httptest.Server, path, payload string) message.AcceptedResponse {
		t.Helper()
		resp, err := httpClient.Post(server.URL+path, "application/json", strings.NewReader(payload))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var envelope struct {
			Code    int                      `json:"code"`
			Message string                   `json:"message"`
			Data    message.AcceptedResponse `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK || envelope.Code != 0 {
			t.Fatalf("%s: %s (%d) %s", path, resp.Status, envelope.Code, envelope.Message)
		}
		return envelope.Data
	}

	sendControl := func(method string, data any) {
		t.Helper()
		body, err := domain.MarshalClientControl(method, data)
		if err != nil {
			t.Fatal(err)
		}
		if err := xwebsocket.Message.Send(client, string(body)); err != nil {
			t.Fatal(err)
		}
	}
	receive := func(method string) json.RawMessage {
		t.Helper()
		var raw string
		if err := xwebsocket.Message.Receive(client, &raw); err != nil {
			t.Fatal(err)
		}
		envelope, err := domain.DecodeClientEnvelope([]byte(raw))
		if err != nil || envelope.M != method || strings.Contains(raw, connID) {
			t.Fatalf("unexpected envelope for %s: %s (%v)", method, raw, err)
		}
		return envelope.Data
	}

	assertReceipt := func(msgID, connectionID, want string) {
		t.Helper()
		got, err := acks.State(ctx, msgID, connectionID)
		if err != nil || got != want {
			t.Fatalf("receipt for %q on %q = %q, %v; want %q", msgID, connectionID, got, err, want)
		}
	}

	sendControl("ping", struct{}{})
	receive("pong")
	sendControl("ack", domain.ClientAckData{})
	receive("error")

	single := post(messageServer, "/api/messages/single", `{"from_user_id":"sender","to_user_id":"receiver","content":{"text":"hello"}}`)
	var pushed domain.ClientMessageData
	if err := json.Unmarshal(receive("message"), &pushed); err != nil {
		t.Fatal(err)
	}
	if !single.Accepted || !slices.Equal(single.AcceptedConnIDs, []string{connID}) ||
		pushed.MessageID == "" || pushed.MessageID != single.MessageID || pushed.BizType != "single" ||
		string(pushed.Payload) != `{"text":"hello"}` || !pushed.AckRequired {
		t.Fatalf("unexpected single delivery: %+v, %+v", single, pushed)
	}
	assertReceipt(single.MessageID, connID, "pending")
	sendControl("ack", domain.ClientAckData{MessageID: pushed.MessageID})
	sendControl("ping", struct{}{})
	if data := receive("pong"); string(data) != "{}" {
		t.Fatalf("unexpected pong: %s", data)
	}
	assertReceipt(single.MessageID, connID, "done")

	group := post(messageServer, "/api/messages/group", `{"message_id":"group-e2e","group_id":"g1","member_user_ids":["receiver","receiver","offline"],"content":"group"}`)
	if !group.Accepted || !slices.Equal(group.AcceptedConnIDs, []string{connID}) {
		t.Fatalf("unexpected group delivery: %+v", group)
	}
	pushed = domain.ClientMessageData{}
	if err := json.Unmarshal(receive("message"), &pushed); err != nil || pushed.GroupID != "g1" || pushed.MessageID != group.MessageID {
		t.Fatalf("group frame: %+v, error=%v", pushed, err)
	}
	assertReceipt(group.MessageID, connID, "pending")
	receipt := post(wsServer, "/api/ws-node/ack", `{"message_id":"group-e2e","conn_id":"conn-e2e"}`)
	if receipt.Status != "ack" {
		t.Fatalf("unexpected HTTP receipt response: %+v", receipt)
	}
	assertReceipt(group.MessageID, connID, "done")

	post(wsServer, "/api/ws-node/push-conns", `{"conn_ids":["conn-e2e","missing"],"message":{"MessageID":"local-e2e","AckRequired":true}}`)
	if err := json.Unmarshal(receive("message"), &pushed); err != nil || pushed.MessageID != "local-e2e" || !pushed.AckRequired {
		t.Fatalf("local push frame: %+v, error=%v", pushed, err)
	}
	assertReceipt("local-e2e", connID, "pending")
	assertReceipt("local-e2e", "missing", "")
	post(wsServer, "/api/ws-node/ack", `{"message_id":"local-e2e","conn_id":"conn-e2e"}`)
	assertReceipt("local-e2e", connID, "done")

	sendControl("join_room", domain.ClientRoomData{RoomID: "room-e2e"})
	sendControl("ping", struct{}{})
	receive("pong")
	notice := post(messageServer, "/api/live/rooms/room-e2e/messages", `{"type":"notice","content":"notice"}`)
	pushed = domain.ClientMessageData{}
	if err := json.Unmarshal(receive("message"), &pushed); err != nil || pushed.BizType != "live_notice" ||
		pushed.RoomID != "room-e2e" || pushed.MessageID != notice.MessageID || pushed.AckRequired {
		t.Fatalf("notice frame: %+v, error=%v", pushed, err)
	}
	danmu := post(messageServer, "/api/live/rooms/room-e2e/messages", `{"content":"danmu"}`)
	pushed = domain.ClientMessageData{}
	if err := json.Unmarshal(receive("message"), &pushed); err != nil || pushed.BizType != "live_danmu" ||
		pushed.RoomID != "room-e2e" || pushed.MessageID != danmu.MessageID || pushed.AckRequired {
		t.Fatalf("danmu frame: %+v, error=%v", pushed, err)
	}
	assertReceipt(notice.MessageID, connID, "")
	assertReceipt(danmu.MessageID, connID, "")
	if !strings.HasPrefix(notice.MessageID, "live-notice-") || !strings.HasPrefix(danmu.MessageID, "live-danmu-") {
		t.Fatalf("unexpected live message IDs: notice=%q, danmu=%q", notice.MessageID, danmu.MessageID)
	}

	entries, err := redisClient.XRange(ctx, cfg.KeyPrefix+"stream:live:room:room-e2e", "-", "+").Result()
	if err != nil || len(entries) != 1 || entries[0].Values["message_id"] != notice.MessageID {
		t.Fatalf("notice stream: got %v, error=%v", entries, err)
	}

	if err := node.BeginDrain(ctx); err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable} {
		resp, err := httpClient.Get(wsServer.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("%s status = %s, want %d", path, resp.Status, want)
		}
	}
}
