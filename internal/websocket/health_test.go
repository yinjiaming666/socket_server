package websocket

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"socket_server/internal/ack"
	"socket_server/internal/endpoint"
	"socket_server/internal/gateway"
	"socket_server/internal/route"
)

func TestHealthEndpointsReflectDrainState(t *testing.T) {
	// 健康测试使用内存依赖，避免部署门禁测试依赖外部 Redis。
	node := NewNode(
		NodeConfig{EndpointID: "node-health", MaxConn: 10, SendBuffer: 8},
		endpoint.NewMemoryRepository(),
		route.NewMemoryResolver(),
		ack.NewMemoryStore(),
		gateway.NewRouteTokenManager("test-secret", time.Minute),
	)
	var dependencyErr error
	handler := NewHandler(node)
	handler.SetReadinessCheck(func(context.Context) error {
		return dependencyErr
	})
	mux := NewMux(handler)

	assertStatus := func(path string, want int) {
		t.Helper()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("%s status = %d, want %d; body=%s", path, response.Code, want, response.Body.String())
		}
	}

	assertStatus("/readyz", http.StatusOK)
	assertStatus("/healthz", http.StatusOK)

	// Redis 等依赖异常时只退出就绪，进程存活探针仍保持成功。
	dependencyErr = errors.New("redis unavailable")
	assertStatus("/readyz", http.StatusServiceUnavailable)
	assertStatus("/healthz", http.StatusOK)
	dependencyErr = nil

	// 排空后节点仍存活，但不再允许负载均衡或部署工具分配新连接。
	if err := node.BeginDrain(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertStatus("/readyz", http.StatusServiceUnavailable)
	assertStatus("/healthz", http.StatusOK)
}
