package test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yinjiaming666/wskit"

	wsnode "socket_server/internal/websocket"
)

func TestHealthEndpointsBeforeNodeRuns(t *testing.T) {
	backend := wskit.NewRedisBackend(wskit.RedisConfig{})
	defer backend.Close()
	node, err := wskit.CreateNode(wskit.NodeConfig{
		EndpointID: "node-health", GRPCAddr: "127.0.0.1:0",
	}, backend)
	if err != nil {
		t.Fatal(err)
	}
	mux := wsnode.NewMux(node, nil, "node-health", nil, func(context.Context) error {
		t.Fatal("unstarted node must fail readiness before checking Redis")
		return nil
	})
	for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != want {
			t.Errorf("%s status = %d, want %d; body=%s", path, response.Code, want, response.Body.String())
		}
	}
}
