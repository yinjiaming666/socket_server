package test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"socket_server/internal/gateway"
)

func TestGatewayReadyChecksDependenciesWhileHealthStaysLive(t *testing.T) {
	dependencyErr := errors.New("redis unavailable")
	// readiness 使用可替换检查器，测试无需连接真实 Redis。
	mux := gateway.NewMux(nil, nil, func(context.Context) error {
		return dependencyErr
	})

	assertStatus := func(method, path string, want int) {
		t.Helper()
		request := httptest.NewRequest(method, path, nil)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("%s %s status = %d, want %d; body=%s", method, path, response.Code, want, response.Body.String())
		}
		if want == http.StatusMethodNotAllowed &&
			(response.Header().Get("Content-Type") != "application/json; charset=utf-8" ||
				response.Body.String() != "{\"code\":405,\"message\":\"method not allowed\"}\n") {
			t.Fatalf("unexpected method error response: %s", response.Body.String())
		}
	}

	assertStatus(http.MethodGet, "/healthz", http.StatusOK)
	assertStatus(http.MethodGet, "/readyz", http.StatusServiceUnavailable)
	assertStatus(http.MethodHead, "/healthz", http.StatusMethodNotAllowed)
	assertStatus(http.MethodPost, "/api/ws/route", http.StatusMethodNotAllowed)
	dependencyErr = nil
	assertStatus(http.MethodGet, "/readyz", http.StatusOK)
}
