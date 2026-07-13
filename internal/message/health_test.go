package message

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReadyChecksDependenciesWhileHealthStaysLive(t *testing.T) {
	handler := NewHandler(nil)
	dependencyErr := errors.New("redis unavailable")
	// readiness 使用可替换检查器，测试无需构造消息发送依赖。
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

	assertStatus("/healthz", http.StatusOK)
	assertStatus("/readyz", http.StatusServiceUnavailable)
	dependencyErr = nil
	assertStatus("/readyz", http.StatusOK)
}
