package httpx

import (
	"context"
	"net/http"
)

// HandleMethod 注册路由，并用统一的 JSON 格式拒绝其他 HTTP 方法。
func HandleMethod(mux *http.ServeMux, path, method string, handler http.HandlerFunc) {
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handler(w, r)
	})
}

// RegisterProbeRoutes 注册进程存活和依赖就绪检查。
func RegisterProbeRoutes(mux *http.ServeMux, check func(context.Context) error, unavailable string) {
	HandleMethod(mux, "/healthz", http.MethodGet, func(w http.ResponseWriter, _ *http.Request) {
		WriteOK(w, map[string]string{"status": "ok"})
	})
	HandleMethod(mux, "/readyz", http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
		if check != nil && check(r.Context()) != nil {
			WriteError(w, http.StatusServiceUnavailable, unavailable)
			return
		}
		WriteOK(w, map[string]string{"status": "ready"})
	})
}
