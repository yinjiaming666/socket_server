package gateway

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/yinjiaming666/wskit"

	"socket_server/internal/httpx"
)

type handler struct {
	router       *wskit.Gateway
	tokenManager *RouteTokenManager
}

// NewMux 创建 Gateway HTTP 路由。
func NewMux(router *wskit.Gateway, tokens *RouteTokenManager, ready func(context.Context) error) *http.ServeMux {
	h := &handler{router: router, tokenManager: tokens}
	mux := http.NewServeMux()
	httpx.RegisterProbeRoutes(mux, ready, "gateway dependency unavailable")
	httpx.HandleMethod(mux, "/api/ws/route", http.MethodGet, h.handleRoute)
	return mux
}

// RouteResponse 表示 Gateway 返回给客户端的 WebSocket 路由信息。
type RouteResponse struct {
	// EndpointID 表示分配到的 WebSocket 节点编号。
	EndpointID string `json:"endpoint_id"`
	// WSURL 表示客户端需要连接的 WebSocket 地址。
	WSURL string `json:"ws_url"`
	// RouteToken 表示临时路由凭证。
	RouteToken string `json:"route_token"`
	// ExpiresIn 表示临时路由凭证有效秒数。
	ExpiresIn int64 `json:"expires_in"`
}

// handleRoute 处理客户端获取 WebSocket endpoint 的请求。
func (h *handler) handleRoute(w http.ResponseWriter, r *http.Request) {
	userID := resolveUserID(r)
	if userID == "" {
		httpx.WriteError(w, http.StatusUnauthorized, "user_id is empty")
		return
	}

	route, err := h.router.Route(r.Context(), userID)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	token, err := h.tokenManager.Generate(userID, route.EndpointID)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	wsURL := route.WSURL + "?route_token=" + token
	httpx.WriteOK(w, RouteResponse{EndpointID: route.EndpointID, WSURL: wsURL, RouteToken: token, ExpiresIn: int64(h.tokenManager.ttl / time.Second)})
}

// resolveUserID 从请求中解析用户编号，正式环境应替换为 token 校验结果。
func resolveUserID(r *http.Request) string {
	userID := r.Header.Get("X-User-ID")
	if userID != "" {
		return userID
	}

	authorization := r.Header.Get("Authorization")
	authorization = strings.TrimPrefix(authorization, "Bearer ")
	if authorization != "" {
		return authorization
	}

	return r.URL.Query().Get("user_id")
}
