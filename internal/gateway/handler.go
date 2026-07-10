package gateway

import (
	"net/http"
	"strings"

	"socket_server/internal/httpx"
)

// Handler 提供 Go Gateway 的 HTTP 入口。
type Handler struct {
	// selector 表示 endpoint 选择器。
	selector EndpointSelector
	// tokenManager 表示路由凭证管理器。
	tokenManager *RouteTokenManager
	// publicHost 表示客户端访问 WebSocket 使用的公开域名。
	publicHost string
	// routeTokenTTLSeconds 表示 route_token 有效秒数。
	routeTokenTTLSeconds int64
}

// NewHandler 创建 Go Gateway HTTP 处理器。
func NewHandler(selector EndpointSelector, tokenManager *RouteTokenManager, publicHost string, routeTokenTTLSeconds int64) *Handler {
	if publicHost == "" {
		publicHost = "example.com"
	}
	if routeTokenTTLSeconds <= 0 {
		routeTokenTTLSeconds = 30
	}
	return &Handler{selector: selector, tokenManager: tokenManager, publicHost: publicHost, routeTokenTTLSeconds: routeTokenTTLSeconds}
}

// NewMux 创建 Gateway HTTP 路由。
func NewMux(handler *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	return mux
}

// RegisterRoutes 注册 Gateway 路由。
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/ws/route", h.HandleRoute)
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

// HandleRoute 处理客户端获取 WebSocket endpoint 的请求。
func (h *Handler) HandleRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	userID := h.resolveUserID(r)
	if userID == "" {
		httpx.WriteError(w, http.StatusUnauthorized, "user_id is empty")
		return
	}

	ep, err := h.selector.SelectEndpoint(r.Context(), userID)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	token, err := h.tokenManager.Generate(userID, ep.ID)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	wsURL := "wss://" + h.publicHost + ep.WSPath + "?route_token=" + token
	httpx.WriteOK(w, RouteResponse{EndpointID: ep.ID, WSURL: wsURL, RouteToken: token, ExpiresIn: h.routeTokenTTLSeconds})
}

// resolveUserID 从请求中解析用户编号，正式环境应替换为 token 校验结果。
func (h *Handler) resolveUserID(r *http.Request) string {
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
