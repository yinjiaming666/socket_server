package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"socket_server/internal/domain"
	"socket_server/internal/httpx"
)

const maxWebSocketJSONBodyBytes int64 = 1 << 20

// Handler 提供 WebSocket Node 的 HTTP 和 WebSocket 入口。
type Handler struct {
	node *Node
	// readinessCheck 验证 Redis 等关键依赖是否可用。
	readinessCheck func(context.Context) error
}

// NewHandler 创建 WebSocket Node HTTP 处理器。
func NewHandler(node *Node) *Handler {
	return &Handler{node: node}
}

// SetReadinessCheck 设置发布和负载均衡使用的依赖就绪检查。
func (h *Handler) SetReadinessCheck(check func(context.Context) error) {
	h.readinessCheck = check
}

// NewMux 创建 WebSocket Node HTTP 路由。
func NewMux(handler *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	return mux
}

// RegisterRoutes 注册 WebSocket Node 相关路由。
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", h.HandleHealth)
	mux.HandleFunc("/readyz", h.HandleReady)
	mux.HandleFunc("/ws/", h.HandleConnect)
	mux.HandleFunc("/api/ws-node/push-conns", h.HandlePushConns)
	mux.HandleFunc("/api/ws-node/push-users", h.HandlePushUsers)
	mux.HandleFunc("/api/ws-node/join-room", h.HandleJoinRoom)
	mux.HandleFunc("/api/ws-node/leave-room", h.HandleLeaveRoom)
	mux.HandleFunc("/api/ws-node/ack", h.HandleAck)
	mux.HandleFunc("/api/ws-node/stats", h.HandleStats)
}

type RoomRequest struct {
	ConnID string `json:"conn_id"`
	RoomID string `json:"room_id"`
}

type AckRequest struct {
	MessageID string `json:"message_id"`
	ConnID    string `json:"conn_id"`
}

type PushRequest struct {
	ConnIDs []string        `json:"conn_ids"`
	UserIDs []string        `json:"user_ids"`
	Message *domain.Message `json:"message"`
}

type ClientMessage struct {
	Type      string `json:"type"`
	MessageID string `json:"message_id"`
	RoomID    string `json:"room_id"`
}

type StatsResponse struct {
	ConnCount int64 `json:"conn_count"`
}

// HealthResponse 表示部署健康检查需要的节点状态。
type HealthResponse struct {
	Status    string `json:"status"`
	Ready     bool   `json:"ready"`
	ConnCount int64  `json:"conn_count"`
}

// HandleHealth 返回进程存活状态；draining 节点仍然存活，因此保持 HTTP 200。
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	httpx.WriteOK(w, HealthResponse{Status: "ok", Ready: h.node.Ready(), ConnCount: h.node.ConnCount()})
}

// HandleReady 返回节点能否接收新连接，部署健康门禁应使用该接口。
func (h *Handler) HandleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !h.node.Ready() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "node is draining")
		return
	}
	if h.readinessCheck != nil {
		if err := h.readinessCheck(r.Context()); err != nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "websocket dependency unavailable")
			return
		}
	}
	httpx.WriteOK(w, HealthResponse{Status: "ready", Ready: true, ConnCount: h.node.ConnCount()})
}

// HandleConnect 处理真实 WebSocket Upgrade 连接注册请求。
func (h *Handler) HandleConnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		httpx.WriteError(w, http.StatusBadRequest, "websocket upgrade required")
		return
	}
	connID := r.URL.Query().Get("conn_id")
	routeToken := r.URL.Query().Get("route_token")
	deviceID := r.URL.Query().Get("device_id")
	platform := r.URL.Query().Get("platform")

	client, err := h.node.RegisterConnection(r.Context(), connID, routeToken, deviceID, platform)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.serveWebSocket(w, r, client)
}

// serveWebSocket 完成 WebSocket 升级并启动读写循环。
func (h *Handler) serveWebSocket(w http.ResponseWriter, r *http.Request, client *Client) {
	conn, err := upgradeWebSocket(w, r)
	if err != nil {
		_ = h.node.RemoveClientConnection(r.Context(), client)
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	defer conn.close()
	defer h.node.RemoveClientConnection(r.Context(), client)

	client.AttachWriter(conn.writeText)
	done := make(chan struct{})
	go h.writeLoop(conn, client, done)

	for {
		payload, opcode, err := conn.readMessage()
		if err != nil {
			return
		}
		switch opcode {
		case 0x8:
			_ = conn.writeClose()
			return
		case 0x9:
			_ = conn.writePong(payload)
		case 0xA:
			continue
		case 0x1, 0x2:
			if err := h.handleClientMessage(r, client, payload); err != nil {
				_ = conn.writeText([]byte(`{"type":"error"}`))
			}
		}
		select {
		case <-done:
			return
		default:
		}
	}
}

// writeLoop 写消息并定期发送服务端 ping。
func (h *Handler) writeLoop(conn *wsConn, client *Client, done chan<- struct{}) {
	defer close(done)
	defer conn.close()
	defer client.Close()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-client.Done():
			return
		case payload := <-client.Send:
			if err := client.WriteDirect(payload); err != nil {
				return
			}
		case <-ticker.C:
			if err := conn.writePing([]byte("ping")); err != nil {
				return
			}
		}
	}
}

// handleClientMessage 处理客户端通过 WebSocket 上报的控制消息。
func (h *Handler) handleClientMessage(r *http.Request, client *Client, payload []byte) error {
	var msg ClientMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return err
	}
	switch msg.Type {
	case "ack":
		return h.node.HandleClientAck(r.Context(), msg.MessageID, client.ConnID)
	case "join_room":
		return h.node.JoinRoom(r.Context(), client.ConnID, msg.RoomID)
	case "leave_room":
		return h.node.LeaveRoom(r.Context(), client.ConnID, msg.RoomID)
	case "ping":
		return client.WriteDirect([]byte(`{"type":"pong"}`))
	default:
		return errors.New("unsupported client message type")
	}
}

func (h *Handler) HandlePushConns(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req PushRequest
	if err := decodeNodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := h.node.PushToConns(r.Context(), req.ConnIDs, req.Message)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteOK(w, resp)
}

func (h *Handler) HandlePushUsers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req PushRequest
	if err := decodeNodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	resp, err := h.node.PushToUsers(r.Context(), req.UserIDs, req.Message)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteOK(w, resp)
}

func (h *Handler) HandleJoinRoom(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req RoomRequest
	if err := decodeNodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.node.JoinRoom(r.Context(), req.ConnID, req.RoomID); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteOK(w, map[string]string{"status": "joined"})
}

func (h *Handler) HandleLeaveRoom(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req RoomRequest
	if err := decodeNodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.node.LeaveRoom(r.Context(), req.ConnID, req.RoomID); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteOK(w, map[string]string{"status": "left"})
}

func (h *Handler) HandleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req AckRequest
	if err := decodeNodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.node.HandleClientAck(r.Context(), req.MessageID, req.ConnID); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteOK(w, map[string]string{"status": "ack"})
}

func (h *Handler) HandleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	httpx.WriteOK(w, StatsResponse{ConnCount: h.node.ConnCount()})
}

// decodeNodeJSON 限制请求体大小并解析 JSON。
func decodeNodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxWebSocketJSONBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}
