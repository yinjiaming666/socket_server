package websocket

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/yinjiaming666/wskit"

	"socket_server/internal/domain"
	"socket_server/internal/gateway"
	"socket_server/internal/httpx"
)

type handler struct {
	node           *wskit.Node
	tokenManager   *gateway.RouteTokenManager
	endpointID     string
	readinessCheck func(context.Context) error
}

// NewMux 创建 WebSocket Node 的 HTTP 和 WebSocket 路由。
func NewMux(node *wskit.Node, tokens *gateway.RouteTokenManager, endpointID string, ready func(context.Context) error) *http.ServeMux {
	h := &handler{node: node, tokenManager: tokens, endpointID: endpointID, readinessCheck: ready}
	mux := http.NewServeMux()
	httpx.HandleMethod(mux, "/healthz", http.MethodGet, h.handleHealth)
	httpx.HandleMethod(mux, "/readyz", http.MethodGet, h.handleReady)
	httpx.HandleMethod(mux, "/ws/", http.MethodGet, h.handleConnect)
	httpx.HandleMethod(mux, "/api/ws-node/push-conns", http.MethodPost, h.handlePush(false))
	httpx.HandleMethod(mux, "/api/ws-node/push-users", http.MethodPost, h.handlePush(true))
	httpx.HandleMethod(mux, "/api/ws-node/join-room", http.MethodPost, handleRoom(node.Subscribe, "joined"))
	httpx.HandleMethod(mux, "/api/ws-node/leave-room", http.MethodPost, handleRoom(node.Unsubscribe, "left"))
	httpx.HandleMethod(mux, "/api/ws-node/ack", http.MethodPost, h.handleAck)
	httpx.HandleMethod(mux, "/api/ws-node/stats", http.MethodGet, h.handleStats)
	return mux
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

type StatsResponse struct {
	ConnCount int64 `json:"conn_count"`
}

// HealthResponse 表示部署健康检查需要的节点状态。
type HealthResponse struct {
	Status    string `json:"status"`
	Ready     bool   `json:"ready"`
	ConnCount int64  `json:"conn_count"`
}

// handleHealth 返回进程存活状态；draining 节点仍然存活，因此保持 HTTP 200。
func (h *handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteOK(w, HealthResponse{Status: "ok", Ready: h.node.Ready(), ConnCount: h.node.ConnCount()})
}

// handleReady 返回节点能否接收新连接，部署健康门禁应使用该接口。
func (h *handler) handleReady(w http.ResponseWriter, r *http.Request) {
	if !h.node.Ready() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "node is draining")
		return
	}
	if h.readinessCheck != nil && h.readinessCheck(r.Context()) != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, "websocket dependency unavailable")
		return
	}
	httpx.WriteOK(w, HealthResponse{Status: "ready", Ready: true, ConnCount: h.node.ConnCount()})
}

// handleConnect 处理真实 WebSocket Upgrade 连接注册请求。
func (h *handler) handleConnect(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		httpx.WriteError(w, http.StatusBadRequest, "websocket upgrade required")
		return
	}
	query := r.URL.Query()
	connID, token := query.Get("conn_id"), query.Get("route_token")
	if connID == "" || token == "" {
		httpx.WriteError(w, http.StatusBadRequest, "conn_id or route_token is empty")
		return
	}
	claims, err := h.tokenManager.Verify(token)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if claims.EndpointID != h.endpointID {
		httpx.WriteError(w, http.StatusBadRequest, "route token endpoint mismatch")
		return
	}
	if claims.UserID == "" || len(claims.UserID) > 128 {
		httpx.WriteError(w, http.StatusBadRequest, "route token user is invalid")
		return
	}
	h.node.ServeWS(w, r, wskit.SessionInfo{
		ConnID: connID, UserID: claims.UserID,
		DeviceID: query.Get("device_id"), Platform: query.Get("platform"),
	})
}

type clientErrorData struct {
	Message string `json:"message"`
}

func HandleClientFrame(ctx context.Context, session *wskit.Session, frame wskit.Frame) error {
	if err := handleClientMessage(ctx, session, frame.Data); err != nil {
		return sendControl(session, "error", clientErrorData{Message: err.Error()})
	}
	return nil
}

func handleClientMessage(ctx context.Context, session *wskit.Session, payload []byte) error {
	envelope, err := domain.DecodeClientEnvelope(payload)
	if err != nil {
		return err
	}

	switch envelope.M {
	case "ack":
		var data domain.ClientAckData
		if err := domain.DecodeClientData(envelope.Data, &data); err != nil {
			return err
		}
		return session.Ack(ctx, data.MessageID)
	case "join_room", "leave_room":
		var data domain.ClientRoomData
		if err := domain.DecodeClientData(envelope.Data, &data); err != nil {
			return err
		}
		if envelope.M == "join_room" {
			return session.Subscribe(ctx, data.RoomID)
		}
		return session.Unsubscribe(ctx, data.RoomID)
	case "ping":
		var data struct{}
		if err := domain.DecodeClientData(envelope.Data, &data); err != nil {
			return err
		}
		return sendControl(session, "pong", struct{}{})
	default:
		return errors.New("unsupported websocket method")
	}
}

func sendControl(session *wskit.Session, method string, data any) error {
	payload, err := domain.MarshalClientControl(method, data)
	if err != nil {
		return err
	}
	return session.Send(wskit.Frame{Type: wskit.TextFrame, Data: payload})
}

func (h *handler) handlePush(byUser bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req PushRequest
		if err := httpx.DecodeJSON(w, r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		frame, err := domain.EncodeMessageFrame(req.Message)
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		var resp *wskit.DeliveryResult
		if byUser {
			resp, err = h.node.PushToUsers(r.Context(), req.UserIDs, frame)
		} else {
			resp, err = h.node.PushToConns(r.Context(), req.ConnIDs, frame)
		}
		if err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httpx.WriteOK(w, resp)
	}
}

func handleRoom(update func(context.Context, string, string) error, status string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req RoomRequest
		if err := httpx.DecodeJSON(w, r, &req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := update(r.Context(), req.ConnID, req.RoomID); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, err.Error())
			return
		}
		httpx.WriteOK(w, map[string]string{"status": status})
	}
}

func (h *handler) handleAck(w http.ResponseWriter, r *http.Request) {
	var req AckRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.node.Ack(r.Context(), req.ConnID, req.MessageID); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteOK(w, map[string]string{"status": "ack"})
}

func (h *handler) handleStats(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteOK(w, StatsResponse{ConnCount: h.node.ConnCount()})
}
