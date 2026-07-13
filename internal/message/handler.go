package message

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"socket_server/internal/domain"
	"socket_server/internal/httpx"
)

const maxJSONBodyBytes int64 = 1 << 20

// Handler 提供 Message API 的 HTTP 入口。
type Handler struct {
	// service 表示消息业务服务。
	service *Service
	// readinessCheck 验证 Redis 等关键依赖是否可用。
	readinessCheck func(context.Context) error
}

// NewHandler 创建 Message API HTTP 处理器。
func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// SetReadinessCheck 设置发布和负载均衡使用的依赖就绪检查。
func (h *Handler) SetReadinessCheck(check func(context.Context) error) {
	h.readinessCheck = check
}

// NewMux 创建 Message API HTTP 路由。
func NewMux(handler *Handler) *http.ServeMux {
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)
	return mux
}

// RegisterRoutes 注册 Message API 路由。
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", h.HandleHealth)
	mux.HandleFunc("/readyz", h.HandleReady)
	mux.HandleFunc("/api/messages/single", h.HandleSingleMessage)
	mux.HandleFunc("/api/messages/group", h.HandleGroupMessage)
	mux.HandleFunc("/api/messages/ack", h.HandleClientAck)
	mux.HandleFunc("/api/live/rooms/", h.HandleLiveRoomMessage)
}

// HandleHealth 返回 Message API 进程健康状态，供负载均衡和自动发布门禁使用。
func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	httpx.WriteOK(w, map[string]string{"status": "ok"})
}

// HandleReady 在关键依赖可用时返回就绪状态。
func (h *Handler) HandleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if h.readinessCheck != nil {
		if err := h.readinessCheck(r.Context()); err != nil {
			httpx.WriteError(w, http.StatusServiceUnavailable, "message dependency unavailable")
			return
		}
	}
	httpx.WriteOK(w, map[string]string{"status": "ready"})
}

// SingleMessageRequest 表示单聊消息请求。
type SingleMessageRequest struct {
	MessageID  string          `json:"message_id"`
	FromUserID string          `json:"from_user_id"`
	ToUserID   string          `json:"to_user_id"`
	Content    json.RawMessage `json:"content"`
}

// GroupMessageRequest 表示普通群聊消息请求。
type GroupMessageRequest struct {
	MessageID     string          `json:"message_id"`
	FromUserID    string          `json:"from_user_id"`
	GroupID       string          `json:"group_id"`
	MemberUserIDs []string        `json:"member_user_ids"`
	Content       json.RawMessage `json:"content"`
}

// LiveMessageRequest 表示直播间消息请求。
type LiveMessageRequest struct {
	MessageID  string          `json:"message_id"`
	FromUserID string          `json:"from_user_id"`
	Type       string          `json:"type"`
	Content    json.RawMessage `json:"content"`
}

// ClientAckRequest 表示客户端 ACK 请求。
type ClientAckRequest struct {
	MessageID string `json:"message_id"`
	UserID    string `json:"user_id"`
	ConnID    string `json:"conn_id"`
	AckType   string `json:"ack_type"`
}

// AcceptedResponse 表示消息接口 accepted 响应。
type AcceptedResponse struct {
	MessageID       string   `json:"message_id"`
	Status          string   `json:"status"`
	Accepted        bool     `json:"accepted"`
	AcceptedConnIDs []string `json:"accepted_conn_ids,omitempty"`
	FailedConnIDs   []string `json:"failed_conn_ids,omitempty"`
	Reason          string   `json:"reason,omitempty"`
}

// HandleSingleMessage 处理单聊消息发送请求。
func (h *Handler) HandleSingleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req SingleMessageRequest
	if err := decodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	msg := &domain.Message{MessageID: req.MessageID, FromUserID: req.FromUserID, ToUserIDs: []string{req.ToUserID}, Payload: req.Content}
	result, err := h.service.SendSingleMessage(r.Context(), msg)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	httpx.WriteOK(w, acceptedResponse(msg.MessageID, result))
}

// HandleGroupMessage 处理普通群聊消息发送请求。
func (h *Handler) HandleGroupMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req GroupMessageRequest
	if err := decodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	msg := &domain.Message{MessageID: req.MessageID, FromUserID: req.FromUserID, GroupID: req.GroupID, Payload: req.Content}
	result, err := h.service.SendGroupMessage(r.Context(), msg, req.MemberUserIDs)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	httpx.WriteOK(w, acceptedResponse(msg.MessageID, result))
}

// HandleLiveRoomMessage 处理直播间消息发送请求。
func (h *Handler) HandleLiveRoomMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	roomID := parseRoomID(r.URL.Path)
	if roomID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "room_id is empty")
		return
	}

	var req LiveMessageRequest
	if err := decodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	msg := &domain.Message{MessageID: req.MessageID, FromUserID: req.FromUserID, RoomID: roomID, Payload: req.Content}

	var result *domain.SendResult
	var err error
	if req.Type == "notice" {
		result, err = h.service.SendLiveNotice(r.Context(), msg)
	} else {
		result, err = h.service.SendLiveDanmu(r.Context(), msg)
	}
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	httpx.WriteOK(w, acceptedResponse(msg.MessageID, result))
}

// HandleClientAck 处理客户端 ACK 上报请求。
func (h *Handler) HandleClientAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		httpx.WriteError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req ClientAckRequest
	if err := decodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := h.service.MarkClientAck(r.Context(), req.MessageID, req.UserID, req.ConnID, req.AckType); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	httpx.WriteOK(w, map[string]string{"status": "ok"})
}

func acceptedResponse(messageID string, result *domain.SendResult) AcceptedResponse {
	status := "accepted"
	if !result.Accepted && len(result.AcceptedConnIDs) > 0 {
		status = "partial"
	} else if !result.Accepted {
		status = "failed"
	}
	return AcceptedResponse{
		MessageID: messageID, Status: status, Accepted: result.Accepted,
		AcceptedConnIDs: result.AcceptedConnIDs, FailedConnIDs: result.FailedConnIDs,
		Reason: result.Reason,
	}
}

// decodeJSON 限制请求体大小并解析 JSON。
func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// parseRoomID 从 /api/live/rooms/{room_id}/messages 中解析 room_id。
func parseRoomID(path string) string {
	prefix := "/api/live/rooms/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	parts := strings.Split(rest, "/")
	if len(parts) < 2 || parts[1] != "messages" {
		return ""
	}
	return parts[0]
}
