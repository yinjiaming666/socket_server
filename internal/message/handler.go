package message

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/yinjiaming666/wskit"

	"socket_server/internal/domain"
	"socket_server/internal/httpx"
)

type handler struct {
	service *Service
}

// NewMux 创建 Message API HTTP 路由。
func NewMux(service *Service, ready func(context.Context) error) *http.ServeMux {
	h := &handler{service: service}
	mux := http.NewServeMux()
	httpx.RegisterProbeRoutes(mux, ready, "message dependency unavailable")
	httpx.HandleMethod(mux, "/api/messages/single", http.MethodPost, h.handleSingleMessage)
	httpx.HandleMethod(mux, "/api/messages/group", http.MethodPost, h.handleGroupMessage)
	httpx.HandleMethod(mux, "/api/live/rooms/", http.MethodPost, h.handleLiveRoomMessage)
	return mux
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

// AcceptedResponse 表示消息接口 accepted 响应。
type AcceptedResponse struct {
	MessageID       string   `json:"message_id"`
	Status          string   `json:"status"`
	Accepted        bool     `json:"accepted"`
	AcceptedConnIDs []string `json:"accepted_conn_ids,omitempty"`
	FailedConnIDs   []string `json:"failed_conn_ids,omitempty"`
	Reason          string   `json:"reason,omitempty"`
}

// handleSingleMessage 处理单聊消息发送请求。
func (h *handler) handleSingleMessage(w http.ResponseWriter, r *http.Request) {
	var req SingleMessageRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	msg := &domain.Message{MessageID: req.MessageID, FromUserID: req.FromUserID, ToUserIDs: []string{req.ToUserID}, Payload: req.Content}
	result, err := h.service.SendSingleMessage(r.Context(), msg)
	writeDelivery(w, msg.MessageID, result, err)
}

// handleGroupMessage 处理普通群聊消息发送请求。
func (h *handler) handleGroupMessage(w http.ResponseWriter, r *http.Request) {
	var req GroupMessageRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	msg := &domain.Message{MessageID: req.MessageID, FromUserID: req.FromUserID, GroupID: req.GroupID, Payload: req.Content}
	result, err := h.service.SendGroupMessage(r.Context(), msg, req.MemberUserIDs)
	writeDelivery(w, msg.MessageID, result, err)
}

// handleLiveRoomMessage 处理直播间消息发送请求。
func (h *handler) handleLiveRoomMessage(w http.ResponseWriter, r *http.Request) {
	roomID := parseRoomID(r.URL.Path)
	if roomID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "room_id is empty")
		return
	}

	var req LiveMessageRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	msg := &domain.Message{MessageID: req.MessageID, FromUserID: req.FromUserID, RoomID: roomID, Payload: req.Content}
	result, err := h.service.SendLiveMessage(r.Context(), msg, req.Type == "notice")
	writeDelivery(w, msg.MessageID, result, err)
}

func writeDelivery(w http.ResponseWriter, messageID string, result *wskit.DeliveryResult, err error) {
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	status := "accepted"
	if !result.Accepted && len(result.AcceptedConnIDs) > 0 {
		status = "partial"
	} else if !result.Accepted {
		status = "failed"
	}
	httpx.WriteOK(w, AcceptedResponse{
		MessageID: messageID, Status: status, Accepted: result.Accepted,
		AcceptedConnIDs: result.AcceptedConnIDs, FailedConnIDs: result.FailedConnIDs,
		Reason: result.Reason,
	})
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
