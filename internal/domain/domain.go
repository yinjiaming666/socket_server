package domain

import (
	"encoding/json"
	"errors"

	"github.com/yinjiaming666/wskit"
)

// Message 表示业务消息。ConnIDs 只供内部接口使用。
type Message struct {
	MessageID   string
	FromUserID  string
	ToUserIDs   []string
	ConnIDs     []string
	GroupID     string
	RoomID      string
	BizType     string
	Payload     []byte
	AckRequired bool
	CreatedAt   int64
}

const (
	// ClientChannel 是客户端与服务端 WebSocket JSON 信封的固定协议通道。
	ClientChannel = "socket_server"
	// ClientMessageMethod 表示服务端下发业务消息的方法名。
	ClientMessageMethod = "message"
)

// ClientMessage 是发送给 WebSocket 客户端的统一信封，不包含内部连接路由。
type ClientMessage struct {
	C    string            `json:"c"`
	M    string            `json:"m"`
	Data ClientMessageData `json:"data"`
}

// ClientMessageData 是业务消息公开数据。
type ClientMessageData struct {
	MessageID   string          `json:"message_id"`
	FromUserID  string          `json:"from_user_id,omitempty"`
	ToUserIDs   []string        `json:"to_user_ids,omitempty"`
	GroupID     string          `json:"group_id,omitempty"`
	RoomID      string          `json:"room_id,omitempty"`
	BizType     string          `json:"biz_type"`
	Payload     json.RawMessage `json:"payload"`
	AckRequired bool            `json:"ack_required"`
	CreatedAt   int64           `json:"created_at"`
}

// MarshalClientMessage 将内部消息转换为客户端统一信封，避免暴露 ConnIDs 并避免 []byte 被 Base64 编码。
func MarshalClientMessage(msg *Message) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	payload := json.RawMessage(msg.Payload)
	if !json.Valid(payload) {
		encoded, err := json.Marshal(string(msg.Payload))
		if err != nil {
			return nil, err
		}
		payload = encoded
	}
	return json.Marshal(ClientMessage{
		C: ClientChannel,
		M: ClientMessageMethod,
		Data: ClientMessageData{
			MessageID:   msg.MessageID,
			FromUserID:  msg.FromUserID,
			ToUserIDs:   msg.ToUserIDs,
			GroupID:     msg.GroupID,
			RoomID:      msg.RoomID,
			BizType:     msg.BizType,
			Payload:     payload,
			AckRequired: msg.AckRequired,
			CreatedAt:   msg.CreatedAt,
		},
	})
}

func EncodeMessageFrame(msg *Message) (wskit.Frame, error) {
	if msg == nil {
		return wskit.Frame{}, errors.New("message is nil")
	}
	body, err := MarshalClientMessage(msg)
	if err != nil {
		return wskit.Frame{}, err
	}
	frame := wskit.Frame{Type: wskit.TextFrame, Data: body}
	return frame, frame.Validate()
}

// RouteTokenClaims 表示 Gateway 下发给客户端的临时路由凭证内容。
type RouteTokenClaims struct {
	UserID     string
	EndpointID string
	ExpiresAt  int64
}
