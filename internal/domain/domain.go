package domain

import "encoding/json"

// Endpoint 表示一个可被 Gateway 分配的 WebSocket 节点。
type Endpoint struct {
	ID        string
	Status    string
	WSPath    string
	WSAddr    string
	GRPCAddr  string
	ConnCount int64
	MaxConn   int64
	UpdatedAt int64
}

// ConnInfo 表示一个用户长连接在系统中的路由信息。
type ConnInfo struct {
	ConnID     string
	UserID     string
	EndpointID string
	DeviceID   string
	Platform   string
	GRPCAddr   string
	CreatedAt  int64
	UpdatedAt  int64
}

// Message 表示系统内部统一流转的消息对象。ConnIDs 和 DeliveryRoutes 仅用于内部路由，不得直接发送给客户端。
type Message struct {
	MessageID  string
	FromUserID string
	ToUserIDs  []string
	ConnIDs    []string
	// DeliveryRoutes 缓存本次投递已解析的路由，避免发送器再次访问 Redis。
	DeliveryRoutes []*ConnInfo
	GroupID        string
	RoomID         string
	BizType        string
	Payload        []byte
	AckRequired    bool
	CreatedAt      int64
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

// ClientMessageData 是业务消息公开数据，不包含 ConnIDs 和 DeliveryRoutes。
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

// SendResult 表示一次消息发送的聚合结果。
type SendResult struct {
	Accepted        bool
	AcceptedConnIDs []string
	FailedConnIDs   []string
	Reason          string
}

// PushResponse 表示 WebSocket Node 接受推送任务后的响应。
type PushResponse struct {
	Accepted        bool
	AcceptedConnIDs []string
	FailedConnIDs   []string
	Reason          string
}

// RouteTokenClaims 表示 Gateway 下发给客户端的临时路由凭证内容。
type RouteTokenClaims struct {
	UserID     string
	EndpointID string
	ExpiresAt  int64
}
