package domain

// Endpoint 表示一个可被 Gateway 分配的 WebSocket 节点。
type Endpoint struct {
	// ID 表示节点唯一编号，例如 ws-node-1。
	ID string
	// Status 表示节点状态，例如 online、draining、offline。
	Status string
	// WSPath 表示 Nginx 转发使用的 WebSocket 路径。
	WSPath string
	// WSAddr 表示 WebSocket Node 对外或内网监听地址。
	WSAddr string
	// GRPCAddr 表示 WebSocket Node 内部推送服务地址。
	GRPCAddr string
	// ConnCount 表示当前节点连接数量。
	ConnCount int64
	// MaxConn 表示当前节点允许承载的最大连接数量。
	MaxConn int64
	// UpdatedAt 表示节点最近一次心跳时间戳，单位秒。
	UpdatedAt int64
}

// ConnInfo 表示一个用户长连接在系统中的路由信息。
type ConnInfo struct {
	// ConnID 表示连接唯一编号。
	ConnID string
	// UserID 表示连接所属用户编号。
	UserID string
	// EndpointID 表示连接所在的 WebSocket 节点编号。
	EndpointID string
	// DeviceID 表示客户端设备编号。
	DeviceID string
	// Platform 表示客户端平台，例如 ios、android、web。
	Platform string
	// GRPCAddr 表示该连接所在节点的内部推送地址。
	GRPCAddr string
	// CreatedAt 表示连接创建时间戳，单位秒。
	CreatedAt int64
	// UpdatedAt 表示连接最近更新时间戳，单位秒。
	UpdatedAt int64
}

// Message 表示系统内部统一流转的消息对象。
type Message struct {
	// MessageID 表示消息唯一编号。
	MessageID string
	// FromUserID 表示发送方用户编号。
	FromUserID string
	// ToUserIDs 表示接收方用户编号列表。
	ToUserIDs []string
	// ConnIDs 表示本次需要推送的连接编号列表。
	ConnIDs []string
	// GroupID 表示群聊编号。
	GroupID string
	// RoomID 表示直播间编号。
	RoomID string
	// BizType 表示业务消息类型，例如 single、group、live_danmu、live_notice、system_to_user。
	BizType string
	// Payload 表示已经序列化后的业务消息体。
	Payload []byte
	// AckRequired 表示客户端收到消息后是否需要 ACK。
	AckRequired bool
	// CreatedAt 表示消息创建时间戳，单位秒。
	CreatedAt int64
}

// SendResult 表示一次消息发送的聚合结果。
type SendResult struct {
	// Accepted 表示发送任务是否被下游发送器接受。
	Accepted bool
	// AcceptedConnIDs 表示已经被节点接受的连接编号列表。
	AcceptedConnIDs []string
	// FailedConnIDs 表示发送失败的连接编号列表。
	FailedConnIDs []string
	// Reason 表示失败原因或补充说明。
	Reason string
}

// PushResponse 表示 WebSocket Node 接受推送任务后的响应。
type PushResponse struct {
	// Accepted 表示节点是否接受本次推送任务。
	Accepted bool
	// AcceptedConnIDs 表示节点已经写入本地发送队列的连接编号列表。
	AcceptedConnIDs []string
	// FailedConnIDs 表示节点无法处理的连接编号列表。
	FailedConnIDs []string
	// Reason 表示节点返回的失败原因或补充说明。
	Reason string
}

// RouteTokenClaims 表示 Gateway 下发给客户端的临时路由凭证内容。
type RouteTokenClaims struct {
	// UserID 表示凭证所属用户编号。
	UserID string
	// EndpointID 表示凭证允许连接的节点编号。
	EndpointID string
	// ExpiresAt 表示凭证过期时间戳，单位秒。
	ExpiresAt int64
}
