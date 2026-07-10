package message

import (
	"context"

	"socket_server/internal/domain"
)

// Store 定义消息持久化接口。
type Store interface {
	// SaveMessage 保存消息主体。
	SaveMessage(ctx context.Context, msg *domain.Message) error
	// SaveDeliveryStatus 保存消息投递状态。
	SaveDeliveryStatus(ctx context.Context, messageID string, userID string, connID string, status string) error
	// SaveClientAck 保存客户端 ACK 状态。
	SaveClientAck(ctx context.Context, messageID string, userID string, connID string, ackType string) error
}

// EmptyStore 是当前第一版使用的空消息存储实现。
type EmptyStore struct{}

// NewEmptyStore 创建空消息存储实现。
func NewEmptyStore() *EmptyStore {
	return &EmptyStore{}
}

// SaveMessage 保存消息主体，当前版本暂不下库。
func (s *EmptyStore) SaveMessage(ctx context.Context, msg *domain.Message) error {
	return ctx.Err()
}

// SaveDeliveryStatus 保存消息投递状态，当前版本暂不下库。
func (s *EmptyStore) SaveDeliveryStatus(ctx context.Context, messageID string, userID string, connID string, status string) error {
	return ctx.Err()
}

// SaveClientAck 保存客户端 ACK 状态，当前版本暂不下库。
func (s *EmptyStore) SaveClientAck(ctx context.Context, messageID string, userID string, connID string, ackType string) error {
	return ctx.Err()
}
