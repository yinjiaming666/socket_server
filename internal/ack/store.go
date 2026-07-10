package ack

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Store 定义消息 ACK 临时状态存储接口。
type Store interface {
	// AddPending 添加待 ACK 的连接编号。
	AddPending(ctx context.Context, messageID string, connIDs []string) error
	// MarkDone 将指定连接标记为已 ACK。
	MarkDone(ctx context.Context, messageID string, connID string) error
	// MarkFailed 将指定连接标记为 ACK 失败。
	MarkFailed(ctx context.Context, messageID string, connID string, reason string) error
	// GetPending 查询指定消息仍未 ACK 的连接编号。
	GetPending(ctx context.Context, messageID string) ([]string, error)
	// GetDone 查询指定消息已经 ACK 的连接编号。
	GetDone(ctx context.Context, messageID string) ([]string, error)
	// GetFailed 查询指定消息 ACK 失败的连接编号。
	GetFailed(ctx context.Context, messageID string) ([]string, error)
}

// PendingItem 表示一条待 ACK 连接记录。
type PendingItem struct {
	// MessageID 表示消息编号。
	MessageID string
	// ConnID 表示连接编号。
	ConnID string
	// CreatedAt 表示进入 pending 的时间。
	CreatedAt time.Time
}

// TimeoutStore 定义支持 ACK 超时扫描的存储接口。
type TimeoutStore interface {
	Store
	// ListExpiredPending 查询已经超时的 pending 记录。
	ListExpiredPending(ctx context.Context, before time.Time, limit int) ([]PendingItem, error)
}

// MemoryStore 是用于本地开发和单元测试的内存 ACK 状态存储。
type MemoryStore struct {
	// mu 保护 pending、done、failed 并发读写。
	mu sync.RWMutex
	// pending 保存 message_id 到待 ACK 连接集合的映射。
	pending map[string]map[string]struct{}
	// pendingAt 保存 message_id:conn_id 到进入 pending 的时间。
	pendingAt map[string]time.Time
	// done 保存 message_id 到已 ACK 连接集合的映射。
	done map[string]map[string]struct{}
	// failed 保存 message_id 到 ACK 失败连接集合的映射。
	failed map[string]map[string]struct{}
	// reasons 保存 message_id:conn_id 到失败原因的映射。
	reasons map[string]string
}

// NewMemoryStore 创建内存 ACK 状态存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		pending:   make(map[string]map[string]struct{}),
		pendingAt: make(map[string]time.Time),
		done:      make(map[string]map[string]struct{}),
		failed:    make(map[string]map[string]struct{}),
		reasons:   make(map[string]string),
	}
}

// AddPending 添加待 ACK 的连接编号。
func (s *MemoryStore) AddPending(ctx context.Context, messageID string, connIDs []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.pending[messageID]; !ok {
		s.pending[messageID] = make(map[string]struct{})
	}
	now := time.Now()
	for _, connID := range connIDs {
		if connID == "" {
			continue
		}
		s.pending[messageID][connID] = struct{}{}
		s.pendingAt[pendingKey(messageID, connID)] = now
	}
	return nil
}

// MarkDone 将指定连接标记为已 ACK。
func (s *MemoryStore) MarkDone(ctx context.Context, messageID string, connID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.pending[messageID], connID)
	delete(s.pendingAt, pendingKey(messageID, connID))
	if _, ok := s.done[messageID]; !ok {
		s.done[messageID] = make(map[string]struct{})
	}
	s.done[messageID][connID] = struct{}{}
	return nil
}

// MarkFailed 将指定连接标记为 ACK 失败。
func (s *MemoryStore) MarkFailed(ctx context.Context, messageID string, connID string, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.pending[messageID], connID)
	delete(s.pendingAt, pendingKey(messageID, connID))
	if _, ok := s.failed[messageID]; !ok {
		s.failed[messageID] = make(map[string]struct{})
	}
	s.failed[messageID][connID] = struct{}{}
	s.reasons[pendingKey(messageID, connID)] = reason
	return nil
}

// GetPending 查询指定消息仍未 ACK 的连接编号。
func (s *MemoryStore) GetPending(ctx context.Context, messageID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return mapKeys(s.pending[messageID]), nil
}

// GetDone 查询指定消息已经 ACK 的连接编号。
func (s *MemoryStore) GetDone(ctx context.Context, messageID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return mapKeys(s.done[messageID]), nil
}

// GetFailed 查询指定消息 ACK 失败的连接编号。
func (s *MemoryStore) GetFailed(ctx context.Context, messageID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return mapKeys(s.failed[messageID]), nil
}

// ListExpiredPending 查询已经超时的 pending 记录。
func (s *MemoryStore) ListExpiredPending(ctx context.Context, before time.Time, limit int) ([]PendingItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 100
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]PendingItem, 0)
	for messageID, conns := range s.pending {
		for connID := range conns {
			createdAt := s.pendingAt[pendingKey(messageID, connID)]
			if !createdAt.IsZero() && createdAt.Before(before) {
				items = append(items, PendingItem{MessageID: messageID, ConnID: connID, CreatedAt: createdAt})
				if len(items) >= limit {
					return items, nil
				}
			}
		}
	}
	return items, nil
}

// pendingKey 返回 pending 记录内部键。
func pendingKey(messageID string, connID string) string {
	return messageID + ":" + connID
}

// mapKeys 将字符串集合转换为有序切片。
func mapKeys(items map[string]struct{}) []string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
