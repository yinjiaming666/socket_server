package ack

import (
	"context"
	"strconv"
	"strings"
	"time"

	"socket_server/internal/redisclient"
	"socket_server/internal/rediskey"
)

const (
	ackPendingIndexKey = "ack:pending:index"
	ackPendingIndexSep = "\x00"
	ackStateTTL        = 24 * time.Hour
)

// RedisStore 是基于 Redis 的 ACK 状态存储。
type RedisStore struct {
	// client 表示 Redis 客户端。
	client *redisclient.Client
}

// NewRedisStore 创建 Redis ACK 存储。
func NewRedisStore(client *redisclient.Client) *RedisStore {
	return &RedisStore{client: client}
}

// AddPending 添加待 ACK 的连接编号。
func (s *RedisStore) AddPending(ctx context.Context, messageID string, connIDs []string) error {
	if len(connIDs) == 0 {
		return nil
	}
	args := []string{"SADD", s.client.Key(rediskey.AckPendingKey(messageID))}
	for _, connID := range connIDs {
		if connID != "" {
			args = append(args, connID)
		}
	}
	if len(args) <= 2 {
		return nil
	}
	if _, err := s.client.Do(ctx, args...); err != nil {
		return err
	}
	score := strconv.FormatInt(time.Now().Unix(), 10)
	for _, connID := range connIDs {
		if connID == "" {
			continue
		}
		if _, err := s.client.Do(ctx, "ZADD", s.client.Key(ackPendingIndexKey), score, ackPendingIndexMember(messageID, connID)); err != nil {
			return err
		}
	}
	return s.expireMessageKeys(ctx, messageID)
}

// MarkDone 将指定连接标记为已 ACK。
func (s *RedisStore) MarkDone(ctx context.Context, messageID string, connID string) error {
	if _, err := s.client.Do(ctx, "SREM", s.client.Key(rediskey.AckPendingKey(messageID)), connID); err != nil {
		return err
	}
	if _, err := s.client.Do(ctx, "SADD", s.client.Key(rediskey.AckDoneKey(messageID)), connID); err != nil {
		return err
	}
	if _, err := s.client.Do(ctx, "ZREM", s.client.Key(ackPendingIndexKey), ackPendingIndexMember(messageID, connID)); err != nil {
		return err
	}
	return s.expireMessageKeys(ctx, messageID)
}

// MarkFailed 将指定连接标记为 ACK 失败。
func (s *RedisStore) MarkFailed(ctx context.Context, messageID string, connID string, reason string) error {
	if _, err := s.client.Do(ctx, "SREM", s.client.Key(rediskey.AckPendingKey(messageID)), connID); err != nil {
		return err
	}
	if _, err := s.client.Do(ctx, "SADD", s.client.Key(rediskey.AckFailedKey(messageID)), connID); err != nil {
		return err
	}
	if _, err := s.client.Do(ctx, "ZREM", s.client.Key(ackPendingIndexKey), ackPendingIndexMember(messageID, connID)); err != nil {
		return err
	}
	return s.expireMessageKeys(ctx, messageID)
}

// GetPending 查询指定消息仍未 ACK 的连接编号。
func (s *RedisStore) GetPending(ctx context.Context, messageID string) ([]string, error) {
	return s.client.Strings(ctx, "SMEMBERS", s.client.Key(rediskey.AckPendingKey(messageID)))
}

// GetDone 查询指定消息已经 ACK 的连接编号。
func (s *RedisStore) GetDone(ctx context.Context, messageID string) ([]string, error) {
	return s.client.Strings(ctx, "SMEMBERS", s.client.Key(rediskey.AckDoneKey(messageID)))
}

// GetFailed 查询指定消息 ACK 失败的连接编号。
func (s *RedisStore) GetFailed(ctx context.Context, messageID string) ([]string, error) {
	return s.client.Strings(ctx, "SMEMBERS", s.client.Key(rediskey.AckFailedKey(messageID)))
}

// ListExpiredPending 查询已经超时的 pending 记录。
func (s *RedisStore) ListExpiredPending(ctx context.Context, before time.Time, limit int) ([]PendingItem, error) {
	if limit <= 0 {
		limit = 100
	}
	values, err := s.client.Strings(ctx, "ZRANGEBYSCORE", s.client.Key(ackPendingIndexKey), "-inf", strconv.FormatInt(before.Unix(), 10), "LIMIT", "0", strconv.Itoa(limit))
	if err != nil {
		return nil, err
	}
	items := make([]PendingItem, 0, len(values))
	for _, value := range values {
		messageID, connID, ok := splitAckPendingIndexMember(value)
		if !ok {
			continue
		}
		exists, err := s.isPending(ctx, messageID, connID)
		if err != nil {
			return nil, err
		}
		if !exists {
			_, _ = s.client.Do(ctx, "ZREM", s.client.Key(ackPendingIndexKey), value)
			continue
		}
		items = append(items, PendingItem{MessageID: messageID, ConnID: connID})
	}
	return items, nil
}

// expireMessageKeys 为单条消息的 ACK 临时状态设置保留窗口。
func (s *RedisStore) expireMessageKeys(ctx context.Context, messageID string) error {
	seconds := strconv.Itoa(int(ackStateTTL.Seconds()))
	for _, key := range []string{
		rediskey.AckPendingKey(messageID),
		rediskey.AckDoneKey(messageID),
		rediskey.AckFailedKey(messageID),
	} {
		if _, err := s.client.Do(ctx, "EXPIRE", s.client.Key(key), seconds); err != nil {
			return err
		}
	}
	return nil
}

// isPending 判断索引成员对应的 pending 记录是否仍然存在。
func (s *RedisStore) isPending(ctx context.Context, messageID string, connID string) (bool, error) {
	value, err := s.client.String(ctx, "SISMEMBER", s.client.Key(rediskey.AckPendingKey(messageID)), connID)
	if err != nil {
		return false, err
	}
	return value == "1", nil
}

// ackPendingIndexMember 返回 ACK pending 索引成员。
func ackPendingIndexMember(messageID string, connID string) string {
	return messageID + ackPendingIndexSep + connID
}

// splitAckPendingIndexMember 拆分 ACK pending 索引成员。
func splitAckPendingIndexMember(value string) (string, string, bool) {
	parts := strings.SplitN(value, ackPendingIndexSep, 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}
