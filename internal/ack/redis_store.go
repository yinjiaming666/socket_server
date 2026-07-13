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

const addPendingScript = "local pending=KEYS[1]; local idx=KEYS[2]; local done=KEYS[3]; local failed=KEYS[4]; local score=ARGV[1]; local ttl=ARGV[2]; for i=3,#ARGV,2 do local conn=ARGV[i]; local member=ARGV[i+1]; redis.call('SADD',pending,conn); redis.call('ZADD',idx,score,member); redis.call('SREM',done,conn); redis.call('SREM',failed,conn); end; redis.call('EXPIRE',pending,ttl); redis.call('EXPIRE',done,ttl); redis.call('EXPIRE',failed,ttl); return 1"

const markDoneScript = "local pending=KEYS[1]; local done=KEYS[2]; local failed=KEYS[3]; local idx=KEYS[4]; local conn=ARGV[1]; local member=ARGV[2]; local ttl=ARGV[3]; if redis.call('SISMEMBER',pending,conn)==0 then redis.call('ZREM',idx,member); return 0; end; redis.call('SREM',pending,conn); redis.call('SADD',done,conn); redis.call('SREM',failed,conn); redis.call('ZREM',idx,member); redis.call('EXPIRE',pending,ttl); redis.call('EXPIRE',done,ttl); redis.call('EXPIRE',failed,ttl); return 1"

const markFailedScript = "local pending=KEYS[1]; local done=KEYS[2]; local failed=KEYS[3]; local idx=KEYS[4]; local conn=ARGV[1]; local member=ARGV[2]; local ttl=ARGV[3]; if redis.call('SISMEMBER',pending,conn)==0 then redis.call('ZREM',idx,member); return 0; end; redis.call('SREM',pending,conn); redis.call('SADD',failed,conn); redis.call('SREM',done,conn); redis.call('ZREM',idx,member); redis.call('EXPIRE',pending,ttl); redis.call('EXPIRE',done,ttl); redis.call('EXPIRE',failed,ttl); return 1"

// RedisStore 是基于 Redis 的 ACK 状态存储。
type RedisStore struct {
	client *redisclient.Client
}

// NewRedisStore 创建 Redis ACK 存储。
func NewRedisStore(client *redisclient.Client) *RedisStore {
	return &RedisStore{client: client}
}

// AddPending 原子添加待 ACK 的连接编号和超时索引。
func (s *RedisStore) AddPending(ctx context.Context, messageID string, connIDs []string) error {
	args := []string{
		"EVAL", addPendingScript, "4",
		s.client.Key(rediskey.AckPendingKey(messageID)),
		s.client.Key(ackPendingIndexKey),
		s.client.Key(rediskey.AckDoneKey(messageID)),
		s.client.Key(rediskey.AckFailedKey(messageID)),
		strconv.FormatInt(time.Now().Unix(), 10),
		strconv.Itoa(int(ackStateTTL.Seconds())),
	}
	for _, connID := range connIDs {
		if connID == "" {
			continue
		}
		args = append(args, connID, ackPendingIndexMember(messageID, connID))
	}
	if len(args) == 9 {
		return nil
	}
	_, err := s.client.Do(ctx, args...)
	return err
}

// MarkDone 仅当连接仍处于 pending 时原子标记为已 ACK。
func (s *RedisStore) MarkDone(ctx context.Context, messageID string, connID string) error {
	_, err := s.client.Do(ctx, "EVAL", markDoneScript, "4",
		s.client.Key(rediskey.AckPendingKey(messageID)),
		s.client.Key(rediskey.AckDoneKey(messageID)),
		s.client.Key(rediskey.AckFailedKey(messageID)),
		s.client.Key(ackPendingIndexKey),
		connID,
		ackPendingIndexMember(messageID, connID),
		strconv.Itoa(int(ackStateTTL.Seconds())),
	)
	return err
}

// MarkFailed 仅当连接仍处于 pending 时原子标记为 ACK 失败。
func (s *RedisStore) MarkFailed(ctx context.Context, messageID string, connID string, reason string) error {
	_, err := s.client.Do(ctx, "EVAL", markFailedScript, "4",
		s.client.Key(rediskey.AckPendingKey(messageID)),
		s.client.Key(rediskey.AckDoneKey(messageID)),
		s.client.Key(rediskey.AckFailedKey(messageID)),
		s.client.Key(ackPendingIndexKey),
		connID,
		ackPendingIndexMember(messageID, connID),
		strconv.Itoa(int(ackStateTTL.Seconds())),
	)
	return err
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

// isPending 判断索引成员对应的 pending 记录是否仍然存在。
func (s *RedisStore) isPending(ctx context.Context, messageID string, connID string) (bool, error) {
	value, err := s.client.String(ctx, "SISMEMBER", s.client.Key(rediskey.AckPendingKey(messageID)), connID)
	if err != nil {
		return false, err
	}
	return value == "1", nil
}

func ackPendingIndexMember(messageID string, connID string) string {
	return messageID + ackPendingIndexSep + connID
}

func splitAckPendingIndexMember(value string) (string, string, bool) {
	parts := strings.SplitN(value, ackPendingIndexSep, 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}
