package ack

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yinjiaming666/wskit"

	"socket_server/internal/config"
	"socket_server/internal/domain"
)

const (
	timeout         = 30 * time.Second
	stateTTLSeconds = int((24 * time.Hour) / time.Second)
)

const redisMillis = `
local now = redis.call("TIME")
local nowMillis = now[1] * 1000 + math.floor(now[2] / 1000)
`

var trackScript = redis.NewScript(redisMillis + `
local deadline = nowMillis + tonumber(ARGV[1])
local added = false
for i = 4, #ARGV, 2 do
	local conn = ARGV[i]
	if redis.call("HSETNX", KEYS[1], conn, ARGV[i + 1]) == 1 then
		redis.call("ZADD", KEYS[2], deadline, ARGV[3] .. ":" .. conn)
		added = true
	end
end
if added then
	redis.call("EXPIRE", KEYS[1], ARGV[2])
end
return 1
`)

var confirmScript = redis.NewScript(redisMillis + `
if redis.call("HGET", KEYS[1], ARGV[1]) ~= ARGV[2] then
	return 0
end

local deadline = tonumber(redis.call("ZSCORE", KEYS[2], ARGV[3]))
if not deadline then
	return 0
end
local state = "done"
if deadline <= nowMillis then
	state = "failed"
end
redis.call("HSET", KEYS[1], ARGV[1], state)
redis.call("EXPIRE", KEYS[1], ARGV[4])
redis.call("ZREM", KEYS[2], ARGV[3])
return 1
`)

// Store 保存共享 Redis 中每条消息的逐连接回执状态。
type Store struct {
	client        *redis.Client
	messagePrefix string
	deadlineKey   string
}

func NewStore(cfg config.RedisConfig) *Store {
	prefix := cfg.KeyPrefix + "socket_server:ack:"
	return &Store{
		client: redis.NewClient(&redis.Options{
			Addr: cfg.Addr, Password: cfg.Password, DB: cfg.DB, PoolSize: cfg.PoolSize,
		}),
		messagePrefix: prefix + "msg:",
		deadlineKey:   prefix + "deadlines",
	}
}

func (s *Store) Close() error { return s.client.Close() }

// Track 在消息入队前登记目标连接。重复投递保留已有期限和状态。
func (s *Store) Track(ctx context.Context, messageID, endpointID string, recipients []wskit.SessionInfo) error {
	if err := domain.ValidateAckMessageID(messageID); err != nil {
		return err
	}
	if len(recipients) == 0 {
		return nil
	}
	id := messageKeySuffix(messageID)
	args := make([]any, 0, 3+2*len(recipients))
	args = append(args, timeout.Milliseconds(), stateTTLSeconds, id)
	ownerPrefix := pendingOwnerPrefix(endpointID)
	for _, recipient := range recipients {
		args = append(args, recipient.ConnID, ownerPrefix+recipient.UserID)
	}
	return trackScript.Run(ctx, s.client, []string{s.messagePrefix + id, s.deadlineKey}, args...).Err()
}

// Confirm 确认当前节点、用户及连接对应的待回执消息。
func (s *Store) Confirm(ctx context.Context, messageID, endpointID, connID, userID string) error {
	if err := domain.ValidateAckMessageID(messageID); err != nil {
		return err
	}
	id := messageKeySuffix(messageID)
	owner := pendingOwnerPrefix(endpointID) + userID
	return confirmScript.Run(ctx, s.client, []string{s.messagePrefix + id, s.deadlineKey},
		connID, owner, id+":"+connID, stateTTLSeconds).Err()
}

// State 查询逐连接状态。记录不存在时返回空字符串。
func (s *Store) State(ctx context.Context, messageID, connID string) (string, error) {
	state, err := s.client.HGet(ctx, s.messagePrefix+messageKeySuffix(messageID), connID).Result()
	if err == redis.Nil {
		return "", nil
	}
	if strings.HasPrefix(state, "pending:") {
		return "pending", err
	}
	return state, err
}

func pendingOwnerPrefix(endpointID string) string {
	return "pending:" + base64.RawURLEncoding.EncodeToString([]byte(endpointID)) + ":"
}

func messageKeySuffix(messageID string) string {
	digest := sha256.Sum256([]byte(messageID))
	return hex.EncodeToString(digest[:])
}
