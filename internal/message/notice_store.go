package message

import (
	"context"

	"github.com/redis/go-redis/v9"

	"socket_server/internal/config"
	"socket_server/internal/domain"
)

// NoticeStore 保存直播间公告历史。
type NoticeStore struct {
	client *redis.Client
	prefix string
}

func NewNoticeStore(cfg config.RedisConfig) *NoticeStore {
	return &NoticeStore{
		client: redis.NewClient(&redis.Options{
			Addr: cfg.Addr, Password: cfg.Password, DB: cfg.DB, PoolSize: cfg.PoolSize,
		}),
		prefix: cfg.KeyPrefix,
	}
}

func (s *NoticeStore) Close() error { return s.client.Close() }

func (s *NoticeStore) save(ctx context.Context, msg *domain.Message, payload []byte) error {
	return s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.prefix + "stream:live:room:" + msg.RoomID,
		MaxLen: 10000,
		Approx: true,
		Values: map[string]any{
			"message_id": msg.MessageID,
			"biz_type":   msg.BizType,
			"payload":    string(payload),
			"created_at": msg.CreatedAt,
		},
	}).Err()
}
