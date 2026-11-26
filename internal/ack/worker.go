package ack

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	scanInterval = 5 * time.Second
	batchSize    = 256
)

var expireScript = redis.NewScript(`
local deadline = tonumber(redis.call("ZSCORE", KEYS[2], ARGV[2]))
if not deadline or deadline > tonumber(ARGV[3]) then
	return 0
end
if redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
	redis.call("HSET", KEYS[1], ARGV[1], "failed")
	redis.call("EXPIRE", KEYS[1], ARGV[4])
end
redis.call("ZREM", KEYS[2], ARGV[2])
return 1
`)

// RunTimeouts 批量处理 Redis 中超过期限的回执，直到 ctx 取消。
func (s *Store) RunTimeouts(ctx context.Context) {
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now, err := s.client.Time(ctx).Result()
			if err == nil {
				err = s.expirePending(ctx, now)
			}
			if err != nil && ctx.Err() == nil {
				log.Printf("ack timeout scan failed: %v", err)
			}
		}
	}
}

func (s *Store) expirePending(ctx context.Context, before time.Time) error {
	maxScore := strconv.FormatInt(before.UnixMilli(), 10)
	for {
		members, err := s.client.ZRangeByScore(ctx, s.deadlineKey, &redis.ZRangeBy{
			Min: "-inf", Max: maxScore, Count: batchSize,
		}).Result()
		if err != nil || len(members) == 0 {
			return err
		}
		_, err = s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, member := range members {
				id, connID, ok := strings.Cut(member, ":")
				if !ok {
					pipe.ZRem(ctx, s.deadlineKey, member)
					continue
				}
				expireScript.Eval(ctx, pipe, []string{s.messagePrefix + id, s.deadlineKey},
					connID, member, maxScore, stateTTLSeconds)
			}
			return nil
		})
		if err != nil || len(members) < batchSize {
			return err
		}
	}
}
