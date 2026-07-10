package ack

import (
	"context"
	"log"
	"time"
)

// TimeoutWorkerConfig 表示 ACK 超时扫描任务配置。
type TimeoutWorkerConfig struct {
	// Timeout 表示 pending 多久未 ACK 后标记失败。
	Timeout time.Duration
	// Interval 表示扫描间隔。
	Interval time.Duration
	// BatchSize 表示每轮最多处理条数。
	BatchSize int
}

// StartTimeoutWorker 启动 ACK 超时扫描任务。
func StartTimeoutWorker(ctx context.Context, store TimeoutStore, config TimeoutWorkerConfig) {
	if store == nil {
		return
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	if config.Interval <= 0 {
		config.Interval = 5 * time.Second
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}

	ticker := time.NewTicker(config.Interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				expirePending(ctx, store, config)
			}
		}
	}()
}

// expirePending 将超时 pending 标记为失败。
func expirePending(ctx context.Context, store TimeoutStore, config TimeoutWorkerConfig) {
	before := time.Now().Add(-config.Timeout)
	items, err := store.ListExpiredPending(ctx, before, config.BatchSize)
	if err != nil {
		log.Printf("ack timeout scan failed: %v", err)
		return
	}
	for _, item := range items {
		if err := store.MarkFailed(ctx, item.MessageID, item.ConnID, "ack timeout"); err != nil {
			log.Printf("ack mark failed error: %v", err)
		}
	}
}
