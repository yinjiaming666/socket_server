package bus

import (
	"context"
	"sync"
)

// Subscription 表示一次 Pub/Sub 订阅。
type Subscription struct {
	// Channel 表示订阅的频道名称。
	Channel string
	// C 表示接收消息的只读通道。
	C <-chan []byte
	// cancel 表示取消订阅的内部函数。
	cancel func()
}

// Close 关闭当前订阅并释放本地资源。
func (s *Subscription) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}

// MemoryPubSub 是用于本地开发和测试的内存 Pub/Sub 实现。
type MemoryPubSub struct {
	// mu 保护 subscribers 并发读写。
	mu sync.RWMutex
	// subscribers 保存 channel 到订阅通道集合的映射。
	subscribers map[string]map[chan []byte]struct{}
}

// NewMemoryPubSub 创建内存 Pub/Sub。
func NewMemoryPubSub() *MemoryPubSub {
	return &MemoryPubSub{subscribers: make(map[string]map[chan []byte]struct{})}
}

// Publish 向指定频道发布消息。
func (p *MemoryPubSub) Publish(ctx context.Context, channel string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	// payload 在发布期间视为不可变，所有订阅者共享同一底层缓冲区，
	// 避免房间广播时按订阅者数量重复分配内存。
	for ch := range p.subscribers[channel] {
		select {
		case ch <- payload:
		default:
		}
	}
	return nil
}

// PublishMany 将同一只读消息发布到多个内存频道。
func (p *MemoryPubSub) PublishMany(ctx context.Context, channels []string, payload []byte) error {
	for _, channel := range channels {
		if err := p.Publish(ctx, channel, payload); err != nil {
			return err
		}
	}
	return nil
}

// Subscribe 订阅指定频道。
func (p *MemoryPubSub) Subscribe(channel string, buffer int) *Subscription {
	if buffer <= 0 {
		buffer = 16
	}
	ch := make(chan []byte, buffer)

	p.mu.Lock()
	if _, ok := p.subscribers[channel]; !ok {
		p.subscribers[channel] = make(map[chan []byte]struct{})
	}
	p.subscribers[channel][ch] = struct{}{}
	p.mu.Unlock()

	return &Subscription{
		Channel: channel,
		C:       ch,
		cancel: func() {
			p.mu.Lock()
			delete(p.subscribers[channel], ch)
			if len(p.subscribers[channel]) == 0 {
				delete(p.subscribers, channel)
			}
			close(ch)
			p.mu.Unlock()
		},
	}
}

// StreamEntry 表示一条内存 Stream 消息。
type StreamEntry struct {
	// ID 表示 Stream 内的自增消息编号。
	ID int64
	// Values 表示 Stream 消息字段。
	Values map[string]any
}

// MemoryStream 是用于本地开发和测试的内存 Stream 实现。
type MemoryStream struct {
	// mu 保护 entries 和 nextID 并发读写。
	mu sync.RWMutex
	// entries 保存 stream 到消息列表的映射。
	entries map[string][]StreamEntry
	// nextID 保存 stream 到下一个消息编号的映射。
	nextID map[string]int64
}

// NewMemoryStream 创建内存 Stream。
func NewMemoryStream() *MemoryStream {
	return &MemoryStream{
		entries: make(map[string][]StreamEntry),
		nextID:  make(map[string]int64),
	}
}

// XAdd 向指定 Stream 写入消息。
func (s *MemoryStream) XAdd(ctx context.Context, stream string, values map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextID[stream]++
	copied := make(map[string]any, len(values))
	for key, value := range values {
		copied[key] = value
	}
	s.entries[stream] = append(s.entries[stream], StreamEntry{ID: s.nextID[stream], Values: copied})
	return nil
}

// List 返回指定 Stream 的全部内存消息。
func (s *MemoryStream) List(stream string) []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := make([]StreamEntry, 0, len(s.entries[stream]))
	for _, entry := range s.entries[stream] {
		copiedValues := make(map[string]any, len(entry.Values))
		for key, value := range entry.Values {
			copiedValues[key] = value
		}
		entries = append(entries, StreamEntry{ID: entry.ID, Values: copiedValues})
	}
	return entries
}
