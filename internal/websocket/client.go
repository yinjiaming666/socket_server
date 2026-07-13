package websocket

import (
	"errors"
	"sync"
)

const maxRoomsPerClient = 64

// Client 表示 WebSocket Node 本地维护的一条客户端连接。
type Client struct {
	// ConnID 表示连接唯一编号。
	ConnID string
	// UserID 表示连接所属用户编号。
	UserID string
	// DeviceID 表示客户端设备编号。
	DeviceID string
	// Platform 表示客户端平台，例如 ios、android、web。
	Platform string
	// Send 表示连接的异步发送队列。
	Send chan []byte
	// mu 保护 rooms、closed、writeFunc 并发读写。
	mu sync.RWMutex
	// rooms 表示当前连接加入的直播间集合。
	rooms map[string]struct{}
	// closed 表示连接是否已经关闭。
	closed bool
	// done 在连接关闭时广播，Send 不关闭以避免并发发送 panic。
	done chan struct{}
	// writeFunc 表示真实 WebSocket 连接的写入函数。
	writeFunc func([]byte) error
}

// NewClient 创建本地客户端连接对象。
func NewClient(connID string, userID string, deviceID string, platform string, sendBuffer int) *Client {
	if sendBuffer <= 0 {
		sendBuffer = 128
	}
	return &Client{
		ConnID:   connID,
		UserID:   userID,
		DeviceID: deviceID,
		Platform: platform,
		Send:     make(chan []byte, sendBuffer),
		done:     make(chan struct{}),
		rooms:    make(map[string]struct{}),
	}
}

// AttachWriter 绑定真实 WebSocket 写入函数。
func (c *Client) AttachWriter(writeFunc func([]byte) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeFunc = writeFunc
}

// Enqueue 将只读消息缓冲区写入连接发送队列。
//
// payload 在入队后必须保持不可变。多个客户端共享同一底层字节数组，
// 可以避免直播间广播时为每条连接复制完整消息。队列满时主动关闭慢连接。
func (c *Client) Enqueue(payload []byte) bool {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return false
	}
	select {
	case c.Send <- payload:
		c.mu.RUnlock()
		return true
	default:
		c.mu.RUnlock()
		c.Close()
		return false
	}
}

// WriteDirect 直接写入真实 WebSocket 连接。
func (c *Client) WriteDirect(payload []byte) error {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return errors.New("websocket client is closed")
	}
	writeFunc := c.writeFunc
	c.mu.RUnlock()
	if writeFunc == nil {
		return errors.New("websocket writer is not attached")
	}
	return writeFunc(payload)
}

// Done 返回连接关闭通知。
func (c *Client) Done() <-chan struct{} {
	return c.done
}

// JoinRoom 标记当前连接加入指定直播间，并限制单连接房间数量。
func (c *Client) JoinRoom(roomID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	if _, exists := c.rooms[roomID]; exists {
		return true
	}
	if len(c.rooms) >= maxRoomsPerClient {
		return false
	}
	c.rooms[roomID] = struct{}{}
	return true
}

// LeaveRoom 标记当前连接离开指定直播间。
func (c *Client) LeaveRoom(roomID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.rooms, roomID)
}

// Rooms 返回当前连接加入的直播间列表。
func (c *Client) Rooms() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	rooms := make([]string, 0, len(c.rooms))
	for roomID := range c.rooms {
		rooms = append(rooms, roomID)
	}
	return rooms
}

// Close 关闭当前连接。Send 保持打开，消费者通过 Done 退出。
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	c.closed = true
	close(c.done)
}
