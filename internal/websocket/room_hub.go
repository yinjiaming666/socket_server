package websocket

import (
	"sort"
	"sync"
)

// RoomHub 维护 WebSocket Node 本地直播间到连接的映射。
type RoomHub struct {
	// mu 保护 rooms 并发读写。
	mu sync.RWMutex
	// rooms 保存 room_id 到 conn_id 和连接对象的映射。
	rooms map[string]map[string]*Client
}

// NewRoomHub 创建本地直播间连接管理器。
func NewRoomHub() *RoomHub {
	return &RoomHub{rooms: make(map[string]map[string]*Client)}
}

// Join 将连接加入指定直播间，并返回该房间是否首次出现在本节点。
func (h *RoomHub) Join(roomID string, client *Client) (bool, bool) {
	if roomID == "" || client == nil || !client.JoinRoom(roomID) {
		return false, false
	}

	h.mu.Lock()
	firstLocalMember := len(h.rooms[roomID]) == 0
	if _, ok := h.rooms[roomID]; !ok {
		h.rooms[roomID] = make(map[string]*Client)
	}
	h.rooms[roomID][client.ConnID] = client
	h.mu.Unlock()
	return true, firstLocalMember
}

// Leave 将连接移出指定直播间，并返回房间是否已无本地成员。
func (h *RoomHub) Leave(roomID string, connID string) bool {
	h.mu.Lock()
	client := h.rooms[roomID][connID]
	delete(h.rooms[roomID], connID)
	lastLocalMember := client != nil && len(h.rooms[roomID]) == 0
	if len(h.rooms[roomID]) == 0 {
		delete(h.rooms, roomID)
	}
	h.mu.Unlock()

	if client != nil {
		client.LeaveRoom(roomID)
	}
	return lastLocalMember
}

// RemoveClient 仅移除仍指向期望 Client 实例的房间关系，并返回因此失去最后成员的房间。
// 相同 conn_id 已被新实例替换时，旧连接的延迟清理不会误删新连接。
func (h *RoomHub) RemoveClient(expected *Client) []string {
	if expected == nil {
		return nil
	}

	h.mu.Lock()
	removed := false
	emptyRooms := make([]string, 0)
	for roomID, clients := range h.rooms {
		current, ok := clients[expected.ConnID]
		if !ok || current != expected {
			continue
		}
		removed = true
		delete(clients, expected.ConnID)
		if len(clients) == 0 {
			delete(h.rooms, roomID)
			emptyRooms = append(emptyRooms, roomID)
		}
	}
	h.mu.Unlock()

	if removed {
		for _, roomID := range expected.Rooms() {
			expected.LeaveRoom(roomID)
		}
	}
	sort.Strings(emptyRooms)
	return emptyRooms
}

// Broadcast 向指定直播间内本节点连接广播消息。
func (h *RoomHub) Broadcast(roomID string, payload []byte) int {
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.rooms[roomID]))
	for _, client := range h.rooms[roomID] {
		clients = append(clients, client)
	}
	h.mu.RUnlock()

	sent := 0
	for _, client := range clients {
		if client.Enqueue(payload) {
			sent++
		}
	}
	return sent
}

// ListConns 返回指定直播间在本节点上的连接编号列表。
func (h *RoomHub) ListConns(roomID string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	connIDs := make([]string, 0, len(h.rooms[roomID]))
	for connID := range h.rooms[roomID] {
		connIDs = append(connIDs, connID)
	}
	return connIDs
}

// ListRooms 返回本节点当前承载的直播间列表。
func (h *RoomHub) ListRooms() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	roomIDs := make([]string, 0, len(h.rooms))
	for roomID := range h.rooms {
		roomIDs = append(roomIDs, roomID)
	}
	sort.Strings(roomIDs)
	return roomIDs
}
