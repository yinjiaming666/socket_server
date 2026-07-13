package websocket

import "testing"

// TestRoomHubOldClientCannotRemoveReplacement 验证旧连接延迟清理不会删除相同 conn_id 的新实例。
func TestRoomHubOldClientCannotRemoveReplacement(t *testing.T) {
	hub := NewRoomHub()
	oldClient := NewClient("same-conn", "user-1", "old-device", "test", 4)
	newClient := NewClient("same-conn", "user-1", "new-device", "test", 4)

	if joined, _ := hub.Join("room-1", oldClient); !joined {
		t.Fatal("old client failed to join room")
	}
	if joined, _ := hub.Join("room-1", newClient); !joined {
		t.Fatal("replacement client failed to join room")
	}

	// 房间映射已经指向新实例，此时清理旧实例必须成为空操作。
	if emptyRooms := hub.RemoveClient(oldClient); len(emptyRooms) != 0 {
		t.Fatalf("old client unexpectedly emptied rooms: %#v", emptyRooms)
	}
	connIDs := hub.ListConns("room-1")
	if len(connIDs) != 1 || connIDs[0] != "same-conn" {
		t.Fatalf("replacement room membership was removed: %#v", connIDs)
	}

	if emptyRooms := hub.RemoveClient(newClient); len(emptyRooms) != 1 || emptyRooms[0] != "room-1" {
		t.Fatalf("replacement cleanup did not empty room: %#v", emptyRooms)
	}
}
