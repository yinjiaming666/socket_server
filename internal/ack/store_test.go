package ack

import (
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/yinjiaming666/wskit"

	"socket_server/internal/config"
)

func TestReceiptTransitions(t *testing.T) {
	addr := os.Getenv("WS_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set WS_TEST_REDIS_ADDR to run the Redis ACK test")
	}
	store := NewStore(config.RedisConfig{Addr: addr, KeyPrefix: "ack-test:" + rand.Text() + ":"})
	const messageID, nodeID = "message:1", "node-1"
	id := messageKeySuffix(messageID)
	t.Cleanup(func() {
		defer store.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := store.client.Del(ctx, store.messagePrefix+id, store.deadlineKey).Err(); err != nil {
			t.Error(err)
		}
	})

	ctx := t.Context()
	recipients := []wskit.SessionInfo{
		{ConnID: "phone", UserID: "user-1"},
		{ConnID: "web:1", UserID: "user-1"},
		{ConnID: "tablet", UserID: "user-1"},
		{ConnID: "laptop", UserID: "user-1"},
	}
	assertState := func(connID, want string) {
		t.Helper()
		got, err := store.State(ctx, messageID, connID)
		if err != nil || got != want {
			t.Fatalf("receipt for %q = %q, %v; want %q", connID, got, err, want)
		}
	}
	confirm := func(nodeID, connID, userID string) {
		t.Helper()
		if err := store.Confirm(ctx, messageID, nodeID, connID, userID); err != nil {
			t.Fatal(err)
		}
	}

	if err := store.Track(ctx, messageID, nodeID, recipients); err != nil {
		t.Fatal(err)
	}
	confirm(nodeID, "phone", "other-user")
	confirm("other-node", "phone", "user-1")
	confirm(nodeID, "unknown", "user-1")
	assertState("phone", "pending")
	assertState("unknown", "")
	confirm(nodeID, "phone", "user-1")
	confirm(nodeID, "phone", "user-1")
	assertState("phone", "done")
	assertState("web:1", "pending")

	now, err := store.client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	expired := float64(now.Add(-time.Second).UnixMilli())
	if err := store.client.ZAdd(ctx, store.deadlineKey,
		redis.Z{Score: expired, Member: id + ":web:1"},
		redis.Z{Score: expired, Member: id + ":tablet"},
	).Err(); err != nil {
		t.Fatal(err)
	}
	confirm(nodeID, "web:1", "user-1")
	assertState("web:1", "failed")
	if err := store.expirePending(ctx, now); err != nil {
		t.Fatal(err)
	}
	confirm(nodeID, "tablet", "user-1")
	assertState("tablet", "failed")
	assertState("laptop", "pending")

	if err := store.Track(ctx, messageID, nodeID, recipients); err != nil {
		t.Fatal(err)
	}
	assertState("phone", "done")
	assertState("web:1", "failed")
	assertState("tablet", "failed")
	assertState("laptop", "pending")
	if count, err := store.client.ZCard(ctx, store.deadlineKey).Result(); err != nil || count != 1 {
		t.Fatalf("remaining timeout entries = %d, %v; want 1", count, err)
	}
}
