package ack

import (
	"context"
	"testing"
)

func TestMemoryStoreDoesNotMoveFinishedAckAgain(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.AddPending(ctx, "msg-1", []string{"conn-1"}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkFailed(ctx, "msg-1", "conn-1", "timeout"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDone(ctx, "msg-1", "conn-1"); err != nil {
		t.Fatal(err)
	}
	done, err := store.GetDone(ctx, "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	failed, err := store.GetFailed(ctx, "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 0 || len(failed) != 1 || failed[0] != "conn-1" {
		t.Fatalf("unexpected terminal ACK state done=%v failed=%v", done, failed)
	}
}
