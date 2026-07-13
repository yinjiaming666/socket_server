package domain

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestMarshalClientMessageHidesInternalRoutes(t *testing.T) {
	body, err := MarshalClientMessage(&Message{
		MessageID:   "msg-1",
		FromUserID:  "user-1",
		ConnIDs:     []string{"secret-conn-1", "secret-conn-2"},
		BizType:     "single",
		Payload:     []byte(`{"text":"hello"}`),
		AckRequired: true,
		CreatedAt:   123,
	})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(body, []byte("secret-conn")) {
		t.Fatalf("client payload leaked connection routes: %s", body)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	payload, ok := decoded["payload"].(map[string]any)
	if !ok || payload["text"] != "hello" {
		t.Fatalf("payload should remain JSON instead of Base64: %#v", decoded["payload"])
	}
}
