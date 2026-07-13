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
	var decoded struct {
		C    string `json:"c"`
		M    string `json:"m"`
		Data struct {
			Payload map[string]any `json:"payload"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.C != ClientChannel || decoded.M != ClientMessageMethod {
		t.Fatalf("unexpected client envelope c=%q m=%q", decoded.C, decoded.M)
	}
	if decoded.Data.Payload["text"] != "hello" {
		t.Fatalf("payload should remain JSON instead of Base64: %#v", decoded.Data.Payload)
	}
}
