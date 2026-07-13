package websocket

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"socket_server/internal/domain"
)

func TestDecodeClientEnvelope(t *testing.T) {
	envelope, err := decodeClientEnvelope([]byte(`{"c":"socket_server","m":"join_room","data":{"room_id":"room-1"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if envelope.C != domain.ClientChannel || envelope.M != "join_room" {
		t.Fatalf("unexpected envelope: %#v", envelope)
	}

	var data clientRoomData
	if err := decodeClientData(envelope.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.RoomID != "room-1" {
		t.Fatalf("room_id = %q, want room-1", data.RoomID)
	}
}

func TestDecodeClientEnvelopeRejectsLegacyTypeMessage(t *testing.T) {
	if _, err := decodeClientEnvelope([]byte(`{"type":"ping"}`)); err == nil {
		t.Fatal("legacy top-level type message should be rejected")
	}
}

func TestMarshalClientControlUsesEnvelope(t *testing.T) {
	body, err := marshalClientControl("pong", struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		C    string          `json:"c"`
		M    string          `json:"m"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.C != domain.ClientChannel || envelope.M != "pong" || string(envelope.Data) != "{}" {
		t.Fatalf("unexpected control message: %s", body)
	}
}

func TestHandleClientPingUsesEnvelope(t *testing.T) {
	handler := NewHandler(nil)
	client := NewClient("conn-1", "user-1", "device-1", "test", 1)
	var response []byte
	client.AttachWriter(func(payload []byte) error {
		response = append([]byte(nil), payload...)
		return nil
	})

	request := httptest.NewRequest("GET", "/", nil)
	if err := handler.handleClientMessage(request, client, []byte(`{"c":"socket_server","m":"ping","data":{}}`)); err != nil {
		t.Fatal(err)
	}

	var envelope struct {
		C    string          `json:"c"`
		M    string          `json:"m"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.C != domain.ClientChannel || envelope.M != "pong" || string(envelope.Data) != "{}" {
		t.Fatalf("unexpected ping response: %s", response)
	}
}
