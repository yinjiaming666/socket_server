package test

import (
	"encoding/json"
	"testing"

	"socket_server/internal/domain"
)

func TestDecodeClientEnvelope(t *testing.T) {
	envelope, err := domain.DecodeClientEnvelope([]byte(`{"c":"socket_server","m":"join_room","data":{"room_id":"room-1"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if envelope.C != domain.ClientChannel || envelope.M != "join_room" {
		t.Fatalf("unexpected envelope: %#v", envelope)
	}

	var data domain.ClientRoomData
	if err := domain.DecodeClientData(envelope.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.RoomID != "room-1" {
		t.Fatalf("room_id = %q, want room-1", data.RoomID)
	}
}

func TestDecodeClientEnvelopeRejectsLegacyTypeMessage(t *testing.T) {
	if _, err := domain.DecodeClientEnvelope([]byte(`{"type":"ping"}`)); err == nil {
		t.Fatal("legacy top-level type message should be rejected")
	}
}

func TestMarshalClientControlUsesEnvelope(t *testing.T) {
	body, err := domain.MarshalClientControl("pong", struct{}{})
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
