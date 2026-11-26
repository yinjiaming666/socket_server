package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ClientEnvelope 是 WebSocket JSON 消息的统一信封。
//
// 所有客户端控制消息和服务端业务响应均使用 c / m / data 三段式结构。
// c 固定为 socket_server，m 表示具体方法，data 承载该方法的参数。
type ClientEnvelope struct {
	C    string          `json:"c"`
	M    string          `json:"m"`
	Data json.RawMessage `json:"data"`
}

// ClientRoomData 是 join_room、leave_room 方法的数据结构。
type ClientRoomData struct {
	RoomID string `json:"room_id"`
}

// ClientAckData 是 ack 方法的数据结构。
type ClientAckData struct {
	MessageID string `json:"message_id"`
}

// ValidateAckMessageID 校验需要确认的消息编号。
func ValidateAckMessageID(id string) error {
	if id == "" {
		return errors.New("message_id is required")
	}
	return nil
}

// DecodeClientEnvelope 严格解析 c / m / data 信封。
func DecodeClientEnvelope(payload []byte) (*ClientEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()

	var envelope ClientEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode websocket envelope: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode websocket envelope: %w", err)
	}
	if envelope.C != ClientChannel {
		return nil, errors.New("unsupported websocket channel")
	}
	if envelope.M == "" {
		return nil, errors.New("websocket method is empty")
	}
	data := bytes.TrimSpace(envelope.Data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil, errors.New("websocket data is required")
	}
	return &envelope, nil
}

// DecodeClientData 严格解析方法专属 data，拒绝未知字段和拼接 JSON。
func DecodeClientData(data json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode websocket data: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode websocket data: %w", err)
	}
	return nil
}

// MarshalClientControl 构造 WebSocket 控制消息。
func MarshalClientControl(method string, data any) ([]byte, error) {
	if method == "" {
		return nil, errors.New("websocket method is empty")
	}
	return json.Marshal(struct {
		C    string `json:"c"`
		M    string `json:"m"`
		Data any    `json:"data"`
	}{
		C:    ClientChannel,
		M:    method,
		Data: data,
	})
}

// ensureJSONEOF 确保一个 WebSocket 帧中只包含一段 JSON。
func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return errors.New("multiple JSON values are not allowed")
}
