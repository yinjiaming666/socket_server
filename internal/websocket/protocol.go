package websocket

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"socket_server/internal/domain"
)

const (
	// clientProtocolChannel 是客户端与 socket server 通信时固定使用的协议通道。
	clientProtocolChannel = domain.ClientChannel
)

// clientEnvelope 是 WebSocket JSON 消息的统一信封。
//
// 所有客户端控制消息和服务端业务响应均使用 c / m / data 三段式结构。
// c 固定为 socket_server，m 表示具体方法，data 承载该方法的参数。
type clientEnvelope struct {
	C    string          `json:"c"`
	M    string          `json:"m"`
	Data json.RawMessage `json:"data"`
}

// clientRoomData 是 join_room、leave_room 方法的数据结构。
type clientRoomData struct {
	RoomID string `json:"room_id"`
}

// clientAckData 是 ack 方法的数据结构。
type clientAckData struct {
	MessageID string `json:"message_id"`
}

// clientErrorData 是服务端 error 方法的数据结构。
type clientErrorData struct {
	Message string `json:"message"`
}

// decodeClientEnvelope 严格解析客户端统一信封，不兼容旧版顶层 type 字段。
func decodeClientEnvelope(payload []byte) (*clientEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()

	var envelope clientEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, fmt.Errorf("decode websocket envelope: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode websocket envelope: %w", err)
	}
	if envelope.C != clientProtocolChannel {
		return nil, errors.New("unsupported websocket channel")
	}
	if envelope.M == "" {
		return nil, errors.New("websocket method is empty")
	}
	if len(bytes.TrimSpace(envelope.Data)) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Data), []byte("null")) {
		return nil, errors.New("websocket data is required")
	}
	return &envelope, nil
}

// decodeClientData 严格解析方法专属 data，拒绝未知字段和拼接 JSON。
func decodeClientData(data json.RawMessage, target any) error {
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

// marshalClientControl 构造服务端控制类 WebSocket JSON 消息。
func marshalClientControl(method string, data any) ([]byte, error) {
	if method == "" {
		return nil, errors.New("websocket method is empty")
	}
	return json.Marshal(struct {
		C    string `json:"c"`
		M    string `json:"m"`
		Data any    `json:"data"`
	}{
		C:    clientProtocolChannel,
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
