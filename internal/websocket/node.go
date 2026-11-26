package websocket

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/yinjiaming666/wskit"

	"socket_server/internal/ack"
	"socket_server/internal/domain"
)

// NewNode 绑定 socket_server 的 WebSocket 协议和推送回执。
func NewNode(cfg wskit.NodeConfig, backend *wskit.Backend, acks *ack.Store) (*wskit.Node, error) {
	protocol := clientProtocol{endpointID: cfg.EndpointID, acks: acks}
	cfg.BeforePush = protocol.beforePush
	cfg.OnMessage = protocol.handleFrame
	return wskit.CreateNode(cfg, backend)
}

type clientProtocol struct {
	endpointID string
	acks       *ack.Store
}

func (p clientProtocol) beforePush(ctx context.Context, frame wskit.Frame, recipients []wskit.SessionInfo) error {
	if frame.Type != wskit.TextFrame {
		return nil
	}
	var message struct {
		C    string `json:"c"`
		M    string `json:"m"`
		Data struct {
			MessageID   string `json:"message_id"`
			AckRequired bool   `json:"ack_required"`
		} `json:"data"`
	}
	if err := json.Unmarshal(frame.Data, &message); err != nil {
		return nil
	}
	if message.C != domain.ClientChannel || message.M != domain.ClientMessageMethod || !message.Data.AckRequired {
		return nil
	}
	return p.acks.Track(ctx, message.Data.MessageID, p.endpointID, recipients)
}

type clientErrorData struct {
	Message string `json:"message"`
}

func (p clientProtocol) handleFrame(ctx context.Context, session *wskit.Session, frame wskit.Frame) error {
	if err := p.handleMessage(ctx, session, frame.Data); err != nil {
		return sendControl(session, "error", clientErrorData{Message: err.Error()})
	}
	return nil
}

func (p clientProtocol) handleMessage(ctx context.Context, session *wskit.Session, payload []byte) error {
	envelope, err := domain.DecodeClientEnvelope(payload)
	if err != nil {
		return err
	}

	switch envelope.M {
	case "ack":
		var data domain.ClientAckData
		if err := domain.DecodeClientData(envelope.Data, &data); err != nil {
			return err
		}
		return p.acks.Confirm(ctx, data.MessageID, p.endpointID, session.ConnID(), session.UserID())
	case "join_room", "leave_room":
		var data domain.ClientRoomData
		if err := domain.DecodeClientData(envelope.Data, &data); err != nil {
			return err
		}
		if envelope.M == "join_room" {
			return session.Subscribe(ctx, data.RoomID)
		}
		return session.Unsubscribe(ctx, data.RoomID)
	case "ping":
		var data struct{}
		if err := domain.DecodeClientData(envelope.Data, &data); err != nil {
			return err
		}
		return sendControl(session, "pong", struct{}{})
	default:
		return errors.New("unsupported websocket method")
	}
}

func sendControl(session *wskit.Session, method string, data any) error {
	payload, err := domain.MarshalClientControl(method, data)
	if err != nil {
		return err
	}
	return session.Send(wskit.Frame{Type: wskit.TextFrame, Data: payload})
}
