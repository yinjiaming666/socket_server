package grpcpush

import (
	websocketpb "socket_server/api/proto/websocket"
	"socket_server/internal/domain"
)

// MessageToProto 将领域消息转换为 protobuf 消息。
func MessageToProto(msg *domain.Message) *websocketpb.PushMessage {
	if msg == nil {
		return nil
	}
	// 目标连接由 PushToConnsRequest.ConnIds 单独携带，消息体不再重复传输内部路由列表。
	return &websocketpb.PushMessage{
		MessageId:   msg.MessageID,
		FromUserId:  msg.FromUserID,
		BizType:     msg.BizType,
		Payload:     msg.Payload,
		AckRequired: msg.AckRequired,
		CreatedAt:   msg.CreatedAt,
		ToUserIds:   msg.ToUserIDs,
		GroupId:     msg.GroupID,
		RoomId:      msg.RoomID,
	}
}

// MessageFromProto 将 protobuf 消息转换为领域消息。
func MessageFromProto(msg *websocketpb.PushMessage) *domain.Message {
	if msg == nil {
		return nil
	}
	return &domain.Message{
		MessageID:   msg.GetMessageId(),
		FromUserID:  msg.GetFromUserId(),
		BizType:     msg.GetBizType(),
		Payload:     msg.GetPayload(),
		AckRequired: msg.GetAckRequired(),
		CreatedAt:   msg.GetCreatedAt(),
		ToUserIDs:   msg.GetToUserIds(),
		ConnIDs:     msg.GetConnIds(),
		GroupID:     msg.GetGroupId(),
		RoomID:      msg.GetRoomId(),
	}
}

// PushResponseToProto 将领域响应转换为 protobuf 响应。
func PushResponseToProto(resp *domain.PushResponse) *websocketpb.PushResponse {
	if resp == nil {
		return &websocketpb.PushResponse{Accepted: false, Reason: "nil response"}
	}
	return &websocketpb.PushResponse{
		Accepted:        resp.Accepted,
		AcceptedConnIds: resp.AcceptedConnIDs,
		FailedConnIds:   resp.FailedConnIDs,
		Reason:          resp.Reason,
	}
}

// PushResponseFromProto 将 protobuf 响应转换为领域响应。
func PushResponseFromProto(resp *websocketpb.PushResponse) *domain.PushResponse {
	if resp == nil {
		return &domain.PushResponse{Accepted: false, Reason: "nil response"}
	}
	return &domain.PushResponse{
		Accepted:        resp.GetAccepted(),
		AcceptedConnIDs: resp.GetAcceptedConnIds(),
		FailedConnIDs:   resp.GetFailedConnIds(),
		Reason:          resp.GetReason(),
	}
}
