package message

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/yinjiaming666/wskit"

	"socket_server/internal/domain"
)

const maxGroupMemberCount = 10000

// Service 提供单聊、群聊、直播间消息的业务编排能力。
type Service struct {
	sender  *wskit.Sender
	notices *NoticeStore
}

func NewService(sender *wskit.Sender, notices *NoticeStore) *Service {
	return &Service{sender: sender, notices: notices}
}

func (s *Service) SendSingleMessage(ctx context.Context, msg *domain.Message) (*wskit.DeliveryResult, error) {
	if msg == nil || len(msg.ToUserIDs) == 0 || msg.ToUserIDs[0] == "" {
		return nil, errors.New("single message receiver is empty")
	}
	frame, err := s.prepareMessage(ctx, msg, "single", true)
	if err != nil {
		return nil, err
	}
	delivery, err := s.sender.Send(ctx, msg.ToUserIDs[0], frame)
	return &delivery.DeliveryResult, err
}

func (s *Service) SendGroupMessage(ctx context.Context, msg *domain.Message, memberUserIDs []string) (*wskit.DeliveryResult, error) {
	if msg == nil || msg.GroupID == "" || len(memberUserIDs) == 0 {
		return nil, errors.New("group message is incomplete")
	}
	if len(memberUserIDs) > maxGroupMemberCount {
		return nil, errors.New("group member count exceeds limit")
	}
	frame, err := s.prepareMessage(ctx, msg, "group", true)
	if err != nil {
		return nil, err
	}

	result := &wskit.DeliveryResult{Accepted: true}
	seen := make(map[string]struct{}, len(memberUserIDs))
	var sendErr error
	for _, userID := range memberUserIDs {
		if _, exists := seen[userID]; exists || userID == "" {
			continue
		}
		seen[userID] = struct{}{}
		delivery, err := s.sender.Send(ctx, userID, frame)
		if err != nil {
			if sendErr == nil {
				sendErr = err
			}
			if ctx.Err() != nil {
				break
			}
			continue
		}
		result.Accepted = result.Accepted && delivery.Accepted
		result.AcceptedConnIDs = append(result.AcceptedConnIDs, delivery.AcceptedConnIDs...)
		result.FailedConnIDs = append(result.FailedConnIDs, delivery.FailedConnIDs...)
		if result.Reason == "" {
			result.Reason = delivery.Reason
		}
	}
	if sendErr != nil {
		if len(result.AcceptedConnIDs) == 0 && len(result.FailedConnIDs) == 0 {
			return nil, sendErr
		}
		result.Accepted = false
		if result.Reason == "" {
			result.Reason = sendErr.Error()
		}
	}
	return result, nil
}

func (s *Service) SendLiveMessage(ctx context.Context, msg *domain.Message, notice bool) (*wskit.DeliveryResult, error) {
	if msg == nil || msg.RoomID == "" || len(msg.RoomID) > 128 {
		return nil, errors.New("invalid room_id")
	}
	bizType := "live_danmu"
	if notice {
		bizType = "live_notice"
	}
	frame, err := s.prepareMessage(ctx, msg, bizType, false)
	if err != nil {
		return nil, err
	}
	if notice {
		if err := s.notices.save(ctx, msg, frame.Frame.Data); err != nil {
			return nil, err
		}
	}
	if err := s.sender.SendToChannel(ctx, msg.RoomID, frame); err != nil {
		return nil, err
	}
	return &wskit.DeliveryResult{Accepted: true}, nil
}

func (s *Service) prepareMessage(ctx context.Context, msg *domain.Message, bizType string, requiresAck bool) (wskit.Message, error) {
	if msg.CreatedAt == 0 {
		msg.CreatedAt = time.Now().Unix()
	}
	msg.MessageID = EnsureMessageID(strings.ReplaceAll(bizType, "_", "-"), msg.MessageID)
	msg.BizType = bizType
	msg.AckRequired = requiresAck
	frame, err := domain.EncodeMessageFrame(msg)
	if err != nil {
		return wskit.Message{}, err
	}
	if err := ctx.Err(); err != nil {
		return wskit.Message{}, err
	}
	return wskit.Message{ID: msg.MessageID, Frame: frame}, nil
}
