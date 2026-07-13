package message

import (
	"context"
	"errors"
	"time"

	"socket_server/internal/ack"
	"socket_server/internal/domain"
	"socket_server/internal/route"
	"socket_server/internal/sender"
)

const maxGroupMemberCount = 10000

// Service 提供单聊、群聊、直播间消息的业务编排能力。
type Service struct {
	// store 表示消息持久化接口，当前第一版可为空实现。
	store Store
	// routeResolver 表示用户连接路由解析器。
	routeResolver route.Resolver
	// ackStore 表示 ACK 临时状态存储。
	ackStore ack.Store
	// senderSelector 表示发送器选择器。
	senderSelector sender.SenderSelector
}

// NewService 创建消息服务。
func NewService(store Store, routeResolver route.Resolver, ackStore ack.Store, senderSelector sender.SenderSelector) *Service {
	return &Service{store: store, routeResolver: routeResolver, ackStore: ackStore, senderSelector: senderSelector}
}

// attachDeliveryRoutes 将完整路由和连接编号同时挂到消息上，供 ACK 与发送器分别复用。
func attachDeliveryRoutes(msg *domain.Message, routes []*domain.ConnInfo) []string {
	connIDs := make([]string, 0, len(routes))
	for _, info := range routes {
		if info == nil || info.ConnID == "" {
			continue
		}
		connIDs = append(connIDs, info.ConnID)
	}
	msg.DeliveryRoutes = routes
	msg.ConnIDs = connIDs
	return connIDs
}

// SendSingleMessage 发送单聊消息，并默认推送到目标用户全部在线端。
func (s *Service) SendSingleMessage(ctx context.Context, msg *domain.Message) (*domain.SendResult, error) {
	if msg == nil || len(msg.ToUserIDs) == 0 || msg.ToUserIDs[0] == "" {
		return nil, errors.New("single message receiver is empty")
	}

	if msg.CreatedAt == 0 {
		msg.CreatedAt = time.Now().Unix()
	}
	msg.MessageID = EnsureMessageID("single", msg.MessageID)
	msg.BizType = "single"
	msg.AckRequired = true

	if err := s.store.SaveMessage(ctx, msg); err != nil {
		return nil, err
	}

	connIDs, err := s.routeResolver.GetUserConnIDs(ctx, msg.ToUserIDs[0])
	if err != nil {
		return nil, err
	}
	msg.ConnIDs = connIDs

	if err := s.ackStore.AddPending(ctx, msg.MessageID, connIDs); err != nil {
		return nil, err
	}

	selectedSender := s.senderSelector.Select(ctx, msg)
	return selectedSender.Send(ctx, msg)
}

// SendGroupMessage 发送中小群消息，并通过路由查询后按 endpoint 批量推送。
func (s *Service) SendGroupMessage(ctx context.Context, msg *domain.Message, memberUserIDs []string) (*domain.SendResult, error) {
	if msg == nil || msg.GroupID == "" || len(memberUserIDs) == 0 {
		return nil, errors.New("group message is incomplete")
	}
	if len(memberUserIDs) > maxGroupMemberCount {
		return nil, errors.New("group member count exceeds limit")
	}

	if msg.CreatedAt == 0 {
		msg.CreatedAt = time.Now().Unix()
	}
	msg.MessageID = EnsureMessageID("group", msg.MessageID)
	msg.BizType = "group"
	msg.AckRequired = true

	if err := s.store.SaveMessage(ctx, msg); err != nil {
		return nil, err
	}

	routes, err := s.routeResolver.ResolveUsers(ctx, memberUserIDs)
	if err != nil {
		return nil, err
	}
	connIDs := attachDeliveryRoutes(msg, routes)

	if err := s.ackStore.AddPending(ctx, msg.MessageID, connIDs); err != nil {
		return nil, err
	}

	selectedSender := s.senderSelector.Select(ctx, msg)
	return selectedSender.Send(ctx, msg)
}

// SendLiveDanmu 发送直播间弹幕类高频低价值消息。
func (s *Service) SendLiveDanmu(ctx context.Context, msg *domain.Message) (*domain.SendResult, error) {
	if msg == nil || msg.RoomID == "" {
		return nil, errors.New("live danmu room is empty")
	}

	if msg.CreatedAt == 0 {
		msg.CreatedAt = time.Now().Unix()
	}
	msg.MessageID = EnsureMessageID("live-danmu", msg.MessageID)
	msg.BizType = "live_danmu"
	msg.AckRequired = false

	if err := s.store.SaveMessage(ctx, msg); err != nil {
		return nil, err
	}

	selectedSender := s.senderSelector.Select(ctx, msg)
	return selectedSender.Send(ctx, msg)
}

// SendLiveNotice 发送直播间公告、状态变更等重要消息。
func (s *Service) SendLiveNotice(ctx context.Context, msg *domain.Message) (*domain.SendResult, error) {
	if msg == nil || msg.RoomID == "" {
		return nil, errors.New("live notice room is empty")
	}

	if msg.CreatedAt == 0 {
		msg.CreatedAt = time.Now().Unix()
	}
	msg.MessageID = EnsureMessageID("live-notice", msg.MessageID)
	msg.BizType = "live_notice"
	msg.AckRequired = false

	if err := s.store.SaveMessage(ctx, msg); err != nil {
		return nil, err
	}

	selectedSender := s.senderSelector.Select(ctx, msg)
	return selectedSender.Send(ctx, msg)
}

// MarkClientAck 记录客户端收到消息后的 ACK。
func (s *Service) MarkClientAck(ctx context.Context, messageID string, userID string, connID string, ackType string) error {
	if messageID == "" || connID == "" {
		return errors.New("ack message_id or conn_id is empty")
	}

	if err := s.ackStore.MarkDone(ctx, messageID, connID); err != nil {
		return err
	}
	return s.store.SaveClientAck(ctx, messageID, userID, connID, ackType)
}
