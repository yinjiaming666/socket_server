package gateway

import (
	"context"
	"errors"
	"time"

	"socket_server/internal/domain"
	"socket_server/internal/endpoint"
	"socket_server/internal/route"
)

// EndpointSelector 定义 Gateway 选择 WebSocket endpoint 的接口。
type EndpointSelector interface {
	// SelectEndpoint 为指定用户选择可用的 WebSocket endpoint。
	SelectEndpoint(ctx context.Context, userID string) (*domain.Endpoint, error)
}

// DefaultEndpointSelector 是按连接负载和最近连接节点选择 endpoint 的默认实现。
type DefaultEndpointSelector struct {
	// endpointRepo 表示 WebSocket 节点仓储。
	endpointRepo endpoint.Repository
	// routeResolver 表示连接路由解析器。
	routeResolver route.Resolver
	// heartbeatTimeout 表示节点心跳超时时间。
	heartbeatTimeout time.Duration
	// lastEndpointTTL 表示最近连接节点缓存时间。
	lastEndpointTTL time.Duration
}

// NewDefaultEndpointSelector 创建默认 endpoint 选择器。
func NewDefaultEndpointSelector(endpointRepo endpoint.Repository, routeResolver route.Resolver, heartbeatTimeout time.Duration, lastEndpointTTL time.Duration) *DefaultEndpointSelector {
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = 60 * time.Second
	}
	if lastEndpointTTL <= 0 {
		lastEndpointTTL = 10 * time.Minute
	}
	return &DefaultEndpointSelector{
		endpointRepo:     endpointRepo,
		routeResolver:    routeResolver,
		heartbeatTimeout: heartbeatTimeout,
		lastEndpointTTL:  lastEndpointTTL,
	}
}

// SelectEndpoint 为指定用户选择可用的 WebSocket endpoint。
func (s *DefaultEndpointSelector) SelectEndpoint(ctx context.Context, userID string) (*domain.Endpoint, error) {
	lastEndpointID, _ := s.routeResolver.GetLastEndpoint(ctx, userID)
	if lastEndpointID != "" {
		ep, err := s.endpointRepo.GetEndpoint(ctx, lastEndpointID)
		if err == nil && s.isEndpointAvailable(ep) {
			return ep, nil
		}
	}

	endpoints, err := s.endpointRepo.ListEndpoints(ctx)
	if err != nil {
		return nil, err
	}

	var selected *domain.Endpoint
	bestScore := 999999.0
	for _, ep := range endpoints {
		if !s.isEndpointAvailable(ep) {
			continue
		}
		score := float64(ep.ConnCount) / float64(ep.MaxConn)
		if selected == nil || score < bestScore {
			copied := *ep
			selected = &copied
			bestScore = score
		}
	}

	if selected == nil {
		return nil, errors.New("no available websocket endpoint")
	}

	_ = s.routeResolver.SetLastEndpoint(ctx, userID, selected.ID, s.lastEndpointTTL)
	return selected, nil
}

// isEndpointAvailable 判断 endpoint 是否可用。
func (s *DefaultEndpointSelector) isEndpointAvailable(ep *domain.Endpoint) bool {
	if ep == nil {
		return false
	}
	if ep.Status != "online" {
		return false
	}
	if ep.MaxConn <= 0 || ep.ConnCount >= ep.MaxConn {
		return false
	}
	if time.Since(time.Unix(ep.UpdatedAt, 0)) > s.heartbeatTimeout {
		return false
	}
	return true
}
