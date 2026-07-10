package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"socket_server/internal/ack"
	"socket_server/internal/domain"
	"socket_server/internal/endpoint"
	"socket_server/internal/gateway"
	"socket_server/internal/route"
)

const connRouteTTL = 180 * time.Second

// NodeConfig 表示 WebSocket Node 启动配置。
type NodeConfig struct {
	EndpointID        string
	WSPath            string
	WSAddr            string
	GRPCAddr          string
	MaxConn           int64
	SendBuffer        int
	HeartbeatInterval time.Duration
}

// Node 表示一个 WebSocket 长连接节点。
type Node struct {
	config        NodeConfig
	endpointRepo  endpoint.Repository
	routeResolver route.Resolver
	ackStore      ack.Store
	tokenManager  *gateway.RouteTokenManager
	roomHub       *RoomHub
	mu            sync.RWMutex
	clients       map[string]*Client
	userConns     map[string]map[string]struct{}
	stopCh        chan struct{}
	stopOnce      sync.Once
}

// NewNode 创建 WebSocket Node。
func NewNode(config NodeConfig, endpointRepo endpoint.Repository, routeResolver route.Resolver, ackStore ack.Store, tokenManager *gateway.RouteTokenManager) *Node {
	if config.MaxConn <= 0 {
		config.MaxConn = 50000
	}
	if config.SendBuffer <= 0 {
		config.SendBuffer = 128
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 15 * time.Second
	}
	return &Node{config: config, endpointRepo: endpointRepo, routeResolver: routeResolver, ackStore: ackStore, tokenManager: tokenManager, roomHub: NewRoomHub(), clients: make(map[string]*Client), userConns: make(map[string]map[string]struct{}), stopCh: make(chan struct{})}
}

// Start 启动节点注册和心跳上报。
func (n *Node) Start(ctx context.Context) error {
	if err := n.HeartbeatOnce(ctx, "online"); err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(n.config.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = n.HeartbeatOnce(context.Background(), "online")
			case <-n.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// Stop 将节点标记为 draining 并清理本节点连接路由。
func (n *Node) Stop(ctx context.Context) error {
	n.stopOnce.Do(func() { close(n.stopCh) })
	_ = n.HeartbeatOnce(ctx, "draining")
	n.mu.Lock()
	connIDs := make([]string, 0, len(n.clients))
	for connID := range n.clients {
		connIDs = append(connIDs, connID)
	}
	n.mu.Unlock()
	for _, connID := range connIDs {
		_ = n.RemoveConnection(ctx, connID)
	}
	return n.endpointRepo.DeleteEndpoint(ctx, n.config.EndpointID)
}

// HeartbeatOnce 上报一次 endpoint 状态和连接数量，并刷新本节点连接路由 TTL。
func (n *Node) HeartbeatOnce(ctx context.Context, status string) error {
	n.mu.RLock()
	connCount := int64(len(n.clients))
	n.mu.RUnlock()
	ep := &domain.Endpoint{ID: n.config.EndpointID, Status: status, WSPath: n.config.WSPath, WSAddr: n.config.WSAddr, GRPCAddr: n.config.GRPCAddr, ConnCount: connCount, MaxConn: n.config.MaxConn, UpdatedAt: time.Now().Unix()}
	if err := n.endpointRepo.UpsertEndpoint(ctx, ep); err != nil {
		return err
	}
	if status == "online" && connCount > 0 {
		_ = n.routeResolver.RefreshEndpointConns(ctx, n.config.EndpointID, connRouteTTL)
	}
	return nil
}

// RegisterConnection 校验 route_token 并注册一条用户连接。
func (n *Node) RegisterConnection(ctx context.Context, connID string, routeToken string, deviceID string, platform string) (*Client, error) {
	if connID == "" || routeToken == "" {
		return nil, errors.New("conn_id or route_token is empty")
	}
	claims, err := n.tokenManager.Verify(routeToken)
	if err != nil {
		return nil, err
	}
	if claims.EndpointID != n.config.EndpointID {
		return nil, errors.New("route token endpoint mismatch")
	}
	client := NewClient(connID, claims.UserID, deviceID, platform, n.config.SendBuffer)
	n.mu.Lock()
	if int64(len(n.clients)) >= n.config.MaxConn {
		n.mu.Unlock()
		return nil, errors.New("websocket node is full")
	}
	if oldClient := n.clients[connID]; oldClient != nil {
		delete(n.userConns[oldClient.UserID], connID)
		if len(n.userConns[oldClient.UserID]) == 0 {
			delete(n.userConns, oldClient.UserID)
		}
		oldClient.Close()
	}
	n.clients[connID] = client
	if _, ok := n.userConns[claims.UserID]; !ok {
		n.userConns[claims.UserID] = make(map[string]struct{})
	}
	n.userConns[claims.UserID][connID] = struct{}{}
	n.mu.Unlock()
	info := &domain.ConnInfo{ConnID: connID, UserID: claims.UserID, EndpointID: n.config.EndpointID, DeviceID: deviceID, Platform: platform, GRPCAddr: n.config.GRPCAddr, CreatedAt: time.Now().Unix(), UpdatedAt: time.Now().Unix()}
	if err := n.routeResolver.RegisterConn(ctx, info); err != nil {
		_ = n.RemoveConnection(ctx, connID)
		return nil, err
	}
	_ = n.HeartbeatOnce(ctx, "online")
	return client, nil
}

// RemoveConnection 移除一条本地连接并清理路由。
func (n *Node) RemoveConnection(ctx context.Context, connID string) error {
	n.mu.Lock()
	client := n.clients[connID]
	if client != nil {
		delete(n.clients, connID)
		delete(n.userConns[client.UserID], connID)
		if len(n.userConns[client.UserID]) == 0 {
			delete(n.userConns, client.UserID)
		}
	}
	n.mu.Unlock()
	if client != nil {
		n.roomHub.RemoveConn(connID)
		client.Close()
	}
	if err := n.routeResolver.RemoveConn(ctx, connID); err != nil {
		return err
	}
	return n.HeartbeatOnce(ctx, "online")
}

// JoinRoom 将连接加入本节点直播间。
func (n *Node) JoinRoom(ctx context.Context, connID string, roomID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if roomID == "" {
		return errors.New("room_id is empty")
	}
	n.mu.RLock()
	client := n.clients[connID]
	n.mu.RUnlock()
	if client == nil {
		return errors.New("conn not found")
	}
	n.roomHub.Join(roomID, client)
	return nil
}

// LeaveRoom 将连接移出本节点直播间。
func (n *Node) LeaveRoom(ctx context.Context, connID string, roomID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if roomID == "" {
		return errors.New("room_id is empty")
	}
	n.roomHub.Leave(roomID, connID)
	return nil
}

// BroadcastRoom 向本节点指定直播间广播消息。
func (n *Node) BroadcastRoom(ctx context.Context, roomID string, payload []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return n.roomHub.Broadcast(roomID, payload), nil
}

// PushToConns 将消息写入本节点指定连接的发送队列。
func (n *Node) PushToConns(ctx context.Context, connIDs []string, msg *domain.Message) (*domain.PushResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, errors.New("message is nil")
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	resp := &domain.PushResponse{Accepted: true}
	for _, connID := range connIDs {
		n.mu.RLock()
		client := n.clients[connID]
		n.mu.RUnlock()
		if client == nil || !client.Enqueue(payload) {
			resp.FailedConnIDs = append(resp.FailedConnIDs, connID)
			continue
		}
		resp.AcceptedConnIDs = append(resp.AcceptedConnIDs, connID)
	}
	if len(resp.FailedConnIDs) > 0 {
		resp.Accepted = false
		resp.Reason = "some connections failed"
	}
	return resp, nil
}

// PushToUsers 将消息写入本节点指定用户的发送队列。
func (n *Node) PushToUsers(ctx context.Context, userIDs []string, msg *domain.Message) (*domain.PushResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	connIDs := make([]string, 0)
	n.mu.RLock()
	for _, userID := range userIDs {
		for connID := range n.userConns[userID] {
			connIDs = append(connIDs, connID)
		}
	}
	n.mu.RUnlock()
	return n.PushToConns(ctx, connIDs, msg)
}

// HandleClientAck 处理客户端通过 WebSocket 上报的 ACK。
func (n *Node) HandleClientAck(ctx context.Context, messageID string, connID string) error {
	if messageID == "" || connID == "" {
		return errors.New("ack message_id or conn_id is empty")
	}
	return n.ackStore.MarkDone(ctx, messageID, connID)
}

// ConnCount 返回当前节点本地连接数量。
func (n *Node) ConnCount() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return int64(len(n.clients))
}

// RoomConns 返回指定直播间在本节点的连接编号列表。
func (n *Node) RoomConns(roomID string) []string { return n.roomHub.ListConns(roomID) }
