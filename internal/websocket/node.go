package websocket

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"socket_server/internal/ack"
	"socket_server/internal/domain"
	"socket_server/internal/endpoint"
	"socket_server/internal/gateway"
	"socket_server/internal/liveroute"
	"socket_server/internal/route"
)

const (
	// connRouteTTL 与 Redis 默认值一致，节点每次心跳都会批量续期。
	connRouteTTL             = 30 * time.Second
	liveRouteRefreshInterval = liveroute.DefaultLeaseTTL / 3
	maxIDLength              = 128
)

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
	config           NodeConfig
	endpointRepo     endpoint.Repository
	routeResolver    route.Resolver
	ackStore         ack.Store
	tokenManager     *gateway.RouteTokenManager
	roomHub          *RoomHub
	liveRoomRegistry liveroute.Registry // 保存房间到承载节点的短租约路由，内存模式下可为空。
	roomTransitionMu sync.Mutex         // 串行化首个加入和最后离开，保证本地状态与租约同步。
	mu               sync.RWMutex
	clients          map[string]*Client
	userConns        map[string]map[string]struct{}
	stopping         bool
	stopCh           chan struct{}
	stopOnce         sync.Once
	stopErr          error
	connLocks        [256]sync.Mutex
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

// SetLiveRoomRegistry 设置直播间节点租约表；该方法应在 Start 之前调用。
func (n *Node) SetLiveRoomRegistry(registry liveroute.Registry) {
	n.liveRoomRegistry = registry
}

// Start 启动节点注册、连接路由续期和直播间租约刷新。
func (n *Node) Start(ctx context.Context) error {
	if err := n.HeartbeatOnce(ctx, "online"); err != nil {
		return err
	}
	go func() {
		heartbeatTicker := time.NewTicker(n.config.HeartbeatInterval)
		defer heartbeatTicker.Stop()
		// 连接绑定 TTL 为 30 秒，独立每 10 秒续期，避免业务心跳配置影响路由存活。
		routeRefreshTicker := time.NewTicker(connRouteTTL / 3)
		defer routeRefreshTicker.Stop()

		// nil channel 会永久阻塞对应 select 分支，使内存模式无需额外协程或条件判断。
		var liveRouteTick <-chan time.Time
		var liveRouteTicker *time.Ticker
		if n.liveRoomRegistry != nil {
			liveRouteTicker = time.NewTicker(liveRouteRefreshInterval)
			liveRouteTick = liveRouteTicker.C
			defer liveRouteTicker.Stop()
		}

		for {
			select {
			case <-heartbeatTicker.C:
				heartbeatCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := n.HeartbeatOnce(heartbeatCtx, "online"); err != nil {
					log.Printf("websocket heartbeat failed endpoint=%s error=%v", n.config.EndpointID, err)
				}
				cancel()
			case <-routeRefreshTicker.C:
				refreshCtx, cancel := context.WithTimeout(context.Background(), connRouteTTL/3)
				if err := n.routeResolver.RefreshEndpointConns(refreshCtx, n.config.EndpointID, connRouteTTL); err != nil {
					log.Printf("websocket route refresh failed endpoint=%s error=%v", n.config.EndpointID, err)
				}
				cancel()
			case <-liveRouteTick:
				refreshCtx, cancel := context.WithTimeout(context.Background(), liveRouteRefreshInterval/2)
				if err := n.refreshLiveRoomRoutes(refreshCtx); err != nil {
					log.Printf("websocket live route refresh failed endpoint=%s error=%v", n.config.EndpointID, err)
				}
				cancel()
			case <-n.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// refreshLiveRoomRoutes 用本地房间快照刷新节点短租约，并与加入、离开操作串行化。
func (n *Node) refreshLiveRoomRoutes(ctx context.Context) error {
	if n.liveRoomRegistry == nil {
		return nil
	}
	n.roomTransitionMu.Lock()
	defer n.roomTransitionMu.Unlock()
	return n.liveRoomRegistry.RefreshNodeRooms(ctx, n.config.EndpointID, n.roomHub.ListRooms())
}

// Stop 将节点标记为 draining 并批量清理本节点连接路由。
func (n *Node) Stop(ctx context.Context) error {
	n.stopOnce.Do(func() {
		close(n.stopCh)

		n.mu.Lock()
		n.stopping = true
		n.mu.Unlock()

		// 等待所有正在注册或删除的连接完成，防止停止清理后又写回 Redis 路由。
		for index := range n.connLocks {
			n.connLocks[index].Lock()
		}
		defer func() {
			for index := len(n.connLocks) - 1; index >= 0; index-- {
				n.connLocks[index].Unlock()
			}
		}()

		n.mu.Lock()
		clients := make([]*Client, 0, len(n.clients))
		for _, client := range n.clients {
			clients = append(clients, client)
		}
		n.clients = make(map[string]*Client)
		n.userConns = make(map[string]map[string]struct{})
		n.mu.Unlock()

		if err := n.HeartbeatOnce(ctx, "draining"); err != nil {
			n.stopErr = err
		}
		// 停止期间阻止新的房间状态变更，并用停止前快照清理节点全部直播租约。
		n.roomTransitionMu.Lock()
		roomIDs := n.roomHub.ListRooms()
		for _, client := range clients {
			n.roomHub.RemoveClient(client)
			client.Close()
		}
		if n.liveRoomRegistry != nil {
			if err := n.liveRoomRegistry.RemoveNode(ctx, n.config.EndpointID, roomIDs); err != nil && n.stopErr == nil {
				n.stopErr = err
			}
		}
		n.roomTransitionMu.Unlock()

		if err := n.routeResolver.RemoveEndpointConns(ctx, n.config.EndpointID); err != nil && n.stopErr == nil {
			n.stopErr = err
		}
		if err := n.endpointRepo.DeleteEndpoint(ctx, n.config.EndpointID); err != nil && n.stopErr == nil {
			n.stopErr = err
		}
	})
	return n.stopErr
}

// BeginDrain 将节点标记为 draining，Gateway 将停止分配新连接，现有连接继续服务到部署超时。
func (n *Node) BeginDrain(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.mu.Lock()
	n.stopping = true
	n.mu.Unlock()
	return n.HeartbeatOnce(ctx, "draining")
}

// Ready 返回节点是否仍可接收新连接。
func (n *Node) Ready() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return !n.stopping
}

// HeartbeatOnce 上报一次 endpoint 状态和连接数量。
func (n *Node) HeartbeatOnce(ctx context.Context, status string) error {
	n.mu.RLock()
	connCount := int64(len(n.clients))
	stopping := n.stopping
	n.mu.RUnlock()
	if stopping && status == "online" {
		status = "draining"
	}
	ep := &domain.Endpoint{ID: n.config.EndpointID, Status: status, WSPath: n.config.WSPath, WSAddr: n.config.WSAddr, GRPCAddr: n.config.GRPCAddr, ConnCount: connCount, MaxConn: n.config.MaxConn, UpdatedAt: time.Now().Unix()}
	return n.endpointRepo.UpsertEndpoint(ctx, ep)
}

// RegisterConnection 校验 route_token 并注册一条用户连接。
func (n *Node) RegisterConnection(ctx context.Context, connID string, routeToken string, deviceID string, platform string) (*Client, error) {
	if connID == "" || routeToken == "" {
		return nil, errors.New("conn_id or route_token is empty")
	}
	if len(connID) > maxIDLength || len(deviceID) > maxIDLength || len(platform) > 32 {
		return nil, errors.New("connection metadata is too long")
	}
	claims, err := n.tokenManager.Verify(routeToken)
	if err != nil {
		return nil, err
	}
	if claims.EndpointID != n.config.EndpointID {
		return nil, errors.New("route token endpoint mismatch")
	}
	if claims.UserID == "" || len(claims.UserID) > maxIDLength {
		return nil, errors.New("route token user is invalid")
	}

	connLock := n.connectionLock(connID)
	connLock.Lock()
	defer connLock.Unlock()

	client := NewClient(connID, claims.UserID, deviceID, platform, n.config.SendBuffer)
	var oldClient *Client

	n.mu.Lock()
	if n.stopping {
		n.mu.Unlock()
		return nil, errors.New("websocket node is draining")
	}
	oldClient = n.clients[connID]
	if oldClient == nil && int64(len(n.clients)) >= n.config.MaxConn {
		n.mu.Unlock()
		return nil, errors.New("websocket node is full")
	}
	if oldClient != nil {
		delete(n.userConns[oldClient.UserID], connID)
		if len(n.userConns[oldClient.UserID]) == 0 {
			delete(n.userConns, oldClient.UserID)
		}
	}
	n.clients[connID] = client
	if _, ok := n.userConns[claims.UserID]; !ok {
		n.userConns[claims.UserID] = make(map[string]struct{})
	}
	n.userConns[claims.UserID][connID] = struct{}{}
	n.mu.Unlock()

	if oldClient != nil {
		if err := n.removeClientFromRooms(context.Background(), oldClient); err != nil {
			log.Printf("replace connection live route cleanup failed conn=%s error=%v", connID, err)
		}
		oldClient.Close()
	}

	now := time.Now().Unix()
	info := &domain.ConnInfo{ConnID: connID, UserID: claims.UserID, EndpointID: n.config.EndpointID, DeviceID: deviceID, Platform: platform, GRPCAddr: n.config.GRPCAddr, CreatedAt: now, UpdatedAt: now}
	if err := n.routeResolver.RegisterConn(ctx, info); err != nil {
		n.removeLocalClient(client)
		_ = n.routeResolver.RemoveConn(context.Background(), connID)
		return nil, err
	}
	return client, nil
}

// RemoveClientConnection 仅在 client 仍是当前 conn_id 对应实例时移除连接。
func (n *Node) RemoveClientConnection(ctx context.Context, client *Client) error {
	if client == nil {
		return nil
	}
	return n.removeConnection(ctx, client.ConnID, client)
}

// RemoveConnection 按 conn_id 移除当前连接。
func (n *Node) RemoveConnection(ctx context.Context, connID string) error {
	return n.removeConnection(ctx, connID, nil)
}

func (n *Node) removeConnection(ctx context.Context, connID string, expected *Client) error {
	connLock := n.connectionLock(connID)
	connLock.Lock()
	defer connLock.Unlock()

	n.mu.Lock()
	client := n.clients[connID]
	if expected != nil && client != expected {
		n.mu.Unlock()
		return nil
	}
	if client != nil {
		delete(n.clients, connID)
		delete(n.userConns[client.UserID], connID)
		if len(n.userConns[client.UserID]) == 0 {
			delete(n.userConns, client.UserID)
		}
	}
	n.mu.Unlock()

	var roomErr error
	if client != nil {
		roomErr = n.removeClientFromRooms(ctx, client)
		client.Close()
	}
	if client == nil && expected != nil {
		return nil
	}
	if err := n.routeResolver.RemoveConn(ctx, connID); err != nil {
		return err
	}
	return roomErr
}

func (n *Node) removeLocalClient(expected *Client) {
	n.mu.Lock()
	if n.clients[expected.ConnID] == expected {
		delete(n.clients, expected.ConnID)
		delete(n.userConns[expected.UserID], expected.ConnID)
		if len(n.userConns[expected.UserID]) == 0 {
			delete(n.userConns, expected.UserID)
		}
	}
	n.mu.Unlock()
	if err := n.removeClientFromRooms(context.Background(), expected); err != nil {
		log.Printf("local connection live route cleanup failed conn=%s error=%v", expected.ConnID, err)
	}
	expected.Close()
}

// removeClientFromRooms 删除期望连接实例的本地房间关系，并在房间失去最后成员时撤销节点租约。
func (n *Node) removeClientFromRooms(ctx context.Context, expected *Client) error {
	if expected == nil {
		return nil
	}
	n.roomTransitionMu.Lock()
	defer n.roomTransitionMu.Unlock()

	emptyRooms := n.roomHub.RemoveClient(expected)
	if n.liveRoomRegistry == nil {
		return nil
	}
	var firstErr error
	for _, roomID := range emptyRooms {
		if err := n.liveRoomRegistry.UnregisterNode(ctx, roomID, n.config.EndpointID); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (n *Node) connectionLock(connID string) *sync.Mutex {
	var hash uint32 = 2166136261
	for index := 0; index < len(connID); index++ {
		hash ^= uint32(connID[index])
		hash *= 16777619
	}
	return &n.connLocks[hash%uint32(len(n.connLocks))]
}

// JoinRoom 将连接加入本节点直播间，并在首个本地成员加入时注册节点租约。
func (n *Node) JoinRoom(ctx context.Context, connID string, roomID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if roomID == "" || len(roomID) > maxIDLength {
		return errors.New("room_id is invalid")
	}

	n.roomTransitionMu.Lock()
	defer n.roomTransitionMu.Unlock()
	n.mu.RLock()
	client := n.clients[connID]
	stopping := n.stopping
	n.mu.RUnlock()
	if stopping {
		return errors.New("websocket node is draining")
	}
	if client == nil {
		return errors.New("conn not found")
	}

	joined, firstLocalMember := n.roomHub.Join(roomID, client)
	if !joined {
		return errors.New("room limit reached or connection closed")
	}
	if firstLocalMember && n.liveRoomRegistry != nil {
		if err := n.liveRoomRegistry.RegisterNode(ctx, roomID, n.config.EndpointID); err != nil {
			// 首次租约注册失败时回滚本地加入，避免客户端误以为后续可以收到跨节点消息。
			n.roomHub.Leave(roomID, connID)
			return err
		}
	}
	return nil
}

// LeaveRoom 将连接移出本节点直播间，并在最后本地成员离开时撤销节点租约。
func (n *Node) LeaveRoom(ctx context.Context, connID string, roomID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if roomID == "" || len(roomID) > maxIDLength {
		return errors.New("room_id is invalid")
	}

	n.roomTransitionMu.Lock()
	defer n.roomTransitionMu.Unlock()
	lastLocalMember := n.roomHub.Leave(roomID, connID)
	if lastLocalMember && n.liveRoomRegistry != nil {
		return n.liveRoomRegistry.UnregisterNode(ctx, roomID, n.config.EndpointID)
	}
	return nil
}

// BroadcastRoom 向本节点指定直播间广播消息。
func (n *Node) BroadcastRoom(ctx context.Context, roomID string, payload []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return n.roomHub.Broadcast(roomID, payload), nil
}

// PushToConns 将客户端消息写入本节点指定连接的发送队列。
func (n *Node) PushToConns(ctx context.Context, connIDs []string, msg *domain.Message) (*domain.PushResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, errors.New("message is nil")
	}
	payload, err := domain.MarshalClientMessage(msg)
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
	n.mu.RLock()
	clientExists := n.clients[connID] != nil
	n.mu.RUnlock()
	if !clientExists {
		return errors.New("conn not found")
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
