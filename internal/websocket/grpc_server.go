package websocket

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	websocketpb "socket_server/api/proto/websocket"
	"socket_server/internal/grpcpush"
)

// grpcPushServer 适配 WebSocket Node 到 protobuf gRPC 服务接口。
type grpcPushServer struct {
	websocketpb.UnimplementedWebSocketPushServiceServer
	node *Node
}

// PushToConns 按连接推送消息。
func (s *grpcPushServer) PushToConns(ctx context.Context, req *websocketpb.PushToConnsRequest) (*websocketpb.PushResponse, error) {
	resp, err := s.node.PushToConns(ctx, req.GetConnIds(), grpcpush.MessageFromProto(req.GetMessage()))
	if err != nil {
		return nil, err
	}
	return grpcpush.PushResponseToProto(resp), nil
}

// PushToUsers 按用户推送消息。
func (s *grpcPushServer) PushToUsers(ctx context.Context, req *websocketpb.PushToUsersRequest) (*websocketpb.PushResponse, error) {
	resp, err := s.node.PushToUsers(ctx, req.GetUserIds(), grpcpush.MessageFromProto(req.GetMessage()))
	if err != nil {
		return nil, err
	}
	return grpcpush.PushResponseToProto(resp), nil
}

// Health 返回节点健康状态。
func (s *grpcPushServer) Health(ctx context.Context, req *websocketpb.HealthRequest) (*websocketpb.HealthResponse, error) {
	return &websocketpb.HealthResponse{EndpointId: req.GetEndpointId(), Status: "online", ConnCount: s.node.ConnCount()}, nil
}

// StartGRPCServer 启动 WebSocket Node 内部 gRPC 推送服务。
func (n *Node) StartGRPCServer(ctx context.Context) error {
	if n.config.GRPCAddr == "" {
		return nil
	}
	listener, err := net.Listen("tcp", n.config.GRPCAddr)
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	websocketpb.RegisterWebSocketPushServiceServer(server, &grpcPushServer{node: n})
	go func() {
		<-ctx.Done()
		server.GracefulStop()
		_ = listener.Close()
	}()
	go func() {
		log.Printf("websocket node %s grpc listening on %s", n.config.EndpointID, n.config.GRPCAddr)
		if err := server.Serve(listener); err != nil && ctx.Err() == nil {
			log.Printf("websocket node grpc stopped: %v", err)
		}
	}()
	return nil
}
