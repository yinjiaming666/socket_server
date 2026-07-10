// Code generated manually to match api/proto/websocket.proto. DO NOT EDIT.
package websocketpb

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const WebSocketPushService_ServiceDescName = "websocket.v1.WebSocketPushService"

// WebSocketPushServiceClient 是 WebSocket Node 内部推送服务客户端。
type WebSocketPushServiceClient interface {
	PushToConns(ctx context.Context, in *PushToConnsRequest, opts ...grpc.CallOption) (*PushResponse, error)
	PushToUsers(ctx context.Context, in *PushToUsersRequest, opts ...grpc.CallOption) (*PushResponse, error)
	Health(ctx context.Context, in *HealthRequest, opts ...grpc.CallOption) (*HealthResponse, error)
}

type webSocketPushServiceClient struct{ cc grpc.ClientConnInterface }

// NewWebSocketPushServiceClient 创建 WebSocketPushService 客户端。
func NewWebSocketPushServiceClient(cc grpc.ClientConnInterface) WebSocketPushServiceClient {
	return &webSocketPushServiceClient{cc: cc}
}

func (c *webSocketPushServiceClient) PushToConns(ctx context.Context, in *PushToConnsRequest, opts ...grpc.CallOption) (*PushResponse, error) {
	out := new(PushResponse)
	err := c.cc.Invoke(ctx, "/websocket.v1.WebSocketPushService/PushToConns", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *webSocketPushServiceClient) PushToUsers(ctx context.Context, in *PushToUsersRequest, opts ...grpc.CallOption) (*PushResponse, error) {
	out := new(PushResponse)
	err := c.cc.Invoke(ctx, "/websocket.v1.WebSocketPushService/PushToUsers", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *webSocketPushServiceClient) Health(ctx context.Context, in *HealthRequest, opts ...grpc.CallOption) (*HealthResponse, error) {
	out := new(HealthResponse)
	err := c.cc.Invoke(ctx, "/websocket.v1.WebSocketPushService/Health", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WebSocketPushServiceServer 是 WebSocket Node 内部推送服务服务端。
type WebSocketPushServiceServer interface {
	PushToConns(context.Context, *PushToConnsRequest) (*PushResponse, error)
	PushToUsers(context.Context, *PushToUsersRequest) (*PushResponse, error)
	Health(context.Context, *HealthRequest) (*HealthResponse, error)
	mustEmbedUnimplementedWebSocketPushServiceServer()
}

// UnimplementedWebSocketPushServiceServer 提供向前兼容的默认实现。
type UnimplementedWebSocketPushServiceServer struct{}

func (UnimplementedWebSocketPushServiceServer) PushToConns(context.Context, *PushToConnsRequest) (*PushResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PushToConns not implemented")
}
func (UnimplementedWebSocketPushServiceServer) PushToUsers(context.Context, *PushToUsersRequest) (*PushResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method PushToUsers not implemented")
}
func (UnimplementedWebSocketPushServiceServer) Health(context.Context, *HealthRequest) (*HealthResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Health not implemented")
}
func (UnimplementedWebSocketPushServiceServer) mustEmbedUnimplementedWebSocketPushServiceServer() {}

// RegisterWebSocketPushServiceServer 注册 WebSocketPushService 服务端。
func RegisterWebSocketPushServiceServer(s grpc.ServiceRegistrar, srv WebSocketPushServiceServer) {
	s.RegisterService(&WebSocketPushService_ServiceDesc, srv)
}

func _WebSocketPushService_PushToConns_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(PushToConnsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WebSocketPushServiceServer).PushToConns(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/websocket.v1.WebSocketPushService/PushToConns"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(WebSocketPushServiceServer).PushToConns(ctx, req.(*PushToConnsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WebSocketPushService_PushToUsers_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(PushToUsersRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WebSocketPushServiceServer).PushToUsers(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/websocket.v1.WebSocketPushService/PushToUsers"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(WebSocketPushServiceServer).PushToUsers(ctx, req.(*PushToUsersRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WebSocketPushService_Health_Handler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(HealthRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WebSocketPushServiceServer).Health(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/websocket.v1.WebSocketPushService/Health"}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(WebSocketPushServiceServer).Health(ctx, req.(*HealthRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// WebSocketPushService_ServiceDesc 是 WebSocketPushService 的 grpc.ServiceDesc。
var WebSocketPushService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: WebSocketPushService_ServiceDescName,
	HandlerType: (*WebSocketPushServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "PushToConns", Handler: _WebSocketPushService_PushToConns_Handler},
		{MethodName: "PushToUsers", Handler: _WebSocketPushService_PushToUsers_Handler},
		{MethodName: "Health", Handler: _WebSocketPushService_Health_Handler},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "api/proto/websocket.proto",
}
