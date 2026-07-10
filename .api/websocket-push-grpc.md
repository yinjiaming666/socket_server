# API：WebSocket Node 内部推送 gRPC

## 摘要
- 用途：Message API 通过 gRPC 将消息投递到目标 WebSocket Node。
- 后端文件：`api/proto/websocket.proto`、`internal/grpcpush/protocol.go`、`internal/websocket/grpc_server.go`
- 状态：active

## 调用方式
- 方法/协议：gRPC
- Procedure：`websocket.v1.WebSocketPushService/PushToConns`、`PushToUsers`、`Health`
- 鉴权：当前实现使用内网/防火墙边界，gRPC 本身未启用 mTLS 或 token。
- Content-Type：`application/grpc`

## 请求
```proto
message PushMessage {
  string message_id = 1;
  string from_user_id = 2;
  string biz_type = 3;
  bytes payload = 4;
  bool ack_required = 5;
  int64 created_at = 6;
  repeated string to_user_ids = 7;
  repeated string conn_ids = 8;
  string group_id = 9;
  string room_id = 10;
}

message PushToConnsRequest {
  repeated string conn_ids = 1;
  PushMessage message = 2;
}

message PushToUsersRequest {
  repeated string user_ids = 1;
  PushMessage message = 2;
}

message HealthRequest {
  string endpoint_id = 1;
}
```

## 成功响应
```proto
message PushResponse {
  bool accepted = 1;
  repeated string accepted_conn_ids = 2;
  repeated string failed_conn_ids = 3;
  string reason = 4;
}

message HealthResponse {
  string endpoint_id = 1;
  string status = 2;
  int64 conn_count = 3;
}
```

## 错误响应
gRPC 调用错误直接返回为 RPC error；业务失败通过 `PushResponse.accepted=false`、`failed_conn_ids` 和 `reason` 表达。

## 备注
- `PushMessage` 必须保留 `group_id`、`room_id`、`to_user_ids`、`conn_ids`，避免 Message API 到 Node 的转换丢失客户端会话上下文。
- Node 写入本地连接发送队列成功只代表 Node 已接受，不代表客户端已经 ACK。
