# API：Message API HTTP 消息入口

## 摘要
- 用途：业务服务提交单聊、群聊和直播间消息，由 Message API 分发到 WebSocket Node 或 Redis 广播通道。
- 后端文件：`internal/message/handler.go`、`internal/message/service.go`
- 状态：active

## 调用方式
- 方法/协议：HTTP JSON
- 路径：`POST /api/messages/single`、`POST /api/messages/group`、`POST /api/messages/ack`、`POST /api/live/rooms/{room_id}/messages`
- 鉴权：当前实现未在 Message API 内校验调用方身份，调用方和网络边界需自行保证。
- Content-Type：`application/json`

## 请求
```ts
type SingleMessageRequest = {
  message_id?: string;
  from_user_id: string;
  to_user_id: string;
  content: unknown;
}

type GroupMessageRequest = {
  message_id?: string;
  from_user_id: string;
  group_id: string;
  member_user_ids: string[];
  content: unknown;
}

type LiveMessageRequest = {
  message_id?: string;
  from_user_id: string;
  type?: "notice" | string; // notice 走重要消息，其余按弹幕类消息处理
  content: unknown;
}

type ClientAckRequest = {
  message_id: string;
  user_id?: string;
  conn_id: string;
  ack_type?: string;
}
```

## 成功响应
```ts
type Response<T> = {
  code: 0;
  message: "ok";
  data: T;
}

type AcceptedResponse = {
  message_id: string;
  status: "accepted" | "partial" | "failed";
  accepted: boolean;
  accepted_conn_ids?: string[];
  failed_conn_ids?: string[];
  reason?: string;
}
```

## 错误响应
```ts
type ErrorResponse = {
  code: number;
  message: string;
}
```

## 备注
- 请求体最大 1 MiB，未知字段会被拒绝。
- 单聊和群聊默认需要客户端 ACK；直播间弹幕和公告默认不做客户端 ACK。
- ACK Redis 临时状态保留 24 小时。
