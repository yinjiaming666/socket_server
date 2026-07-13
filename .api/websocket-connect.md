# API：WebSocket Node 连接

## 摘要
- 用途：客户端连接指定 WebSocket Node，并注册连接路由。
- 后端文件：`internal/websocket/handler.go`、`internal/websocket/node.go`
- 状态：active

## 调用方式
- 方法/协议：WebSocket over HTTP GET Upgrade
- 路径：`/ws/{endpoint_id}?conn_id={conn_id}&route_token={route_token}&device_id={device_id}&platform={platform}`
- 鉴权：当前实现校验 `route_token` 的签名、过期时间和 `endpoint_id`；用户 token 真实性暂未在本服务内校验。
- Content-Type：无

## 请求
```ts
type Query = {
  conn_id: string;      // 必填，连接唯一编号，当前由调用方生成
  route_token: string;  // 必填，由 Gateway /api/ws/route 返回
  device_id?: string;   // 可选，设备编号
  platform?: string;    // 可选，例如 ios、android、web
}
```

## 成功响应
WebSocket 握手成功后返回 `101 Switching Protocols`。普通 HTTP GET 不再注册调试连接。

## WebSocket JSON 协议

控制消息和服务端业务消息均使用统一信封：

```ts
type WebSocketEnvelope<T> = {
  c: "socket_server";
  m: string; // 原 type 字段对应的方法名
  data: T;
};
```

客户端示例：

```json
{"c":"socket_server","m":"join_room","data":{"room_id":"room-1"}}
```

```json
{"c":"socket_server","m":"ack","data":{"message_id":"msg-1"}}
```

服务端业务推送使用 `m: "message"`；协议错误返回 `m: "error"`，错误详情位于 `data.message`。

## 错误响应
```ts
type ErrorResponse = {
  code: number;
  message: string;
}
```

## 备注
- 非 WebSocket Upgrade 请求返回 `400 websocket upgrade required`。
- 连接关闭时服务端会移除本地连接、房间映射和 Redis 路由。
