# 分布式 WebSocket 消息分发服务

本项目是基于 **Golang + Redis + Go Gateway + gRPC** 的分布式 WebSocket 消息分发服务，用于支持多 WebSocket 节点横向扩展、客户端 endpoint 分配、多端登录、ACK、普通消息推送和直播间广播。

> 当前项目保留 Gateway、Message API、WebSocket Node、Redis/gRPC/WebSocket 核心运行链路，并提供基础远程部署脚本。消息下库、离线消息补发、Kafka 超大规模广播按当前需求暂不实现。

---

## 核心能力

```text
1. Gateway 根据节点心跳和连接数分配 WebSocket endpoint
2. WebSocket Node 管理真实 WebSocket 长连接、本地房间连接、客户端 ACK
3. WebSocket Node 启动内部 gRPC 推送服务
4. Message API 通过 gRPC 调用目标 WebSocket Node
5. Message API 提供单聊、群聊、ACK、直播间消息入口
6. 支持同一用户多端登录
7. 支持 ACK pending / done / failed，并有超时扫描任务
8. Redis 使用 github.com/redis/go-redis/v9 连接池实现
9. 支持 Redis EndpointRepository / RouteResolver / AckStore
10. 支持 Redis Pub/Sub 和 Redis Stream 写入与消费
11. 支持直播间消息跨节点广播到本机房间连接
12. 消息下库逻辑预留 MessageStore 接口，当前为空实现
```

---

## 架构概览

```text
Client
  │
  ├── GET /api/ws/route
  │       ↓
  │   Gateway
  │       ↓
  │   查询 Redis 中的 WebSocket Node 状态
  │       ↓
  │   返回 ws_url + route_token
  │
  ├── WebSocket Connect
  │       ↓
  │   WebSocket Node
  │       ↓
  │   注册连接路由到 Redis
  │
  └── POST /api/messages/*
          ↓
      Message API
          ↓
      查询 Redis 连接路由
          ↓
      gRPC 调用目标 WebSocket Node
```

直播间广播链路：

```text
Message API
  ↓
Redis Pub/Sub 或 Redis Stream
  ↓
WebSocket Node Redis 消费端
  ↓
本节点 RoomHub
  ↓
房间内 WebSocket 连接
```

---

## 本地运行

单服务接口调试和单元测试可以使用内存实现；Gateway、Message API、WebSocket Node 分进程运行时内存状态互不共享，完整链路联调请使用 Redis 共享状态模式。

```bash
CONFIG_FILE=configs/app.json ENDPOINT_ID=gateway-1 go run ./cmd/gateway
CONFIG_FILE=configs/app.json ENDPOINT_ID=message-api-1 go run ./cmd/message-api
CONFIG_FILE=configs/app.json ENDPOINT_ID=ws-node-1 go run ./cmd/websocket-node
```

共享状态模式由 `configs/app.json` 顶层的 `storage` 字段决定。完整链路联调请配置：

```json
{
  "storage": "redis"
}
```

本地单进程调试或单元测试可改为 `"storage": "memory"`。

---

## 快速远程部署

本仓库提供本地打包、上传远程服务器、安装 systemd 服务和远程管理脚本。同一台服务器可以部署多个节点，每个实例会生成独立服务名：

```text
socket-server-{service}-{endpoint_id}.service
```

初始化部署配置：

```bash
cp configs/app.example.json configs/app.json
cp deploy/targets.example.json deploy/targets.json
cp deploy/runtime.env.example deploy/runtime.env
```

部署全部目标：

```bash
go run ./cmd/deployctl plan all
go run ./cmd/deployctl deploy all
```

也可以用快捷写法：

```bash
go run ./cmd/deployctl all
```

部署单台服务器：

```bash
go run ./cmd/deployctl deploy ws-server-1
```

本地远程管理：

```bash
go run ./cmd/deployctl help list
go run ./cmd/deployctl list ws-server-1
go run ./cmd/deployctl status ws-server-1 websocket-node ws-node-1
go run ./cmd/deployctl pause ws-server-1 websocket-node ws-node-1
go run ./cmd/deployctl start ws-server-1 websocket-node ws-node-1
go run ./cmd/deployctl logs ws-server-1 websocket-node ws-node-1
```

`list` 的 `STATE` 中，`unit-not-found` 表示服务器上有部署记录但 systemd unit 文件不存在，通常重新部署对应实例即可修复；`systemctl-unavailable` 表示 systemctl 没有返回可识别状态，需要检查服务器 systemd 状态或权限。

如果服务器上已有旧版 `/opt/socket-server/socketctl`，先只同步管理脚本即可，不会构建、部署或重启任何服务：

```bash
go run ./cmd/deployctl syncctl all
```

部署完成后，服务器本地也会有管理脚本：

```bash
/opt/socket-server/socketctl list
/opt/socket-server/socketctl help
/opt/socket-server/socketctl status
/opt/socket-server/socketctl pause websocket-node ws-node-1
```

详细说明见：

```text
deploy/QUICK_DEPLOY.md
```

---

## gRPC 说明

当前已经接入官方 `google.golang.org/grpc`，并使用 protobuf 消息结构：

```text
api/proto/websocket/websocket.pb.go       protobuf 消息结构
api/proto/websocket/websocket_grpc.pb.go  gRPC client/server 接口
internal/grpcpush/protocol.go             domain 与 protobuf 转换
internal/push/grpc_client.go              Message API gRPC client
internal/websocket/grpc_server.go         WebSocket Node gRPC server
```

说明：当前 pb 文件已和 `api/proto/websocket.proto` 对齐。后续如果调整 proto，需要重新生成或同步更新 pb 文件。

---

## WebSocket 客户端消息

客户端通过 Gateway 获取连接地址：

```http
GET /api/ws/route
```

连接 WebSocket 后可发送控制消息：

```json
{"type":"ping"}
```

```json
{"type":"join_room","room_id":"room-1"}
```

```json
{"type":"leave_room","room_id":"room-1"}
```

```json
{"type":"ack","message_id":"msg-1"}
```

WebSocket 已包含基础生产增强：

```text
读超时
写超时
最大帧大小限制
服务端主动 ping
断开自动清理连接路由
慢客户端发送队列保护
```

---

## 当前不实现的模块

按当前需求，以下模块暂时不做：

```text
1. 消息下库
2. 离线消息补发
3. Kafka 超大规模广播
4. 部署 UI
```

---

## 测试

```bash
go mod tidy
go test ./...
```

---

## 文档入口

```text
configs/README.md
dev.md
```
