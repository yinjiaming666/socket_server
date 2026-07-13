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
WebSocket Node 首个本地成员加入
  ↓
Redis ZSET 注册 room_id → endpoint_id 短租约
  ↓
Message API 查询房间当前承载节点
  ↓
Redis Pub/Sub 或 Redis Stream 通知按 endpoint_id 精准扇出
  ↓
目标 WebSocket Node（仅订阅自己的节点频道）
  ↓
本节点 RoomHub
  ↓
房间内 WebSocket 连接
```

直播间节点租约默认 30 秒、每 10 秒续期；用户连接路由绑定同样为 30 秒，并独立每 10 秒批量续期。最后一个本地成员离开、连接断开或节点优雅停止时会立即清理；节点异常退出后，最迟约 30 秒自动排除残留绑定。

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

本仓库提供本地/CI 打包、SHA256 校验、远程部署锁、WebSocket 排空、逐实例健康检查、自动回滚、发布历史和 systemd 管理。同一台服务器可以部署多个节点，每个实例会固定到不可变 release，并生成独立服务名：

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
RELEASE_VERSION=v2026.07.11 GIT_COMMIT="$(git rev-parse HEAD)" \
  go run ./cmd/deployctl deploy ws-server-1
```

查看发布历史或回滚整台物理主机：

```bash
go run ./cmd/deployctl history ws-server-1
go run ./cmd/deployctl rollback ws-server-1
```

WebSocket Node 发布前通过 `SIGUSR1` 进入 draining，默认等待 30 秒。三类服务统一使用 `/readyz` 作为发布门禁并检查 Redis 等关键依赖，`/healthz` 只表示进程存活；后续主机发布失败时，已完成主机会逆序回滚。

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

如果服务器上仍是旧版稳定脚本，可先同步 `socketctl` 和 `install-instance`，不会构建、部署或重启任何服务；旧服务器首次使用新版回滚前也应执行一次：

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

自动化入口：

```text
deploy/ci-deploy.sh              CI 平台无关入口
.github/workflows/ci.yml         自动测试
.github/workflows/deploy.yml     Git Tag/手工生产发布
```

生产配置和 SSH 密钥由 CI Secret 注入，不会上传包含运行配置的发布包作为流水线 Artifact。详细说明见 `deploy/QUICK_DEPLOY.md`。

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

连接 WebSocket 后，所有 JSON 包统一使用 `c / m / data` 信封，其中 `c` 固定为 `socket_server`，`m` 对应原来的 `type`，具体参数放入 `data`：

```json
{"c":"socket_server","m":"ping","data":{}}
```

```json
{"c":"socket_server","m":"join_room","data":{"room_id":"room-1"}}
```

```json
{"c":"socket_server","m":"leave_room","data":{"room_id":"room-1"}}
```

```json
{"c":"socket_server","m":"ack","data":{"message_id":"msg-1"}}
```

服务端业务推送使用 `m: "message"`，原有消息字段均位于 `data`；协议错误则返回 `m: "error"`，心跳响应返回 `m: "pong"`。

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

## 模拟测试客户端

`cmd/simulator` 可以批量获取 Gateway 路由、建立 WebSocket 连接、加入直播间，并周期发送单聊、群聊和直播消息。收到需要 ACK 的消息后会自动回 ACK，结束时输出连接数、收发量和失败数。

单节点本地测试：

```bash
go run ./cmd/simulator \
  -gateway http://127.0.0.1:7000 \
  -message-api http://127.0.0.1:7100 \
  -ws-base ws://127.0.0.1:8081 \
  -users 100 \
  -connections-per-user 2 \
  -duration 1m
```

如果 Gateway 返回的 `ws_url` 已经可以直接访问，不需要设置 `-ws-base`。常用参数：

```text
-users                 模拟用户数
-connections-per-user  每个用户的连接数
-connect-concurrency   建连并发数
-group-size            群消息成员数量
-single-interval       单聊发送间隔，0 表示关闭
-group-interval        群聊发送间隔，0 表示关闭
-live-interval         直播消息发送间隔，0 表示关闭
-verbose               输出单条失败日志
```

自动端到端测试会覆盖 Gateway 路由、WebSocket 握手、消息推送和客户端 ACK：

```bash
go test ./internal/websocket -run TestEndToEndRoutePushAndAck
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
