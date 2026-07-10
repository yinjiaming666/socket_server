# 快速远程部署

本项目推荐用同一份 Linux 构建包部署到多台服务器，再通过 `ENDPOINT_ID` 决定每个进程运行哪个实例。同一台服务器可以部署多个节点，只要每个节点的 `endpoint_id`、HTTP 端口和 gRPC 端口不同。

## 1. 准备运行配置

```bash
cp configs/app.example.json configs/app.json
cp deploy/targets.example.json deploy/targets.json
cp deploy/runtime.env.example deploy/runtime.env
```

按实际环境修改：

```text
configs/app.json       全量服务实例配置，所有服务器共用
deploy/targets.json    本地部署目标，一台服务器一个对象，instances 中声明多个实例
deploy/runtime.env     Redis 密码、route_token 密钥等运行期覆盖项
```

示例默认按“服务器之间没有内网互通”处理，`addr`、`ws_addr`、`grpc_addr`、`redis.addr` 使用跨服务器可访问的公网 IP 或域名。生产环境需要通过安全组/防火墙白名单限制 Redis 和 gRPC 端口，只允许对应服务服务器访问。

生产环境必须在 `configs/app.json` 中使用 Redis 共享状态：

```json
{
  "storage": "redis"
}
```

部署脚本会将同一份 `configs/app.json` 分发给各服务实例。

本地部署脚本需要 `python3` 解析 `deploy/targets.json`。远程服务器不需要额外安装 JSON 解析工具。

## 2. configs/app.json 字段说明

JSON 本身不支持注释，配置字段说明统一放在这里。

### storage

顶层字符串字段。生产及多进程联调使用 `redis`，仅单进程调试或单元测试使用 `memory`；未配置时默认为 `memory`。

### redis

| 字段 | 说明 |
| --- | --- |
| `addr` | Redis 地址。默认无内网互通时填写公网可访问地址或云 Redis 访问域名，例如 `203.0.113.10:6379`。 |
| `password` | Redis 密码。也可以在 `deploy/runtime.env` 中用 `REDIS_PASSWORD` 覆盖。 |
| `db` | Redis DB 编号。 |
| `pool_size` | 每个服务进程的 Redis 连接池大小。 |
| `key_prefix` | Redis key 前缀，用于隔离环境，例如 `socket:`。 |

### route_token

| 字段 | 说明 |
| --- | --- |
| `secret` | Gateway 生成 route_token、WebSocket Node 校验 route_token 的共享密钥，所有实例必须一致。 |
| `ttl_seconds` | route_token 有效秒数。 |

### gateways[]

| 字段 | 说明 |
| --- | --- |
| `endpoint_id` | Gateway 实例唯一编号。 |
| `addr` | Gateway 自身可访问地址。默认无内网互通时填写公网 IP 或域名加端口。 |
| `port` | Gateway HTTP 监听端口。 |
| `public_host` | 客户端最终连接 WebSocket 使用的公网域名，不带协议和路径，例如 `ws.example.com`。Gateway 会返回 `wss://{public_host}{ws_path}?route_token=...`。 |
| `heartbeat_timeout_seconds` | Gateway 判断 WebSocket Node 心跳过期的秒数。 |
| `last_endpoint_ttl_seconds` | 用户最近连接节点缓存时间，尽量让同一用户回到原节点。 |

### message_apis[]

| 字段 | 说明 |
| --- | --- |
| `endpoint_id` | Message API 实例唯一编号。 |
| `addr` | Message API 自身可访问地址。默认无内网互通时填写公网 IP 或域名加端口。 |
| `port` | Message API HTTP 监听端口。 |

### nodes[]

| 字段 | 说明 |
| --- | --- |
| `endpoint_id` | WebSocket Node 实例唯一编号。 |
| `ws_path` | Nginx/LB 转发 WebSocket 的路径，例如 `/ws/ws-node-1`。 |
| `ws_addr` | WebSocket Node HTTP 地址。默认无内网互通时填写公网 IP 或域名加端口；如果使用同 VPC 内网，可以填内网地址。 |
| `grpc_addr` | WebSocket Node gRPC 推送地址，Message API 会使用它推送单聊/群聊。默认无内网互通时填写公网可访问地址，但必须用防火墙白名单只允许 Message API 访问。 |
| `port` | WebSocket Node HTTP 监听端口，同一台服务器部署多个节点时必须不同。 |
| `grpc_port` | WebSocket Node gRPC 监听端口，同一台服务器部署多个节点时必须不同。 |
| `max_conn` | 单节点最大连接数，Gateway 会用连接数比例做节点选择。 |
| `send_buffer` | 单连接发送队列长度。 |
| `heartbeat_interval_seconds` | WebSocket Node 上报心跳的间隔秒数。 |

## 3. deploy/targets.json 字段说明

`deploy/targets.json` 只描述从本地 SSH 到哪些服务器、每台服务器部署哪些实例。它不参与服务运行时配置。

| 字段 | 说明 |
| --- | --- |
| `servers` | 服务器列表。 |
| `servers[].id` | 本地部署脚本使用的服务器编号，例如 `ws-server-1`。 |
| `servers[].host` | SSH 目标地址。默认无内网互通时填写服务器公网 IP 或域名。 |
| `servers[].ssh_user` | SSH 用户，通常为 `root`。非 root 用户需要具备免交互 `sudo`。 |
| `servers[].ssh_port` | SSH 端口。 |
| `servers[].deploy_dir` | 远程部署目录，例如 `/opt/socket-server`。 |
| `servers[].instances` | 该服务器上要部署的服务实例列表。 |
| `servers[].instances[].service` | 服务类型，只能是 `gateway`、`message-api`、`websocket-node`。 |
| `servers[].instances[].endpoint_id` | 对应 `configs/app.json` 里的实例 `endpoint_id`。 |

示例：

```json
{
  "servers": [
    {
      "id": "ws-server-1",
      "host": "203.0.113.31",
      "ssh_user": "root",
      "ssh_port": 22,
      "deploy_dir": "/opt/socket-server",
      "instances": [
        {
          "service": "websocket-node",
          "endpoint_id": "ws-node-1"
        },
        {
          "service": "websocket-node",
          "endpoint_id": "ws-node-2"
        }
      ]
    }
  ]
}
```

上面配置会在同一台 `203.0.113.31` 上部署两个 WebSocket Node：

```text
socket-server-websocket-node-ws-node-1.service
socket-server-websocket-node-ws-node-2.service
```

## 4. 本地部署

本地部署入口是 Go 命令，不再使用 `scripts/deploy.sh`。部署全部目标时，工具会按物理服务器分组：同一个 `host + ssh_user + ssh_port + deploy_dir` 只上传一次包，然后依次安装该服务器匹配到的全部实例。

部署全部服务器：

```bash
go run ./cmd/deployctl plan all
go run ./cmd/deployctl deploy all
```

快捷写法：

```bash
go run ./cmd/deployctl all
```

只部署一台服务器：

```bash
go run ./cmd/deployctl deploy ws-server-1
```

只部署某类服务或某个节点：

```bash
go run ./cmd/deployctl deploy all websocket-node all
go run ./cmd/deployctl deploy all websocket-node ws-node-1
```

默认会先构建 Linux amd64 包，再上传到远程服务器。若已经构建过，可以跳过构建：

```bash
SKIP_BUILD=1 go run ./cmd/deployctl deploy ws-server-1
```

指定 SSH 参数：

```bash
SSH_OPTS="-i ~/.ssh/id_rsa" go run ./cmd/deployctl deploy ws-server-1
```

Go 部署工具默认启用 SSH ControlMaster 连接复用，同一台服务器在一次运行中通常只需要输入一次 SSH 密码。默认 socket 路径在 `/tmp` 下使用短 hash，避免 macOS Unix domain socket path too long。可调整或关闭：

```bash
SSH_CONTROL_PERSIST=10m go run ./cmd/deployctl deploy ws-server-1
SSH_CONTROL_DIR=/tmp/ss-socket go run ./cmd/deployctl deploy ws-server-1
SSH_MULTIPLEX=0 go run ./cmd/deployctl deploy ws-server-1
```

## 5. 本地远程管理

查看某台服务器已部署服务：

```bash
go run ./cmd/deployctl help list
go run ./cmd/deployctl list ws-server-1
```

`list` 只会查看本地 `deploy/targets.json` 选中的实例。例如 `list ws-server-1` 只查看 `ws-server-1` 下配置的节点，不会再把同一台物理机上的其它逻辑服务一起列出。

`STATE` 说明：

```text
active           systemd unit 正在运行
inactive/failed  systemd 识别该 unit，但服务已停止或失败
unit-not-found   runtime/services 有部署记录，但 systemd unit 文件不存在
systemctl-unavailable systemctl 没有返回可识别状态
```

看到 `unit-not-found` 时，通常重新部署对应实例即可修复。

查看状态：

```bash
go run ./cmd/deployctl status ws-server-1
go run ./cmd/deployctl status ws-server-1 websocket-node ws-node-1
```

暂停、启动、重启：

```bash
go run ./cmd/deployctl pause ws-server-1 websocket-node ws-node-1
go run ./cmd/deployctl start ws-server-1 websocket-node ws-node-1
go run ./cmd/deployctl restart ws-server-1 websocket-node ws-node-1
```

查看日志：

```bash
go run ./cmd/deployctl logs ws-server-1 websocket-node ws-node-1
```

## 6. 服务器本地管理脚本

部署时会上传并安装：

```text
/opt/socket-server/socketctl
```

若服务器上还保留旧版本管理脚本，可只同步脚本，不会构建、部署、启动、暂停或重启服务：

```bash
go run ./cmd/deployctl syncctl all
go run ./cmd/deployctl syncctl ws-server-1
```

可直接登录服务器执行：

```bash
/opt/socket-server/socketctl list
/opt/socket-server/socketctl help
/opt/socket-server/socketctl status
/opt/socket-server/socketctl pause websocket-node ws-node-1
/opt/socket-server/socketctl start websocket-node ws-node-1
/opt/socket-server/socketctl logs websocket-node ws-node-1
```

## 7. 端口要求

```text
Gateway HTTP       对公网或业务网开放，例如 7000
Message API HTTP   对业务服务开放，例如 7100
WebSocket HTTP     对公网 Nginx/LB 开放，例如 8081、8082
WebSocket gRPC     默认无内网互通时可用公网地址，但必须只允许 Message API 服务器访问，例如 9001、9002
Redis              默认无内网互通时可用公网/云 Redis 地址，但必须只允许 Gateway、Message API、WebSocket Node 服务器访问
```
