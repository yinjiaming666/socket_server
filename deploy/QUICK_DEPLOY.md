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

部署脚本会将同一份 `configs/app.json` 放入权限受限的发布制品，再分发给各服务实例。生产配置应保留在本地安全目录或 CI Secret 中，不要提交到 Git。若存在 `deploy/runtime.env`，它会以 `runtime/release-env/{release_id}.env` 权限 600 保存，并固定到对应 systemd 实例；代码回滚时环境变量也会一起回到旧版本。

本地需要 Go 1.25+、OpenSSH 和 Bash。远程服务器需要 Linux、systemd、Bash、`tar`、`sha256sum`、`curl` 和 `readlink`；健康检查仅访问远端本机端口，不需要把 `/healthz` 或 `/readyz` 暴露到公网。

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
RELEASE_VERSION=v2026.07.11 GIT_COMMIT="$(git rev-parse HEAD)" \
  go run ./cmd/deployctl deploy all
```

不指定 `RELEASE_VERSION` 时会使用 UTC 时间和提交号生成版本。每次发布会：

1. 运行本地构建，生成 `manifest.json`、归档和 SHA256。
2. 在每台物理主机上获取原子部署锁，再上传并校验制品。
3. 保存 `previous`，将 `current` 切到不可变 `releases/{release_id}`。
4. WebSocket Node 先通过 `SIGUSR1` 进入 draining，默认等待 30 秒。
5. 每次只重启一个实例，并检查 systemd 与 `/readyz`；Redis 模式还会验证 Redis 命令链路。
6. 当前主机失败时自动恢复；后续主机失败时，已完成主机按相反顺序回滚。
7. 写入 `runtime/deploy-history.jsonl`，并清理未被运行实例引用的旧 release。

从不支持 `SIGUSR1` 的旧版本首次升级时，实例还没有 `runtime/active-releases` 标记，工具会跳过排空信号并使用 systemd 的正常 `SIGTERM` 重启；完成一次新版安装后，后续发布自动启用 30 秒 draining。

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

systemd unit 会固定到安装当时解析出的不可变 release 路径，因此定向发布不会影响同机其它实例后续故障自启使用的版本。活动 release 会记录在 `runtime/active-releases`，历史清理不会删除仍被实例使用的目录。

默认会先构建 Linux amd64 包，再上传到远程服务器。若已经构建过，可以跳过构建：

```bash
SKIP_BUILD=1 go run ./cmd/deployctl deploy ws-server-1
```

跳过构建时会校验 SHA256 并读取包内 `manifest.json`，自动复用原 release ID。若同时指定 `RELEASE_VERSION` 或 `GIT_COMMIT`，其值必须与 manifest 一致，否则拒绝部署。

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

常用发布参数：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `RELEASE_VERSION` | UTC 时间加提交号 | 所有主机共享的不可变发布编号。 |
| `GIT_COMMIT` | 空 | 写入制品清单的源码提交号。 |
| `DEPLOY_HEALTH_TIMEOUT_SECONDS` | `30` | 单实例健康检查最大秒数。 |
| `DEPLOY_DRAIN_TIMEOUT_SECONDS` | `30` | WebSocket Node 排空等待秒数。 |
| `DEPLOY_RETAIN_RELEASES` | `5` | 每台主机至少保留的最近 release 数；运行中引用额外保留。 |

查看发布历史：

```bash
go run ./cmd/deployctl history all
go run ./cmd/deployctl history ws-server-1
```

手工回滚到 `previous`：

```bash
go run ./cmd/deployctl rollback ws-server-1
go run ./cmd/deployctl rollback all
```

回滚会切换物理主机级 `current/previous`，因此必须覆盖该物理主机在 `targets.json` 中声明的全部实例；部分选择会在连接远端前被拒绝。

若本地进程被强制终止，远端可能留下部署锁。确认没有其它发布任务运行后，先查看持有者再人工清理：

```bash
cat /opt/socket-server/runtime/deploy.lock/owner
rm -rf /opt/socket-server/runtime/deploy.lock
```

不要在发布任务仍运行时删除锁。

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

部署时会上传并安装两个不依赖 `current` 符号链接的稳定入口：

```text
/opt/socket-server/socketctl          服务器本地管理入口
/opt/socket-server/install-instance   deployctl 内部安装/回滚入口
```

`install-instance` 会把 systemd unit 固定到目标不可变 release；发布切换或回滚后仍从稳定路径执行，避免误用刚切换的 `current/deploy/install-instance.sh`。

若服务器上还保留旧版本稳定入口，可只同步 `socketctl` 和 `install-instance`，不会构建、部署、启动、暂停或重启服务：

```bash
go run ./cmd/deployctl syncctl all
go run ./cmd/deployctl syncctl ws-server-1
```

旧服务器首次使用新版 `rollback` 前应先执行一次 `syncctl`。回滚会在切换 `current/previous` 前预检稳定安装器，缺失时直接终止并保持版本链接不变。

可直接登录服务器执行：

```bash
/opt/socket-server/socketctl list
/opt/socket-server/socketctl help
/opt/socket-server/socketctl status
/opt/socket-server/socketctl pause websocket-node ws-node-1
/opt/socket-server/socketctl start websocket-node ws-node-1
/opt/socket-server/socketctl logs websocket-node ws-node-1
```

## 7. 自动化部署

平台无关的 CI 入口：

```bash
bash deploy/ci-deploy.sh all all all
bash deploy/ci-deploy.sh ws-server-1 all all
```

它会依次执行 `go test ./...`、编译 deployctl、输出部署计划、执行滚动发布，并在成功或失败后查询远端审计记录。发布失败仍会返回非零退出码。

仓库提供两份 GitHub Actions 工作流：

```text
.github/workflows/ci.yml       main/master 推送及 Pull Request 自动测试
.github/workflows/deploy.yml   v* Tag 或 workflow_dispatch 生产发布
```

在 GitHub 的 `production` Environment 中配置审批规则，并设置以下 Secrets：

| Secret | 内容 |
| --- | --- |
| `APP_CONFIG_B64` | 生产 `configs/app.json` 的单行 Base64。 |
| `DEPLOY_TARGETS_B64` | 生产 `deploy/targets.json` 的单行 Base64。 |
| `RUNTIME_ENV_B64` | 可选，生产 `runtime.env` 的单行 Base64。 |
| `SSH_PRIVATE_KEY` | 专用部署私钥原文。 |
| `SSH_KNOWN_HOSTS` | 已人工核对指纹的目标主机 known_hosts 内容。 |

macOS/Linux 可用以下方式生成单行 Base64：

```bash
base64 < configs/app.json | tr -d '\n'
base64 < deploy/targets.json | tr -d '\n'
base64 < deploy/runtime.env | tr -d '\n'
```

`SSH_KNOWN_HOSTS` 的主机指纹应通过云厂商控制台或其它可信渠道核对。工作流强制 `StrictHostKeyChecking=yes`，不会为了方便关闭主机身份校验。发布包包含运行配置，因此流水线不会把归档上传为 GitHub Artifact。

## 8. 端口要求

```text
Gateway HTTP       对公网或业务网开放，例如 7000
Message API HTTP   对业务服务开放，例如 7100
WebSocket HTTP     对公网 Nginx/LB 开放，例如 8081、8082
WebSocket gRPC     默认无内网互通时可用公网地址，但必须只允许 Message API 服务器访问，例如 9001、9002
Redis              默认无内网互通时可用公网/云 Redis 地址，但必须只允许 Gateway、Message API、WebSocket Node 服务器访问
```
