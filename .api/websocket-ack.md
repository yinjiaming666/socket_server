# WebSocket 消息回执

单聊和普通群聊推送通过 `m: "message"` 下发，`data.ack_required` 为 `true` 时，每条收到消息的连接需要确认 `data.message_id`。直播间弹幕和公告的 `ack_required` 为 `false`。单聊和群聊发送方可省略 `message_id` 由服务生成，主动指定时应为每条业务消息使用唯一值。

## 客户端回执

用 Gateway 签发的 `route_token` 连接 WebSocket 后，在收到消息的连接上发送 JSON。

```json
{"c":"socket_server","m":"ack","data":{"message_id":"msg-1"}}
```

`message_id` 是必填的非空字符串。连接 ID、用户和节点来自当前 WebSocket 会话。回执受理时服务端继续处理后续帧，格式、参数或处理错误通过以下信封返回。

```json
{"c":"socket_server","m":"error","data":{"message":"message_id is required"}}
```

## 节点 HTTP 回执

向承载该连接的 WebSocket Node 发起 `POST /api/ws-node/ack`。该路由供内网服务使用，访问控制由部署层负责。请求的 `Content-Type` 为 `application/json`，`message_id` 与 `conn_id` 都是必填的非空字符串。

```json
{"message_id":"msg-1","conn_id":"conn-1"}
```

成功受理返回 HTTP 200。

```json
{"code":0,"message":"ok","data":{"status":"ack"}}
```

请求无效或目标连接不在本节点时返回 HTTP 400。方法错误返回 HTTP 405。Redis 写入失败时返回 HTTP 503。错误响应示例为 `{"code":400,"message":"conn not found"}`。

## 状态和超时

- WebSocket Node 为需要回执的直接推送在入队前登记本地目标连接。状态按消息和连接独立维护。回执登记失败会中止该节点的推送。
- 在登记后 30 秒内收到匹配回执时，从 `pending` 转为 `done`。超时转为 `failed`。Message API 每 5 秒扫描超时记录。
- 同一个消息与连接的重复投递保留初次登记的期限及状态。重复回执、未登记的回执和迟到回执均可重复提交，已终结的状态保持原值。
- 最新一次状态写入后，消息的 Redis 回执记录保留 24 小时。发送接口返回的 `accepted` 表示入队结果，客户端收件以回执状态为准。
