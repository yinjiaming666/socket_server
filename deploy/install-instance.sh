#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  install-instance.sh <deploy_dir> <service> <endpoint_id>

service:
  gateway | message-api | websocket-node
USAGE
}

if [ "$#" -ne 3 ]; then
  usage
  exit 2
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "install-instance.sh must run as root because it writes systemd units." >&2
  exit 1
fi

DEPLOY_DIR="$1"
SERVICE="$2"
ENDPOINT_ID="$3"

case "$SERVICE" in
  gateway|message-api|websocket-node) ;;
  *)
    echo "unsupported service: $SERVICE" >&2
    usage
    exit 2
    ;;
esac

# 将 current 解析为不可变 release 实体，避免其它实例发布后影响本实例故障自启版本。
DEPLOY_ROOT="$(readlink -f "$DEPLOY_DIR")"
CURRENT_DIR="$(readlink -f "$DEPLOY_DIR/current")"
case "$CURRENT_DIR" in
  "$DEPLOY_ROOT"/releases/*) ;;
  *)
    echo "current release is outside deploy releases: $CURRENT_DIR" >&2
    exit 1
    ;;
esac

BIN="$CURRENT_DIR/bin/$SERVICE"
CONFIG_FILE="$CURRENT_DIR/configs/app.json"
UNIT="socket-server-${SERVICE}-${ENDPOINT_ID}.service"
ENV_DIR="$DEPLOY_DIR/runtime/env"
ENV_FILE="$ENV_DIR/${SERVICE}-${ENDPOINT_ID}.env"
# 新版本优先使用 release 级环境文件；没有时兼容旧版共享环境文件。
LEGACY_SHARED_ENV="$DEPLOY_DIR/runtime/socket-server.env"
RELEASE_ID="$(basename "$CURRENT_DIR")"
RELEASE_ENV="$DEPLOY_DIR/runtime/release-env/$RELEASE_ID.env"
RUNTIME_ENV_FILE="$RELEASE_ENV"
if [ ! -f "$RUNTIME_ENV_FILE" ]; then
  RUNTIME_ENV_FILE="$LEGACY_SHARED_ENV"
fi
RECORD_DIR="$DEPLOY_DIR/runtime/services"
RECORD_FILE="$RECORD_DIR/${SERVICE}-${ENDPOINT_ID}.json"
ACTIVE_RELEASE_DIR="$DEPLOY_DIR/runtime/active-releases"
ACTIVE_RELEASE_LINK="$ACTIVE_RELEASE_DIR/${SERVICE}-${ENDPOINT_ID}"

if [ ! -x "$BIN" ]; then
  echo "binary not executable: $BIN" >&2
  exit 1
fi
if [ ! -f "$CONFIG_FILE" ]; then
  echo "config file missing: $CONFIG_FILE" >&2
  exit 1
fi

mkdir -p "$ENV_DIR" "$RECORD_DIR" "$ACTIVE_RELEASE_DIR"

cat > "$ENV_FILE" <<EOF
CONFIG_FILE=$CONFIG_FILE
ENDPOINT_ID=$ENDPOINT_ID
EOF
chmod 600 "$ENV_FILE"

cat > "/etc/systemd/system/$UNIT" <<EOF
[Unit]
Description=Socket Server $SERVICE $ENDPOINT_ID
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$CURRENT_DIR
EnvironmentFile=-$RUNTIME_ENV_FILE
EnvironmentFile=$ENV_FILE
ExecStart=$BIN
Restart=always
RestartSec=3
KillSignal=SIGTERM
TimeoutStopSec=30
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$UNIT"
systemctl restart "$UNIT"

# 仅在重启成功后更新引用，历史清理会保留所有仍被实例使用的 release。
ln -sfn "$CURRENT_DIR" "$ACTIVE_RELEASE_LINK"

cat > "$RECORD_FILE" <<EOF
{
  "service": "$SERVICE",
  "endpoint_id": "$ENDPOINT_ID",
  "unit": "$UNIT",
  "release_dir": "$CURRENT_DIR",
  "runtime_env": "$RUNTIME_ENV_FILE",
  "updated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "installed and restarted $UNIT"
