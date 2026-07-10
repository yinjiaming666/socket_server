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

CURRENT_DIR="$DEPLOY_DIR/current"
BIN="$CURRENT_DIR/bin/$SERVICE"
CONFIG_FILE="$CURRENT_DIR/configs/app.json"
UNIT="socket-server-${SERVICE}-${ENDPOINT_ID}.service"
ENV_DIR="$DEPLOY_DIR/runtime/env"
ENV_FILE="$ENV_DIR/${SERVICE}-${ENDPOINT_ID}.env"
SHARED_ENV="$DEPLOY_DIR/runtime/socket-server.env"
RECORD_DIR="$DEPLOY_DIR/runtime/services"
RECORD_FILE="$RECORD_DIR/${SERVICE}-${ENDPOINT_ID}.json"

if [ ! -x "$BIN" ]; then
  echo "binary not executable: $BIN" >&2
  exit 1
fi
if [ ! -f "$CONFIG_FILE" ]; then
  echo "config file missing: $CONFIG_FILE" >&2
  exit 1
fi

mkdir -p "$ENV_DIR" "$RECORD_DIR"

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
EnvironmentFile=-$SHARED_ENV
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

cat > "$RECORD_FILE" <<EOF
{
  "service": "$SERVICE",
  "endpoint_id": "$ENDPOINT_ID",
  "unit": "$UNIT",
  "updated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "installed and restarted $UNIT"
