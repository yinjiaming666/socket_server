#!/usr/bin/env bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_CONFIG="${APP_CONFIG:-$ROOT_DIR/configs/app.json}"
PACKAGE_FILE="${PACKAGE_FILE:-$ROOT_DIR/runtime/package/socket-server.tar.gz}"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/runtime/build}"

case "$APP_CONFIG" in
  /*) ;;
  *) APP_CONFIG="$ROOT_DIR/$APP_CONFIG" ;;
esac
case "$PACKAGE_FILE" in
  /*) ;;
  *) PACKAGE_FILE="$ROOT_DIR/$PACKAGE_FILE" ;;
esac
case "$BUILD_DIR" in
  /*) ;;
  *) BUILD_DIR="$ROOT_DIR/$BUILD_DIR" ;;
esac

if [ ! -f "$APP_CONFIG" ]; then
  echo "config file not found: $APP_CONFIG" >&2
  echo "copy configs/app.example.json to configs/app.json and edit it first." >&2
  exit 1
fi

PKG_DIR="$BUILD_DIR/socket-server"
rm -rf "$PKG_DIR"
mkdir -p "$PKG_DIR/bin" "$PKG_DIR/configs" "$PKG_DIR/deploy" "$(dirname "$PACKAGE_FILE")"

cd "$ROOT_DIR"
export GOOS="${GOOS:-linux}"
export GOARCH="${GOARCH:-amd64}"
export CGO_ENABLED="${CGO_ENABLED:-0}"

go build -o "$PKG_DIR/bin/gateway" ./cmd/gateway
go build -o "$PKG_DIR/bin/message-api" ./cmd/message-api
go build -o "$PKG_DIR/bin/websocket-node" ./cmd/websocket-node

cp "$APP_CONFIG" "$PKG_DIR/configs/app.json"
chmod 600 "$PKG_DIR/configs/app.json"
cp "$ROOT_DIR/deploy/install-instance.sh" "$PKG_DIR/deploy/install-instance.sh"
cp "$ROOT_DIR/deploy/socketctl.sh" "$PKG_DIR/deploy/socketctl.sh"
chmod +x "$PKG_DIR/deploy/install-instance.sh" "$PKG_DIR/deploy/socketctl.sh"

tar_extra_args=""
if tar --help 2>/dev/null | grep -q -- "--no-xattrs"; then
  tar_extra_args="$tar_extra_args --no-xattrs"
fi
if tar --help 2>/dev/null | grep -q -- "--no-mac-metadata"; then
  tar_extra_args="$tar_extra_args --no-mac-metadata"
fi

# shellcheck disable=SC2086
COPYFILE_DISABLE=1 tar $tar_extra_args -C "$PKG_DIR" -czf "$PACKAGE_FILE" .
echo "built package: $PACKAGE_FILE"
