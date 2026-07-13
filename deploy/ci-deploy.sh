#!/usr/bin/env bash
set -euo pipefail

# ci-deploy.sh 是 CI 平台无关的发布入口，统一执行测试、计划、部署和审计查询。
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
umask 077

SERVER_SELECTOR="${1:-${DEPLOY_SERVER_SELECTOR:-all}}"
SERVICE_SELECTOR="${2:-${DEPLOY_SERVICE_SELECTOR:-all}}"
ENDPOINT_SELECTOR="${3:-${DEPLOY_ENDPOINT_SELECTOR:-all}}"

# 每次流水线生成唯一版本；显式 RELEASE_VERSION 可用于 Git tag 或人工发布编号。
if [ -z "${GIT_COMMIT:-}" ]; then
  GIT_COMMIT="${CI_COMMIT_SHA:-${GITHUB_SHA:-}}"
fi
if [ -z "$GIT_COMMIT" ]; then
  GIT_COMMIT="$(git rev-parse HEAD)"
fi
if [ -z "${RELEASE_VERSION:-}" ]; then
  RELEASE_VERSION="$(date -u +%Y%m%dT%H%M%SZ)-${GIT_COMMIT:0:12}"
fi
export GIT_COMMIT RELEASE_VERSION

TARGETS_FILE="${TARGETS_FILE:-$ROOT_DIR/deploy/targets.json}"
APP_CONFIG="${APP_CONFIG:-$ROOT_DIR/configs/app.json}"
RUNTIME_ENV="${RUNTIME_ENV:-$ROOT_DIR/deploy/runtime.env}"
PACKAGE_FILE="${PACKAGE_FILE:-$ROOT_DIR/runtime/package/socket-server.tar.gz}"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/runtime/build}"
SSH_CONTROL_DIR="${SSH_CONTROL_DIR:-$ROOT_DIR/runtime/ci/ssh-control}"
export TARGETS_FILE APP_CONFIG RUNTIME_ENV PACKAGE_FILE BUILD_DIR SSH_CONTROL_DIR

for required_file in "$TARGETS_FILE" "$APP_CONFIG"; do
  if [ ! -f "$required_file" ]; then
    echo "required deployment file not found: $required_file" >&2
    exit 1
  fi
done

mkdir -p "$ROOT_DIR/runtime/ci"
DEPLOYCTL_BIN="$ROOT_DIR/runtime/ci/deployctl"

echo "release=$RELEASE_VERSION commit=$GIT_COMMIT"
echo "selection server=$SERVER_SELECTOR service=$SERVICE_SELECTOR endpoint=$ENDPOINT_SELECTOR"

# 测试和 deployctl 编译均在接触远端服务器前完成。
go test ./...
go build -o "$DEPLOYCTL_BIN" ./cmd/deployctl
"$DEPLOYCTL_BIN" plan "$SERVER_SELECTOR" "$SERVICE_SELECTOR" "$ENDPOINT_SELECTOR"

# 无论部署是否成功都尽力打印远端审计记录，便于 CI 日志定位失败节点。
print_history() {
  "$DEPLOYCTL_BIN" history "$SERVER_SELECTOR" "$SERVICE_SELECTOR" "$ENDPOINT_SELECTOR" || true
}
trap print_history EXIT
"$DEPLOYCTL_BIN" deploy "$SERVER_SELECTOR" "$SERVICE_SELECTOR" "$ENDPOINT_SELECTOR"
trap - EXIT
print_history
