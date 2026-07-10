#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  socketctl <command> [service|all] [endpoint_id|all]

Commands:
  list                 Show deployed service records.
  help                 Show this help.
  start                Start matching services.
  stop                 Stop matching services.
  pause                Alias of stop.
  restart              Restart matching services.
  status               Show systemd status for matching services.
  logs                 Show recent journal logs for matching services.

Examples:
  socketctl list
  socketctl help
  socketctl status
  socketctl pause websocket-node ws-node-1
  socketctl start websocket-node ws-node-1
  socketctl logs message-api message-api-1

State notes:
  active               systemd unit is running.
  inactive/failed      systemd knows the unit, but it is stopped or failed.
  unit-not-found       runtime/services has a record, but the systemd unit file is missing.
  systemctl-unavailable systemctl did not return a usable state.

Environment:
  DEPLOY_DIR=/opt/socket-server
  LINES=100
USAGE
}

detect_deploy_dir() {
  if [ -n "${DEPLOY_DIR:-}" ]; then
    printf "%s\n" "$DEPLOY_DIR"
    return
  fi

  script_path="${BASH_SOURCE[0]}"
  case "$script_path" in
    /*) ;;
    *) script_path="$(pwd)/$script_path" ;;
  esac
  script_dir="$(cd "$(dirname "$script_path")" && pwd)"
  if [ -d "$script_dir/runtime" ] && [ -d "$script_dir/current" ]; then
    printf "%s\n" "$script_dir"
    return
  fi

  printf "%s\n" "/opt/socket-server"
}

DEPLOY_DIR="$(detect_deploy_dir)"
RECORD_DIR="$DEPLOY_DIR/runtime/services"
COMMAND="${1:-list}"
SERVICE="${2:-all}"
ENDPOINT_ID="${3:-all}"
LINES="${LINES:-100}"
SYSTEMD_NOT_FOUND_ACTIVE_STATE="$(printf 'un%s' 'known')"

if [ "$(id -u)" -eq 0 ]; then
  SUDO_CMD=""
else
  SUDO_CMD="${SUDO_CMD:-sudo}"
fi

require_records() {
  if [ ! -d "$RECORD_DIR" ] || ! find "$RECORD_DIR" -type f -name '*.json' -print -quit | grep -q .; then
    echo "no deployed service records found: $RECORD_DIR" >&2
    exit 1
  fi
}

json_value() {
  key="$1"
  file="$2"
  sed -n 's/^[[:space:]]*"'"$key"'"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$file" | head -n 1
}

record_files() {
  require_records
  find "$RECORD_DIR" -type f -name '*.json' | sort
}

select_units() {
  record_files | while IFS= read -r file; do
    record_service="$(json_value service "$file")"
    record_endpoint="$(json_value endpoint_id "$file")"
    record_unit="$(json_value unit "$file")"
    if [ "$SERVICE" != "all" ] && [ "$record_service" != "$SERVICE" ]; then
      continue
    fi
    if [ "$ENDPOINT_ID" != "all" ] && [ "$record_endpoint" != "$ENDPOINT_ID" ]; then
      continue
    fi
    printf "%s\n" "$record_unit"
  done
}

service_state() {
  unit="$1"
  load_state="$($SUDO_CMD systemctl show "$unit" --property=LoadState --value 2>/dev/null || true)"
  active_state="$($SUDO_CMD systemctl show "$unit" --property=ActiveState --value 2>/dev/null || true)"
  case "$load_state" in
    not-found)
      printf "%s\n" "unit-not-found"
      ;;
    "")
      if [ "$active_state" = "$SYSTEMD_NOT_FOUND_ACTIVE_STATE" ]; then
        printf "%s\n" "unit-not-found"
      elif [ -n "$active_state" ]; then
        printf "%s\n" "$active_state"
      else
        printf "%s\n" "systemctl-unavailable"
      fi
      ;;
    *)
      if [ "$active_state" = "$SYSTEMD_NOT_FOUND_ACTIVE_STATE" ]; then
        printf "%s\n" "unit-not-found"
      elif [ -n "$active_state" ]; then
        printf "%s\n" "$active_state"
      else
        printf "%s\n" "$load_state"
      fi
      ;;
  esac
}

list_records() {
  matched_count=0
  missing_unit_count=0
  unavailable_count=0
  printf "%-16s %-24s %-48s %-24s %-24s\n" "SERVICE" "ENDPOINT" "UNIT" "UPDATED_AT" "STATE"
  while IFS= read -r file; do
    service="$(json_value service "$file")"
    endpoint="$(json_value endpoint_id "$file")"
    unit="$(json_value unit "$file")"
    updated_at="$(json_value updated_at "$file")"
    if [ "$SERVICE" != "all" ] && [ "$service" != "$SERVICE" ]; then
      continue
    fi
    if [ "$ENDPOINT_ID" != "all" ] && [ "$endpoint" != "$ENDPOINT_ID" ]; then
      continue
    fi
    matched_count=$((matched_count + 1))
    state="$(service_state "$unit")"
    case "$state" in
      unit-not-found) missing_unit_count=$((missing_unit_count + 1)) ;;
      systemctl-unavailable) unavailable_count=$((unavailable_count + 1)) ;;
    esac
    printf "%-16s %-24s %-48s %-24s %-24s\n" "$service" "$endpoint" "$unit" "$updated_at" "$state"
  done < <(record_files)

  if [ "$matched_count" -eq 0 ]; then
    echo "[hint] no matching service records: service=$SERVICE endpoint=$ENDPOINT_ID" >&2
    return 0
  fi
  if [ "$missing_unit_count" -gt 0 ]; then
    echo "[hint] unit-not-found 表示 runtime/services 有记录，但 /etc/systemd/system 下的 unit 不存在；重新部署对应实例可修复。" >&2
  fi
  if [ "$unavailable_count" -gt 0 ]; then
    echo "[hint] systemctl-unavailable 表示 systemctl 没有返回可识别状态；请检查 systemd 权限或服务器 systemd 状态。" >&2
  fi
}

run_systemctl() {
  action="$1"
  units="$(select_units)"
  if [ -z "$units" ]; then
    echo "no matching services: service=$SERVICE endpoint=$ENDPOINT_ID" >&2
    exit 1
  fi

  printf "%s\n" "$units" | while IFS= read -r unit; do
    [ -n "$unit" ] || continue
    echo "==> $action $unit"
    case "$action" in
      status)
        $SUDO_CMD systemctl --no-pager --full status "$unit" || true
        ;;
      logs)
        $SUDO_CMD journalctl -u "$unit" -n "$LINES" --no-pager || true
        ;;
      *)
        $SUDO_CMD systemctl "$action" "$unit"
        ;;
    esac
  done
}

case "$COMMAND" in
  list)
    list_records
    ;;
  start|stop|restart|status|logs)
    run_systemctl "$COMMAND"
    ;;
  pause)
    run_systemctl stop
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "unsupported command: $COMMAND" >&2
    usage
    exit 2
    ;;
esac
