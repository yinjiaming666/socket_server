package deployctl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	appconfig "socket_server/internal/config"
)

// envIntValue 读取正整数部署参数，非法值交由默认值逻辑处理。
func envIntValue(name string) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		return 0
	}
	return value
}

// validSSHPort 验证 SSH 端口范围，避免把无效配置传给 ssh/scp。
func validSSHPort(value string) bool {
	port, err := strconv.Atoi(value)
	return err == nil && port >= 1 && port <= 65535
}

// hydrateTargetHTTPPorts 从同一份应用配置解析实例端口，避免 targets.json 重复维护。
func hydrateTargetHTTPPorts(targets []Target, configPath string) error {
	cfg, err := appconfig.Load(configPath)
	if err != nil {
		return fmt.Errorf("load app config for deployment health checks: %w", err)
	}
	for index := range targets {
		target := &targets[index]
		switch target.Service {
		case "gateway":
			item, ok := cfg.FindGateway(target.EndpointID)
			if !ok {
				return fmt.Errorf("gateway %s not found in app config", target.EndpointID)
			}
			target.HTTPPort = item.Port
		case "message-api":
			item, ok := cfg.FindMessageAPI(target.EndpointID)
			if !ok {
				return fmt.Errorf("message api %s not found in app config", target.EndpointID)
			}
			target.HTTPPort = item.Port
		case "websocket-node":
			item, ok := cfg.FindNode(target.EndpointID)
			if !ok {
				return fmt.Errorf("websocket node %s not found in app config", target.EndpointID)
			}
			target.HTTPPort = item.Port
		}
		if target.HTTPPort <= 0 {
			return fmt.Errorf("%s %s health port is invalid", target.Service, target.EndpointID)
		}
	}
	return nil
}

// validateCompleteRollbackSelection 要求一次回滚覆盖所选物理主机上的全部实例。
func validateCompleteRollbackSelection(file *TargetsFile, selectedTargets []Target) error {
	allTargets, err := SelectTargets(file, Filter{Server: "all", Service: "all", Endpoint: "all"})
	if err != nil {
		return err
	}

	fullByRemote := make(map[RemoteKey]map[string]struct{})
	selectedByRemote := make(map[RemoteKey]map[string]struct{})
	collect := func(targets []Target, destination map[RemoteKey]map[string]struct{}) {
		for _, target := range targets {
			key := RemoteKey{Host: target.Host, SSHUser: target.SSHUser, SSHPort: target.SSHPort, DeployDir: target.DeployDir}
			if destination[key] == nil {
				destination[key] = make(map[string]struct{})
			}
			destination[key][target.Service+"\x00"+target.EndpointID] = struct{}{}
		}
	}
	collect(allTargets, fullByRemote)
	collect(selectedTargets, selectedByRemote)

	for key, selected := range selectedByRemote {
		full := fullByRemote[key]
		if len(selected) != len(full) {
			return fmt.Errorf("rollback must include all instances on physical host %s; selected=%d total=%d", key.Host, len(selected), len(full))
		}
		for instance := range full {
			if _, ok := selected[instance]; !ok {
				return fmt.Errorf("rollback selection is incomplete on physical host %s", key.Host)
			}
		}
	}
	return nil
}

// targetUnit 返回实例对应的 systemd unit 名称。
func targetUnit(target Target) string {
	return fmt.Sprintf("socket-server-%s-%s.service", target.Service, target.EndpointID)
}

// targetHealthPath 统一使用 readiness，确保进程和 Redis 等关键依赖均可用。
func targetHealthPath(Target) string {
	return "/readyz"
}

// drainTarget 在重启 WebSocket Node 前通过 systemd 信号停止新连接分配。
func (d *Deployer) drainTarget(ctx context.Context, group RemoteGroup, target Target) error {
	if target.Service != "websocket-node" {
		return nil
	}
	activeReleaseLink := filepath.ToSlash(filepath.Join(
		group.Key.DeployDir,
		"runtime",
		"active-releases",
		target.Service+"-"+target.EndpointID,
	))
	script := fmt.Sprintf(
		"set -eu; if systemctl is-active --quiet %s; then if [ -L %s ]; then systemctl kill --kill-who=main --signal=SIGUSR1 %s; sleep %d; else echo 'legacy instance without drain support; skip SIGUSR1'; fi; fi",
		shellQuote(targetUnit(target)),
		shellQuote(activeReleaseLink),
		shellQuote(targetUnit(target)),
		d.cfg.DrainTimeoutSeconds,
	)
	if err := d.ssh(ctx, group.Key, runRemoteAsRoot(group.Key.SSHUser, script)); err != nil {
		return fmt.Errorf("drain %s: %w", target.EndpointID, err)
	}
	return nil
}

// checkTargetHealth 同时校验 systemd active 和本机 HTTP 健康端点。
func (d *Deployer) checkTargetHealth(ctx context.Context, group RemoteGroup, target Target) error {
	url := fmt.Sprintf("http://127.0.0.1:%d%s", target.HTTPPort, targetHealthPath(target))
	script := fmt.Sprintf(
		"set -eu; deadline=$((SECONDS+%d)); while [ \"$SECONDS\" -lt \"$deadline\" ]; do if systemctl is-active --quiet %s && curl --fail --silent --show-error --max-time 2 %s >/dev/null; then exit 0; fi; sleep 1; done; systemctl --no-pager --full status %s || true; exit 1",
		d.cfg.HealthTimeoutSeconds,
		shellQuote(targetUnit(target)),
		shellQuote(url),
		shellQuote(targetUnit(target)),
	)
	if err := d.ssh(ctx, group.Key, runRemoteAsRoot(group.Key.SSHUser, script)); err != nil {
		return fmt.Errorf("health check %s/%s: %w", target.Service, target.EndpointID, err)
	}
	return nil
}

// rollbackTargetsAfterFailure 在受限时间内切回 previous，并恢复本批次已经处理过的实例。
func (d *Deployer) rollbackTargetsAfterFailure(ctx context.Context, group RemoteGroup, targets []Target) error {
	rollbackCtx, cancel := context.WithTimeout(ctx, deploymentRollbackTimeout(d.cfg, []RemoteGroup{{Targets: targets}}))
	defer cancel()

	if err := d.swapCurrentPrevious(rollbackCtx, group); err != nil {
		return fmt.Errorf("automatic rollback switch on %s: %w", group.Label, err)
	}

	rollbackErrors := make([]error, 0)
	for _, target := range targets {
		// 健康的新版本 WebSocket 实例也要先排空，再切回旧版本。
		if err := d.drainTarget(rollbackCtx, group, target); err != nil {
			d.warn("automatic rollback drain failed %s/%s: %v", target.Service, target.EndpointID, err)
			rollbackErrors = append(rollbackErrors, err)
		}
		if err := d.ssh(rollbackCtx, group.Key, installInstanceCommand(group, target)); err != nil {
			d.warn("automatic rollback restart failed %s/%s: %v", target.Service, target.EndpointID, err)
			rollbackErrors = append(rollbackErrors, err)
			continue
		}
		if err := d.checkTargetHealth(rollbackCtx, group, target); err != nil {
			d.warn("automatic rollback health failed %s/%s: %v", target.Service, target.EndpointID, err)
			rollbackErrors = append(rollbackErrors, err)
		}
	}
	return errors.Join(rollbackErrors...)
}

// joinDeploymentRollbackError 同时保留原始发布错误和自动回滚错误。
func joinDeploymentRollbackError(deployErr error, rollbackErr error) error {
	if rollbackErr == nil {
		return deployErr
	}
	return errors.Join(deployErr, fmt.Errorf("automatic rollback failed: %w", rollbackErr))
}

// cleanupOldReleases 仅删除超出保留数量且未被 current、previous 或运行实例引用的历史目录。
func (d *Deployer) cleanupOldReleases(ctx context.Context, group RemoteGroup) {
	releasesDir := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "releases"))
	activeReleaseDir := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime", "active-releases"))
	releaseEnvDir := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime", "release-env"))
	currentLink := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current"))
	previousLink := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "previous"))
	script := fmt.Sprintf(
		"releases=%s; active=%s; release_env=%s; current=$(readlink -f %s 2>/dev/null || true); previous=$(readlink -f %s 2>/dev/null || true); count=0; for dir in $(ls -1dt \"$releases\"/* 2>/dev/null || true); do count=$((count+1)); in_use=0; for link in \"$active\"/*; do [ -L \"$link\" ] || continue; if [ \"$(readlink -f \"$link\")\" = \"$dir\" ]; then in_use=1; break; fi; done; if [ \"$count\" -le %d ] || [ \"$dir\" = \"$current\" ] || [ \"$dir\" = \"$previous\" ] || [ \"$in_use\" -eq 1 ]; then continue; fi; release_name=$(basename \"$dir\"); rm -rf -- \"$dir\"; rm -f -- \"$release_env/$release_name.env\"; done",
		shellQuote(releasesDir),
		shellQuote(activeReleaseDir),
		shellQuote(releaseEnvDir),
		shellQuote(currentLink),
		shellQuote(previousLink),
		d.cfg.RetainReleases,
	)
	_ = d.ssh(ctx, group.Key, runRemoteAsRoot(group.Key.SSHUser, script))
}

// deploymentRollbackTimeout 根据已发布实例数量预留排空、重启和健康检查时间。
func deploymentRollbackTimeout(cfg Config, groups []RemoteGroup) time.Duration {
	seconds := 60
	for _, group := range groups {
		seconds += len(group.Targets) * (cfg.DrainTimeoutSeconds + cfg.HealthTimeoutSeconds + 30)
	}
	// 小规模集群也至少保留五分钟，覆盖 SSH 抖动和 systemd 重启耗时。
	if seconds < 300 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}
