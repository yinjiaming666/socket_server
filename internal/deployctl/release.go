package deployctl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

// DeploymentHistoryEntry 是写入远端 JSONL 审计日志的一条发布记录。
type DeploymentHistoryEntry struct {
	Time      string `json:"time"`
	Action    string `json:"action"`
	Release   string `json:"release"`
	Status    string `json:"status"`
	Instances int    `json:"instances"`
}

// runRemoteAsRoot 将多条相关命令放入同一个 root shell，避免非 root 部署时逐条处理 sudo。
func runRemoteAsRoot(user string, script string) string {
	command := "bash -lc " + shellQuote(script)
	if user == "root" {
		return command
	}
	return "sudo " + command
}

// acquireDeploymentLock 使用原子 mkdir 阻止同一部署目录被两个发布任务同时修改。
func (d *Deployer) acquireDeploymentLock(ctx context.Context, group RemoteGroup, owner string) error {
	runtimeDir := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime"))
	lockDir := filepath.ToSlash(filepath.Join(runtimeDir, "deploy.lock"))
	ownerFile := filepath.ToSlash(filepath.Join(lockDir, "owner"))
	script := fmt.Sprintf(
		"set -eu; mkdir -p %s; if ! mkdir %s 2>/dev/null; then holder=$(cat %s 2>/dev/null || printf unknown); echo \"deployment lock already held by $holder: %s\" >&2; exit 73; fi; printf '%%s%c' %s > %s",
		shellQuote(runtimeDir),
		shellQuote(lockDir),
		shellQuote(ownerFile),
		lockDir,
		byte(10),
		shellQuote(owner),
		shellQuote(ownerFile),
	)
	if err := d.ssh(ctx, group.Key, runRemoteAsRoot(group.Key.SSHUser, script)); err != nil {
		return fmt.Errorf("acquire deployment lock on %s: %w", group.Label, err)
	}
	return nil
}

// releaseDeploymentLock 仅在 owner 匹配时释放远端部署锁，避免误删其它发布任务的锁。
func (d *Deployer) releaseDeploymentLock(ctx context.Context, group RemoteGroup, owner string) {
	lockDir := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime", "deploy.lock"))
	ownerFile := filepath.ToSlash(filepath.Join(lockDir, "owner"))
	script := fmt.Sprintf(
		"if [ -f %s ] && [ \"$(cat %s)\" = %s ]; then rm -rf %s; fi",
		shellQuote(ownerFile),
		shellQuote(ownerFile),
		shellQuote(owner),
		shellQuote(lockDir),
	)
	_ = d.ssh(ctx, group.Key, runRemoteAsRoot(group.Key.SSHUser, script))
}

// recordDeploymentHistory 以 JSONL 追加远端发布历史，单行损坏不会影响其它记录。
func (d *Deployer) recordDeploymentHistory(ctx context.Context, group RemoteGroup, action string, release string, status string) {
	entry := DeploymentHistoryEntry{
		Time:      d.cfg.Now().UTC().Format(time.RFC3339),
		Action:    action,
		Release:   release,
		Status:    status,
		Instances: len(group.Targets),
	}
	body, err := json.Marshal(&entry)
	if err != nil {
		return
	}
	runtimeDir := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime"))
	historyFile := filepath.ToSlash(filepath.Join(runtimeDir, "deploy-history.jsonl"))
	script := fmt.Sprintf("mkdir -p %s; printf '%%s%c' %s >> %s",
		shellQuote(runtimeDir),
		byte(10),
		shellQuote(string(body)),
		shellQuote(historyFile),
	)
	_ = d.ssh(ctx, group.Key, runRemoteAsRoot(group.Key.SSHUser, script))
}

// HistoryGroup 输出某台物理服务器最近 50 条发布与回滚记录。
func (d *Deployer) HistoryGroup(ctx context.Context, group RemoteGroup) error {
	historyFile := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime", "deploy-history.jsonl"))
	script := fmt.Sprintf("if [ -f %s ]; then tail -n 50 %s; else echo 'no deployment history'; fi",
		shellQuote(historyFile),
		shellQuote(historyFile),
	)
	return d.ssh(ctx, group.Key, runRemoteAsRoot(group.Key.SSHUser, script))
}

// swapCurrentPrevious 原子交换 current 和 previous 指向，供手工回滚及失败恢复复用。
func (d *Deployer) swapCurrentPrevious(ctx context.Context, group RemoteGroup) error {
	currentLink := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current"))
	previousLink := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "previous"))
	script := fmt.Sprintf(
		"set -eu; test -L %s; test -L %s; current_target=$(readlink -f %s); previous_target=$(readlink -f %s); test -d \"$current_target\"; test -d \"$previous_target\"; ln -sfn \"$previous_target\" %s; ln -sfn \"$current_target\" %s",
		shellQuote(currentLink),
		shellQuote(previousLink),
		shellQuote(currentLink),
		shellQuote(previousLink),
		shellQuote(currentLink),
		shellQuote(previousLink),
	)
	return d.ssh(ctx, group.Key, runRemoteAsRoot(group.Key.SSHUser, script))
}

// installInstanceCommand 使用主机级稳定安装器，让旧 release 回滚仍保留新版 unit 规则。
func installInstanceCommand(group RemoteGroup, target Target) string {
	sudo := sudoPrefix(group.Key.SSHUser)
	return fmt.Sprintf("%sbash %s %s %s %s",
		sudo,
		shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "install-instance"))),
		shellQuote(group.Key.DeployDir),
		shellQuote(target.Service),
		shellQuote(target.EndpointID))
}

// ensureStableInstaller 在切换版本链接前确认回滚所需安装器已经就绪。
func (d *Deployer) ensureStableInstaller(ctx context.Context, group RemoteGroup) error {
	installer := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "install-instance"))
	script := fmt.Sprintf(
		"if [ ! -x %s ]; then echo 'stable install-instance is missing; run deployctl syncctl before rollback' >&2; exit 74; fi",
		shellQuote(installer),
	)
	if err := d.ssh(ctx, group.Key, runRemoteAsRoot(group.Key.SSHUser, script)); err != nil {
		return fmt.Errorf("rollback preflight on %s failed; run go run ./cmd/deployctl syncctl all first: %w", group.Label, err)
	}
	return nil
}

// RollbackGroups 按发布的相反顺序回滚已完成主机，并汇总所有回滚错误。
func (d *Deployer) RollbackGroups(ctx context.Context, groups []RemoteGroup) error {
	rollbackErrors := make([]error, 0)
	for index := len(groups) - 1; index >= 0; index-- {
		group := groups[index]
		d.info("rollback completed deployment on %s", group.Label)
		if err := d.RollbackGroup(ctx, group); err != nil {
			rollbackErrors = append(rollbackErrors, fmt.Errorf("%s: %w", group.Label, err))
		}
	}
	return errors.Join(rollbackErrors...)
}

// RollbackGroup 将 current 切回 previous，并依次重启选中的服务实例。
func (d *Deployer) RollbackGroup(ctx context.Context, group RemoteGroup) error {
	owner := "rollback-" + d.releaseID
	if err := d.acquireDeploymentLock(ctx, group, owner); err != nil {
		return err
	}
	success := false
	defer func() {
		status := "failed"
		if success {
			status = "success"
		}
		// 审计写入失败不能阻止锁释放，因此两个收尾动作使用独立超时。
		historyCtx, historyCancel := context.WithTimeout(context.Background(), 5*time.Second)
		d.recordDeploymentHistory(historyCtx, group, "rollback", "previous", status)
		historyCancel()
		lockCtx, lockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		d.releaseDeploymentLock(lockCtx, group, owner)
		lockCancel()
	}()

	// 先做无副作用预检，避免 current 已切换后才发现迁移服务器缺少稳定安装器。
	if err := d.ensureStableInstaller(ctx, group); err != nil {
		return err
	}
	if err := d.swapCurrentPrevious(ctx, group); err != nil {
		return err
	}

	restartedTargets := make([]Target, 0, len(group.Targets))
	for _, target := range group.Targets {
		// current 切换后仍逐实例排空和重启，避免同一主机上的 WebSocket 节点同时退出就绪状态。
		restoreTargets := append(append([]Target(nil), restartedTargets...), target)
		if err := d.drainTarget(ctx, group, target); err != nil {
			rollbackErr := d.rollbackTargetsAfterFailure(context.Background(), group, restoreTargets)
			return joinDeploymentRollbackError(err, rollbackErr)
		}
		if err := d.ssh(ctx, group.Key, installInstanceCommand(group, target)); err != nil {
			// 回滚版本启动失败时重新切回发布前版本，并恢复已经处理过的实例。
			rollbackErr := d.rollbackTargetsAfterFailure(context.Background(), group, restoreTargets)
			return joinDeploymentRollbackError(err, rollbackErr)
		}
		if err := d.checkTargetHealth(ctx, group, target); err != nil {
			rollbackErr := d.rollbackTargetsAfterFailure(context.Background(), group, restoreTargets)
			return joinDeploymentRollbackError(err, rollbackErr)
		}
		restartedTargets = append(restartedTargets, target)
	}
	success = true
	d.success("%s rollback done", group.Label)
	return nil
}
