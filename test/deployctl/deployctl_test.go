package deployctl_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"socket_server/internal/deployctl"
)

const testTargetsJSON = `{
  "servers": [
    {
      "id": "gateway-server-1",
      "host": "43.128.56.217",
      "ssh_user": "root",
      "ssh_port": 22,
      "deploy_dir": "/opt/socket-server",
      "instances": [
        {
          "service": "gateway",
          "endpoint_id": "gateway-1"
        }
      ]
    },
    {
      "id": "message-server-1",
      "host": "43.128.56.217",
      "ssh_user": "root",
      "ssh_port": 22,
      "deploy_dir": "/opt/socket-server",
      "instances": [
        {
          "service": "message-api",
          "endpoint_id": "message-api-1"
        }
      ]
    },
    {
      "id": "ws-server-1",
      "host": "43.128.56.217",
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
    },
    {
      "id": "ws-server-2",
      "host": "43.128.56.217",
      "ssh_user": "root",
      "ssh_port": 22,
      "deploy_dir": "/opt/socket-server",
      "instances": [
        {
          "service": "websocket-node",
          "endpoint_id": "ws-node-3"
        }
      ]
    }
  ]
}`

const testAppConfigJSON = `{
  "storage": "redis",
  "route_token": {"secret": "test-secret", "ttl_seconds": 30},
  "gateways": [
    {"endpoint_id": "gateway-1", "addr": "127.0.0.1:7000", "port": 7000}
  ],
  "message_apis": [
    {"endpoint_id": "message-api-1", "addr": "127.0.0.1:7100", "port": 7100}
  ],
  "nodes": [
    {"endpoint_id": "ws-node-1", "port": 8081, "grpc_port": 9001, "ws_path": "/ws/ws-node-1", "ws_addr": "127.0.0.1:8081", "grpc_addr": "127.0.0.1:9001"},
    {"endpoint_id": "ws-node-2", "port": 8082, "grpc_port": 9002, "ws_path": "/ws/ws-node-2", "ws_addr": "127.0.0.1:8082", "grpc_addr": "127.0.0.1:9002"},
    {"endpoint_id": "ws-node-3", "port": 8083, "grpc_port": 9003, "ws_path": "/ws/ws-node-3", "ws_addr": "127.0.0.1:8083", "grpc_addr": "127.0.0.1:9003"}
  ]
}`

// testTwoRemoteTargetsJSON 构造两个物理主机，用于验证跨主机失败回滚。
const testTwoRemoteTargetsJSON = `{
  "servers": [
    {
      "id": "gateway-server-1",
      "host": "10.0.0.1",
      "ssh_user": "root",
      "ssh_port": 22,
      "deploy_dir": "/opt/socket-server",
      "instances": [
        {"service": "gateway", "endpoint_id": "gateway-1"}
      ]
    },
    {
      "id": "message-server-1",
      "host": "10.0.0.2",
      "ssh_user": "root",
      "ssh_port": 22,
      "deploy_dir": "/opt/socket-server",
      "instances": [
        {"service": "message-api", "endpoint_id": "message-api-1"}
      ]
    }
  ]
}`

type fakeRunner struct {
	commands []deployctl.Command
}

func (r *fakeRunner) Run(ctx context.Context, cmd deployctl.Command, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	r.commands = append(r.commands, cmd)
	return nil
}

// failOnceRunner 在首条同时命中全部关键字的命令上返回错误，用于模拟远端发布失败。
type failOnceRunner struct {
	commands []deployctl.Command
	matches  []string
	failed   bool
}

func (r *failOnceRunner) Run(ctx context.Context, cmd deployctl.Command, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	r.commands = append(r.commands, cmd)
	line := cmd.Name + " " + strings.Join(cmd.Args, " ")
	matched := !r.failed
	for _, value := range r.matches {
		if !strings.Contains(line, value) {
			matched = false
			break
		}
	}
	if matched {
		r.failed = true
		return errors.New("simulated remote failure")
	}
	return nil
}

type fakeBuildRunner struct {
	commands []deployctl.Command
}

func (r *fakeBuildRunner) Run(ctx context.Context, cmd deployctl.Command, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	r.commands = append(r.commands, cmd)
	if cmd.Name != "go" {
		return nil
	}
	for i, arg := range cmd.Args {
		if arg == "-o" && i+1 < len(cmd.Args) {
			output := cmd.Args[i+1]
			if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
				return err
			}
			return os.WriteFile(output, []byte("fake binary"), 0o755)
		}
	}
	return nil
}

func TestDeployAllSelectsFiveInstancesOnOneRemote(t *testing.T) {
	targetsFile := &deployctl.TargetsFile{}
	if err := json.Unmarshal([]byte(testTargetsJSON), targetsFile); err != nil {
		t.Fatal(err)
	}

	targets, err := deployctl.SelectTargets(targetsFile, deployctl.Filter{Server: "all", Service: "all", Endpoint: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 5 {
		t.Fatalf("selected targets = %d, want 5", len(targets))
	}

	groups := deployctl.GroupTargets(targets)
	if len(groups) != 1 {
		t.Fatalf("remote groups = %d, want 1", len(groups))
	}
	if len(groups[0].Targets) != 5 {
		t.Fatalf("remote group target count = %d, want 5", len(groups[0].Targets))
	}
	if groups[0].Label != "gateway-server-1,message-server-1,ws-server-1,ws-server-2" {
		t.Fatalf("remote group label = %q", groups[0].Label)
	}
}

func TestDeployAllRunsInstallForEverySelectedInstance(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	appConfigPath := filepath.Join(tmp, "configs", "app.json")
	runtimeEnvPath := filepath.Join(tmp, "runtime.env")
	packagePath := filepath.Join(tmp, "socket-server.tar.gz")
	controlDir := filepath.Join(tmp, "ssh")
	if err := os.MkdirAll(filepath.Dir(appConfigPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(appConfigPath, []byte(testAppConfigJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(runtimeEnvPath, []byte("REDIS_PASSWORD=test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetsPath, []byte(testTargetsJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	writeTestDeploymentPackage(t, packagePath, "test-release")

	runner := &fakeRunner{}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cfg := deployctl.Config{
		RootDir:           tmp,
		TargetsFile:       targetsPath,
		AppConfig:         appConfigPath,
		PackageFile:       packagePath,
		RuntimeEnv:        runtimeEnvPath,
		SkipBuild:         true,
		SSHMultiplex:      true,
		SSHControlPersist: "10m",
		SSHControlDir:     controlDir,
		Runner:            runner,
		Stdout:            &stdout,
		Stderr:            &stderr,
		Now: func() time.Time {
			return time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
		},
	}

	if err := deployctl.Execute(context.Background(), cfg, []string{"deploy", "all"}); err != nil {
		t.Fatal(err)
	}

	// 所有实例都必须通过宿主机稳定路径安装，避免 current 切换或回滚时执行错版本脚本。
	installCommands := sshRemoteCommandsContaining(runner.commands, "bash", "'/opt/socket-server/install-instance'")
	if len(installCommands) != 5 {
		t.Fatalf("stable install-instance ssh commands = %d, want 5\ncommands:\n%s", len(installCommands), formatCommands(runner.commands))
	}
	legacyInstallCommands := sshRemoteCommandsContaining(runner.commands, "bash '/opt/socket-server/current/deploy/install-instance.sh'")
	if len(legacyInstallCommands) != 0 {
		t.Fatalf("deployment must not invoke installer through current symlink\ncommands:\n%s", formatCommands(legacyInstallCommands))
	}
	for _, want := range []string{
		"gateway-1",
		"message-api-1",
		"ws-node-1",
		"ws-node-2",
		"ws-node-3",
	} {
		if !commandsContain(installCommands, want) {
			t.Fatalf("install commands missing %s\ncommands:\n%s", want, formatCommands(installCommands))
		}
	}

	// 发布归档和对应摘要必须各上传一次，确保远端可以在切换版本前校验完整性。
	artifactUploads := commandArgsContaining(runner.commands, "scp", packagePath)
	if len(artifactUploads) != 2 {
		t.Fatalf("artifact uploads = %d, want 2\ncommands:\n%s", len(artifactUploads), formatCommands(runner.commands))
	}
	if !commandsContain(artifactUploads, packagePath+".sha256") {
		t.Fatalf("artifact uploads missing checksum\ncommands:\n%s", formatCommands(artifactUploads))
	}

	// 认证预热可能先执行 ssh true；排除它后，加锁仍必须发生在上传之前。
	firstBusinessRemote := -1
	for i, command := range runner.commands {
		if command.Name == "ssh" && len(command.Args) > 0 && command.Args[len(command.Args)-1] == "true" {
			continue
		}
		firstBusinessRemote = i
		break
	}
	if firstBusinessRemote < 0 || runner.commands[firstBusinessRemote].Name != "ssh" || !commandsContain(runner.commands[firstBusinessRemote:firstBusinessRemote+1], "deployment lock already held") {
		t.Fatalf("deployment lock must be the first business remote action\ncommands:\n%s", formatCommands(runner.commands))
	}
	for _, want := range []string{
		"sha256sum",
		".artifact.sha256",
		"/opt/socket-server/previous",
		"--signal=SIGUSR1",
		"/readyz",
		"deploy-history.jsonl",
		"active-releases",
		"runtime/release-env/test-release.env",
	} {
		if !commandsContain(runner.commands, want) {
			t.Fatalf("deployment commands missing %q\ncommands:\n%s", want, formatCommands(runner.commands))
		}
	}
	assertRemoteShellSyntax(t, runner.commands)

	if !strings.Contains(stdout.String(), "instances=5") {
		t.Fatalf("plan output should show instances=5, got:\n%s", stdout.String())
	}
}

func TestDeployFailureRollsBackCompletedRemoteGroups(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	appConfigPath := filepath.Join(tmp, "app.json")
	packagePath := filepath.Join(tmp, "socket-server.tar.gz")
	for path, body := range map[string]string{
		targetsPath:   testTwoRemoteTargetsJSON,
		appConfigPath: testAppConfigJSON,
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeTestDeploymentPackage(t, packagePath, "test-release")

	// 第二台主机健康检查首次失败，第一台已发布主机必须被自动回滚。
	runner := &failOnceRunner{matches: []string{"root@10.0.0.2", "http://127.0.0.1:7100/readyz"}}
	cfg := deployctl.Config{
		RootDir:              tmp,
		TargetsFile:          targetsPath,
		AppConfig:            appConfigPath,
		PackageFile:          packagePath,
		RuntimeEnv:           filepath.Join(tmp, "missing.env"),
		SkipBuild:            true,
		SSHMultiplex:         false,
		HealthTimeoutSeconds: 1,
		DrainTimeoutSeconds:  1,
		Runner:               runner,
		Stdout:               io.Discard,
		Stderr:               io.Discard,
		Now: func() time.Time {
			return time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
		},
	}
	if err := deployctl.Execute(context.Background(), cfg, []string{"deploy", "all"}); err == nil {
		t.Fatal("deploy should return the simulated second-host failure")
	}
	if !runner.failed {
		t.Fatal("second-host health check failure was not triggered")
	}

	rollbackCommands := commandsContainingAll(runner.commands, "root@10.0.0.1", "current_target=$(readlink -f")
	if len(rollbackCommands) == 0 {
		t.Fatalf("completed first host was not rolled back\ncommands:\n%s", formatCommands(runner.commands))
	}
	if !commandsContain(commandsContainingAll(runner.commands, "root@10.0.0.1"), "\"action\":\"rollback\"") {
		t.Fatalf("first-host rollback history was not recorded\ncommands:\n%s", formatCommands(runner.commands))
	}
}

func TestDeployRestoresCurrentAfterPostSwitchFailure(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	appConfigPath := filepath.Join(tmp, "app.json")
	packagePath := filepath.Join(tmp, "socket-server.tar.gz")
	for path, body := range map[string]string{
		targetsPath:   testTwoRemoteTargetsJSON,
		appConfigPath: testAppConfigJSON,
	} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeTestDeploymentPackage(t, packagePath, "test-release")

	// current 切换后复制管理脚本失败，必须在释放部署锁前恢复 previous。
	runner := &failOnceRunner{matches: []string{"root@10.0.0.1", "current/deploy/socketctl.sh"}}
	err := deployctl.Execute(context.Background(), deployctl.Config{
		RootDir:              tmp,
		TargetsFile:          targetsPath,
		AppConfig:            appConfigPath,
		PackageFile:          packagePath,
		RuntimeEnv:           filepath.Join(tmp, "missing.env"),
		SkipBuild:            true,
		SSHMultiplex:         false,
		HealthTimeoutSeconds: 1,
		DrainTimeoutSeconds:  1,
		Runner:               runner,
		Stdout:               io.Discard,
		Stderr:               io.Discard,
		Now: func() time.Time {
			return time.Date(2026, 7, 11, 13, 0, 0, 0, time.UTC)
		},
	}, []string{"deploy", "gateway-server-1"})
	if err == nil || !runner.failed {
		t.Fatalf("post-switch failure was not returned: %v", err)
	}
	if len(commandsContainingAll(runner.commands, "root@10.0.0.1", "current_target=$(readlink -f")) == 0 {
		t.Fatalf("current was not restored after post-switch failure\ncommands:\n%s", formatCommands(runner.commands))
	}
}

func TestSelectTargetsRejectsUnsafeAndDuplicateInstances(t *testing.T) {
	baseServer := deployctl.Server{
		ID:        "server-1",
		Host:      "10.0.0.1",
		SSHUser:   "root",
		SSHPort:   deployctl.IntOrStr("22"),
		DeployDir: "/opt/socket-server",
	}

	t.Run("unsafe endpoint", func(t *testing.T) {
		server := baseServer
		server.Instances = []deployctl.Instance{{Service: "gateway", EndpointID: "../../gateway"}}
		_, err := deployctl.SelectTargets(&deployctl.TargetsFile{Servers: []deployctl.Server{server}}, deployctl.Filter{})
		if err == nil || !strings.Contains(err.Error(), "endpoint_id must use") {
			t.Fatalf("unsafe endpoint error = %v", err)
		}
	})

	t.Run("duplicate physical instance", func(t *testing.T) {
		first := baseServer
		first.Instances = []deployctl.Instance{{Service: "gateway", EndpointID: "gateway-1"}}
		second := baseServer
		second.ID = "server-2"
		second.Instances = []deployctl.Instance{{Service: "gateway", EndpointID: "gateway-1"}}
		_, err := deployctl.SelectTargets(&deployctl.TargetsFile{Servers: []deployctl.Server{first, second}}, deployctl.Filter{})
		if err == nil || !strings.Contains(err.Error(), "duplicate instance") {
			t.Fatalf("duplicate instance error = %v", err)
		}
	})
}

func TestSelectWebsocketAllReturnsThreeNodes(t *testing.T) {
	targetsFile := &deployctl.TargetsFile{}
	if err := json.Unmarshal([]byte(testTargetsJSON), targetsFile); err != nil {
		t.Fatal(err)
	}

	targets, err := deployctl.SelectTargets(targetsFile, deployctl.Filter{Server: "all", Service: "websocket-node", Endpoint: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 3 {
		t.Fatalf("websocket targets = %d, want 3", len(targets))
	}
}

func TestSyncCtlUpdatesStableScriptsWithoutDeployingServices(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	localSocketCtl := filepath.Join(tmp, "deploy", "socketctl.sh")
	localInstaller := filepath.Join(tmp, "deploy", "install-instance.sh")
	if err := os.WriteFile(targetsPath, []byte(testTargetsJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(localSocketCtl), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, script := range []string{localSocketCtl, localInstaller} {
		if err := os.WriteFile(script, []byte("#!/usr/bin/env bash\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	runner := &fakeRunner{}
	var stdout bytes.Buffer
	cfg := deployctl.Config{
		RootDir:      tmp,
		TargetsFile:  targetsPath,
		SSHMultiplex: false,
		Runner:       runner,
		Stdout:       &stdout,
		Stderr:       io.Discard,
		Now: func() time.Time {
			return time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
		},
	}

	if err := deployctl.Execute(context.Background(), cfg, []string{"syncctl", "all"}); err != nil {
		t.Fatal(err)
	}

	for name, script := range map[string]string{
		"socketctl":        localSocketCtl,
		"install-instance": localInstaller,
	} {
		uploads := commandArgsContaining(runner.commands, "scp", script)
		if len(uploads) != 1 {
			t.Fatalf("%s uploads = %d, want 1\ncommands:\n%s", name, len(uploads), formatCommands(runner.commands))
		}
	}
	// 同步安装器文件不等于执行安装器，syncctl 不能触发任何实例重启。
	serviceInstallCommands := sshRemoteCommandsContaining(runner.commands, "bash '/opt/socket-server/install-instance'")
	if len(serviceInstallCommands) != 0 {
		t.Fatalf("syncctl must not install services\ncommands:\n%s", formatCommands(serviceInstallCommands))
	}
	if commandsContain(runner.commands, "tar -xzf") || commandsContain(runner.commands, "systemctl") {
		t.Fatalf("syncctl must not build, deploy, or manage services\ncommands:\n%s", formatCommands(runner.commands))
	}
	if !commandsContain(runner.commands, "chmod 755") ||
		!commandsContain(runner.commands, "mv -f") ||
		!commandsContain(runner.commands, "/opt/socket-server/install-instance") ||
		!strings.Contains(stdout.String(), "stable control scripts updated") {
		t.Fatalf("syncctl should atomically install both scripts and report success\ncommands:\n%s\noutput:\n%s", formatCommands(runner.commands), stdout.String())
	}
}

func TestListUsesSelectedInstancesOnly(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	appConfigPath := filepath.Join(tmp, "configs", "app.json")
	controlDir := filepath.Join(tmp, "ssh")
	if err := os.MkdirAll(filepath.Dir(appConfigPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetsPath, []byte(testTargetsJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(appConfigPath, []byte(testAppConfigJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	runner := &fakeRunner{}
	var stdout bytes.Buffer
	cfg := deployctl.Config{
		RootDir:           tmp,
		TargetsFile:       targetsPath,
		AppConfig:         appConfigPath,
		SSHMultiplex:      true,
		SSHControlPersist: "10m",
		SSHControlDir:     controlDir,
		Runner:            runner,
		Stdout:            &stdout,
		Stderr:            io.Discard,
	}
	if err := deployctl.Execute(context.Background(), cfg, []string{"list", "ws-server-1"}); err != nil {
		t.Fatal(err)
	}

	listCommands := sshRemoteCommandsContaining(runner.commands, "service_state")
	if len(listCommands) != 2 {
		t.Fatalf("inline list commands = %d, want 2\ncommands:\n%s", len(listCommands), formatCommands(runner.commands))
	}
	for _, want := range []string{"ws-node-1", "ws-node-2", "8081/9001", "8082/9002", "127.0.0.1"} {
		if !commandsContain(listCommands, want) {
			t.Fatalf("list commands missing %s\ncommands:\n%s", want, formatCommands(listCommands))
		}
	}
	for _, unexpected := range []string{"gateway-1", "message-api-1", "ws-node-3", "'all' 'all'"} {
		if commandsContain(listCommands, unexpected) {
			t.Fatalf("list commands should not include %s\ncommands:\n%s", unexpected, formatCommands(listCommands))
		}
	}
	if commandsContain(listCommands, "socketctl") {
		t.Fatalf("list should not depend on remote socketctl\ncommands:\n%s", formatCommands(listCommands))
	}
	for _, want := range []string{"SERVER", "DEPLOY_DIR", "PORT", "IP", "STATUS", "UPDATED_AT"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("list should print the unified header containing %q, got:\n%s", want, stdout.String())
		}
	}
	if strings.Contains(stdout.String(), "==> list") {
		t.Fatalf("list rows should not be interrupted by progress lines, got:\n%s", stdout.String())
	}
}

func TestStatusIsListAlias(t *testing.T) {
	req, err := deployctl.ParseArgs([]string{"status", "ws-server-1", "websocket-node", "ws-node-1"})
	if err != nil {
		t.Fatal(err)
	}
	if req.Command != "list" {
		t.Fatalf("status command = %q, want list", req.Command)
	}
	if req.Filter.Server != "ws-server-1" || req.Filter.Service != "websocket-node" || req.Filter.Endpoint != "ws-node-1" {
		t.Fatalf("status alias filters = %+v", req.Filter)
	}

	var output bytes.Buffer
	deployctl.PrintHelp(&output, "status")
	if !strings.Contains(output.String(), "已合并到 list") {
		t.Fatalf("status help should explain the list alias, got:\n%s", output.String())
	}
}

func TestRemoteListScriptMapsSystemdStatesToDockerStyle(t *testing.T) {
	runner := &fakeRunner{}
	deployer := deployctl.NewDeployer(deployctl.Config{RootDir: t.TempDir(), Runner: runner, Stdout: io.Discard, Stderr: io.Discard})
	group := deployctl.RemoteGroup{
		Key: deployctl.RemoteKey{Host: "127.0.0.1", SSHUser: "root", SSHPort: "22"},
		Targets: []deployctl.Target{{
			ServerID:   "ws-server-1",
			DeployDir:  "/opt/socket-server",
			Service:    "websocket-node",
			EndpointID: "ws-node-1",
		}},
	}
	if err := deployer.ListGroup(t.Context(), group); err != nil {
		t.Fatal(err)
	}
	listCommands := sshRemoteCommandsContaining(runner.commands, "service_state")
	if len(listCommands) != 1 {
		t.Fatalf("expected one list command, got %v", listCommands)
	}
	args := listCommands[0].Args
	script := args[len(args)-1]
	if strings.Contains(script, "socketctl") {
		t.Fatalf("remote list script should not call socketctl:\n%s", script)
	}
	for _, want := range []string{
		"--property=SubState",
		"activating:auto-restart)",
		`"running"`,
		`"starting"`,
		`"restarting"`,
		`"stopping"`,
		`"stopped"`,
		`"failed"`,
		`"not-found"`,
		`"unknown"`,
		`state="not-deployed"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("remote list script missing Docker-style state mapping %q:\n%s", want, script)
		}
	}
	if strings.Contains(script, "unit-not-found") || strings.Contains(script, "systemctl-unavailable") {
		t.Fatalf("remote list script should not expose raw systemd fallback labels:\n%s", script)
	}
	if strings.Index(script, "service_state()") > strings.Index(script, `state="$(service_state "$unit")"`) {
		t.Fatalf("remote list script should define service_state before calling it:\n%s", script)
	}
}

func TestHelpListExplainsDockerStyleStates(t *testing.T) {
	runner := &fakeRunner{}
	var stdout bytes.Buffer
	cfg := deployctl.Config{
		RootDir: "/tmp",
		Runner:  runner,
		Stdout:  &stdout,
		Stderr:  io.Discard,
	}
	if err := deployctl.Execute(context.Background(), cfg, []string{"help", "list"}); err != nil {
		t.Fatal(err)
	}
	output := stdout.String()
	for _, want := range []string{"状态说明", "Docker 风格", "HTTP/gRPC", "running", "restarting", "stopped", "not-found", "not-deployed", "unknown", "go run ./cmd/deployctl list"} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output missing %q:\n%s", want, output)
		}
	}
	if len(runner.commands) != 0 {
		t.Fatalf("help should not run commands, got:\n%s", formatCommands(runner.commands))
	}
}

func TestHelpDocumentsReleaseOperations(t *testing.T) {
	for topic, wants := range map[string][]string{
		"rollback": {"全部实例", "current", "previous"},
		"history":  {"最近 50 条", "审计"},
		"":         {"参数说明", "RELEASE_VERSION", "DEPLOY_DRAIN_TIMEOUT_SECONDS=30", "已合并到 list"},
	} {
		var stdout bytes.Buffer
		cfg := deployctl.Config{RootDir: "/tmp", Runner: &fakeRunner{}, Stdout: &stdout, Stderr: io.Discard}
		args := []string{"help"}
		if topic != "" {
			args = append(args, topic)
		}
		if err := deployctl.Execute(context.Background(), cfg, args); err != nil {
			t.Fatal(err)
		}
		for _, want := range wants {
			if !strings.Contains(stdout.String(), want) {
				t.Fatalf("help %q missing %q:\n%s", topic, want, stdout.String())
			}
		}
	}
}

func TestDefaultHelpExplainsEveryCommand(t *testing.T) {
	var output bytes.Buffer
	deployctl.PrintHelp(&output, "")

	for _, want := range []string{
		"查看指定命令的详细说明。",
		"仅预览部署目标，不连接服务器。",
		"构建制品并滚动发布选中实例。",
		"等同于 deploy all，发布全部实例。",
		"将选中物理主机回滚到上一版本。",
		"查看发布和回滚审计记录。",
		"同步远端 socketctl 和安装脚本。",
		"查看端口、监听 IP 和服务状态。",
		"启动选中服务。",
		"暂停服务（等同于 stop）。",
		"停止选中服务。",
		"重启选中服务。",
		"查看最近服务日志。",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("default help missing %q:\n%s", want, output.String())
		}
	}
}

func TestBuildPackageCreatesTarGzFromGoTooling(t *testing.T) {
	tmp := t.TempDir()
	appConfig := filepath.Join(tmp, "configs", "app.json")
	packagePath := filepath.Join(tmp, "runtime", "package", "socket-server.tar.gz")
	buildDir := filepath.Join(tmp, "runtime", "build")
	if err := os.MkdirAll(filepath.Dir(appConfig), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(tmp, "deploy"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(appConfig, []byte(`{"server":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"install-instance.sh", "socketctl.sh"} {
		if err := os.WriteFile(filepath.Join(tmp, "deploy", name), []byte("#!/usr/bin/env bash\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	runner := &fakeBuildRunner{}
	var stdout bytes.Buffer
	cfg := deployctl.Config{
		RootDir:     tmp,
		AppConfig:   appConfig,
		PackageFile: packagePath,
		BuildDir:    buildDir,
		Runner:      runner,
		Stdout:      &stdout,
		Stderr:      io.Discard,
	}
	deployer := deployctl.NewDeployer(cfg)
	if err := deployer.BuildPackage(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(runner.commands) != 3 {
		t.Fatalf("go build commands = %d, want 3", len(runner.commands))
	}

	entries := readTarGzEntries(t, packagePath)
	for _, want := range []string{
		"bin/gateway",
		"bin/message-api",
		"bin/websocket-node",
		"configs/app.json",
		"deploy/install-instance.sh",
		"deploy/socketctl.sh",
		"manifest.json",
	} {
		if _, ok := entries[want]; !ok {
			t.Fatalf("package missing %s; entries=%v", want, sortedKeys(entries))
		}
	}
	// 本地摘要必须与最终归档一致，避免上传阶段才发现制品损坏。
	body, err := os.ReadFile(packagePath)
	if err != nil {
		t.Fatal(err)
	}
	checksum, err := os.ReadFile(packagePath + ".sha256")
	if err != nil {
		t.Fatal(err)
	}
	wantChecksum := fmt.Sprintf("%x  %s\n", sha256.Sum256(body), filepath.Base(packagePath))
	if string(checksum) != wantChecksum {
		t.Fatalf("checksum = %q, want %q", checksum, wantChecksum)
	}
	info, err := os.Stat(packagePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("package mode = %o, want 600", info.Mode().Perm())
	}
}

func TestInstallInstancePinsImmutableRelease(t *testing.T) {
	root, err := deployctl.FindRootDir(".")
	if err != nil {
		t.Fatal(err)
	}
	scriptPath := filepath.Join(root, "deploy", "install-instance.sh")
	body, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)
	// systemd 必须固定到实体 release，并登记活动引用供安全清理逻辑使用。
	for _, want := range []string{
		`CURRENT_DIR="$(readlink -f "$DEPLOY_DIR/current")"`,
		"ExecStart=$BIN",
		"ACTIVE_RELEASE_LINK",
		"RUNTIME_ENV_FILE",
		"runtime/release-env",
		`ln -sfn "$CURRENT_DIR" "$ACTIVE_RELEASE_LINK"`,
		`"release_dir": "$CURRENT_DIR"`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("install script missing %q", want)
		}
	}
	check := exec.Command("bash", "-n", scriptPath)
	if output, err := check.CombinedOutput(); err != nil {
		t.Fatalf("install script syntax: %v\n%s", err, output)
	}
}

func TestCIDeployScriptValidatesBeforeRemoteDeployment(t *testing.T) {
	root, err := deployctl.FindRootDir(".")
	if err != nil {
		t.Fatal(err)
	}
	scriptPath := filepath.Join(root, "deploy", "ci-deploy.sh")
	body, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)
	// 自动化入口必须先测试和预览，再执行发布，并在退出时查询审计记录。
	for _, want := range []string{"go test ./...", " plan ", " deploy ", "trap print_history EXIT", "RELEASE_VERSION"} {
		if !strings.Contains(script, want) {
			t.Fatalf("CI deploy script missing %q", want)
		}
	}
	if strings.Index(script, "go test ./...") > strings.Index(script, `"$DEPLOYCTL_BIN" deploy`) {
		t.Fatal("CI deploy script must test before deployment")
	}
	check := exec.Command("bash", "-n", scriptPath)
	if output, err := check.CombinedOutput(); err != nil {
		t.Fatalf("CI deploy script syntax: %v\n%s", err, output)
	}
}

func TestSkipBuildReusesAndValidatesManifestRelease(t *testing.T) {
	packagePath := filepath.Join(t.TempDir(), "artifact.tar.gz")
	writeTestDeploymentPackage(t, packagePath, "release-one")

	// 未显式指定版本时复用制品版本，避免目录编号与 manifest 不一致。
	var output bytes.Buffer
	deployer := deployctl.NewDeployer(deployctl.Config{
		RootDir:     t.TempDir(),
		PackageFile: packagePath,
		SkipBuild:   true,
		Runner:      &fakeRunner{},
		Stdout:      &output,
		Stderr:      io.Discard,
		Now: func() time.Time {
			return time.Date(2026, 7, 11, 14, 0, 0, 0, time.UTC)
		},
	})
	if err := deployer.BuildPackage(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "reuse package release=release-one") {
		t.Fatalf("expected release-one to be reused, got %q", output.String())
	}

	mismatch := deployctl.NewDeployer(deployctl.Config{
		RootDir:        t.TempDir(),
		PackageFile:    packagePath,
		ReleaseVersion: "release-two",
		SkipBuild:      true,
		Runner:         &fakeRunner{},
		Stdout:         io.Discard,
		Stderr:         io.Discard,
	})
	if err := mismatch.BuildPackage(context.Background()); err == nil || !strings.Contains(err.Error(), "release mismatch") {
		t.Fatalf("mismatched release error = %v", err)
	}
}

func TestPackageChecksumRejectsModifiedArtifact(t *testing.T) {
	packagePath := filepath.Join(t.TempDir(), "artifact.tar.gz")
	writeTestDeploymentPackage(t, packagePath, "release-one")
	deployer := deployctl.NewDeployer(deployctl.Config{
		RootDir:     t.TempDir(),
		PackageFile: packagePath,
		SkipBuild:   true,
		Runner:      &fakeRunner{},
		Stdout:      io.Discard,
		Stderr:      io.Discard,
	})
	if err := deployer.BuildPackage(t.Context()); err != nil {
		t.Fatal(err)
	}

	// 摘要生成后修改归档，校验必须明确拒绝被篡改制品。
	if err := os.WriteFile(packagePath, []byte("modified"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := deployer.BuildPackage(t.Context()); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("verify modified package error = %v, want checksum mismatch", err)
	}
}

func TestParseArgsSupportsReleaseCommands(t *testing.T) {
	for _, command := range []string{"rollback", "history"} {
		req, err := deployctl.ParseArgs([]string{command, "ws-server-1", "websocket-node", "ws-node-1"})
		if err != nil {
			t.Fatalf("parse %s: %v", command, err)
		}
		if req.Command != command || req.Filter.Server != "ws-server-1" || req.Filter.Service != "websocket-node" || req.Filter.Endpoint != "ws-node-1" {
			t.Fatalf("parse %s returned unexpected request: %+v", command, req)
		}
	}
}

func TestRollbackPreflightsStableInstallerBeforeSwitch(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	appConfigPath := filepath.Join(tmp, "app.json")
	if err := os.WriteFile(targetsPath, []byte(testTwoRemoteTargetsJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(appConfigPath, []byte(testAppConfigJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	runner := &fakeRunner{}
	err := deployctl.Execute(context.Background(), deployctl.Config{
		RootDir:              tmp,
		TargetsFile:          targetsPath,
		AppConfig:            appConfigPath,
		SSHMultiplex:         false,
		HealthTimeoutSeconds: 1,
		DrainTimeoutSeconds:  1,
		Runner:               runner,
		Stdout:               io.Discard,
		Stderr:               io.Discard,
	}, []string{"rollback", "gateway-server-1"})
	if err != nil {
		t.Fatal(err)
	}

	preflightIndex := -1
	switchIndex := -1
	for index, command := range runner.commands {
		line := command.Name + " " + strings.Join(command.Args, " ")
		if strings.Contains(line, "stable install-instance is missing") {
			preflightIndex = index
		}
		if strings.Contains(line, "current_target=$(readlink -f") {
			switchIndex = index
		}
	}
	// 安装器缺失检查必须早于 current/previous 交换，预检失败时远端版本状态保持不变。
	if preflightIndex < 0 || switchIndex < 0 || preflightIndex >= switchIndex {
		t.Fatalf("rollback preflight index=%d switch index=%d\ncommands:\n%s", preflightIndex, switchIndex, formatCommands(runner.commands))
	}
}

func TestRollbackPreflightFailureKeepsVersionLinksUntouched(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	appConfigPath := filepath.Join(tmp, "app.json")
	if err := os.WriteFile(targetsPath, []byte(testTwoRemoteTargetsJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(appConfigPath, []byte(testAppConfigJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	// 模拟远端 test -x 失败，验证回滚不会继续交换 current 和 previous。
	runner := &failOnceRunner{matches: []string{"stable install-instance is missing"}}
	err := deployctl.Execute(context.Background(), deployctl.Config{
		RootDir:              tmp,
		TargetsFile:          targetsPath,
		AppConfig:            appConfigPath,
		SSHMultiplex:         false,
		HealthTimeoutSeconds: 1,
		DrainTimeoutSeconds:  1,
		Runner:               runner,
		Stdout:               io.Discard,
		Stderr:               io.Discard,
	}, []string{"rollback", "gateway-server-1"})
	if err == nil || !runner.failed {
		t.Fatalf("rollback should return installer preflight failure: %v", err)
	}
	if commandsContain(runner.commands, "current_target=$(readlink -f") {
		t.Fatalf("rollback must not switch version links after preflight failure\ncommands:\n%s", formatCommands(runner.commands))
	}
}

func TestRollbackRejectsPartialPhysicalHost(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	if err := os.WriteFile(targetsPath, []byte(testTargetsJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	// 多个逻辑 server 共享同一物理主机时，部分回滚会造成实例版本不一致，必须提前拒绝。
	err := deployctl.Execute(context.Background(), deployctl.Config{
		RootDir:     tmp,
		TargetsFile: targetsPath,
		Runner:      &fakeRunner{},
		Stdout:      io.Discard,
		Stderr:      io.Discard,
	}, []string{"rollback", "ws-server-1"})
	if err == nil || !strings.Contains(err.Error(), "all instances on physical host") {
		t.Fatalf("partial rollback error = %v", err)
	}
}

// assertRemoteShellSyntax 使用 bash 仅解析生成的远端命令，不执行其中操作。
func assertRemoteShellSyntax(t *testing.T, commands []deployctl.Command) {
	t.Helper()
	for _, command := range commands {
		if command.Name != "ssh" || len(command.Args) == 0 {
			continue
		}
		remoteCommand := command.Args[len(command.Args)-1]
		check := exec.Command("bash", "-n", "-c", remoteCommand)
		if output, err := check.CombinedOutput(); err != nil {
			t.Fatalf("invalid remote shell command: %v\n%s\ncommand:\n%s", err, output, remoteCommand)
		}
	}
}

// commandsContainingAll 返回命令名和参数中同时包含所有关键字的命令。
func commandsContainingAll(commands []deployctl.Command, values ...string) []deployctl.Command {
	matches := make([]deployctl.Command, 0)
	for _, cmd := range commands {
		line := cmd.Name + " " + strings.Join(cmd.Args, " ")
		matched := true
		for _, value := range values {
			if !strings.Contains(line, value) {
				matched = false
				break
			}
		}
		if matched {
			matches = append(matches, cmd)
		}
	}
	return matches
}

func commandArgsContaining(commands []deployctl.Command, name string, value string) []deployctl.Command {
	var matches []deployctl.Command
	for _, cmd := range commands {
		if cmd.Name != name {
			continue
		}
		if strings.Contains(strings.Join(cmd.Args, " "), value) {
			matches = append(matches, cmd)
		}
	}
	return matches
}

func sshRemoteCommandsContaining(commands []deployctl.Command, values ...string) []deployctl.Command {
	var matches []deployctl.Command
	for _, cmd := range commands {
		if cmd.Name != "ssh" || len(cmd.Args) == 0 {
			continue
		}
		remoteCommand := cmd.Args[len(cmd.Args)-1]
		ok := true
		for _, value := range values {
			if !strings.Contains(remoteCommand, value) {
				ok = false
				break
			}
		}
		if ok {
			matches = append(matches, cmd)
		}
	}
	return matches
}

func commandsContain(commands []deployctl.Command, value string) bool {
	for _, cmd := range commands {
		if strings.Contains(strings.Join(cmd.Args, " "), value) {
			return true
		}
	}
	return false
}

func formatCommands(commands []deployctl.Command) string {
	var lines []string
	for _, cmd := range commands {
		lines = append(lines, cmd.Name+" "+strings.Join(cmd.Args, " "))
	}
	return strings.Join(lines, "\n")
}

// writeTestDeploymentPackage 创建带 manifest 的最小有效发布包。
func writeTestDeploymentPackage(t *testing.T, path string, version string) {
	t.Helper()
	body, err := json.Marshal(deployctl.ArtifactManifest{Version: version, Commit: "test-commit"})
	if err != nil {
		t.Fatal(err)
	}
	var contents bytes.Buffer
	compressed := gzip.NewWriter(&contents)
	archive := tar.NewWriter(compressed)
	if err := archive.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0o600, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if _, err := archive.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, contents.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readTarGzEntries(t *testing.T, path string) map[string]struct{} {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	entries := make(map[string]struct{})
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		entries[header.Name] = struct{}{}
	}
	return entries
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
