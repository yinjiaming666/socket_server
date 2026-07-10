package deployctl

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
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

type fakeRunner struct {
	commands []Command
}

func (r *fakeRunner) Run(ctx context.Context, cmd Command, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	r.commands = append(r.commands, cmd)
	return nil
}

type fakeBuildRunner struct {
	commands []Command
}

func (r *fakeBuildRunner) Run(ctx context.Context, cmd Command, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
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
	targetsFile := &TargetsFile{}
	if err := json.Unmarshal([]byte(testTargetsJSON), targetsFile); err != nil {
		t.Fatal(err)
	}

	targets, err := SelectTargets(targetsFile, Filter{Server: "all", Service: "all", Endpoint: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 5 {
		t.Fatalf("selected targets = %d, want 5", len(targets))
	}

	groups := GroupTargets(targets)
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
	packagePath := filepath.Join(tmp, "socket-server.tar.gz")
	controlDir := filepath.Join(tmp, "ssh")
	if err := os.WriteFile(targetsPath, []byte(testTargetsJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(packagePath, []byte("package"), 0o644); err != nil {
		t.Fatal(err)
	}

	runner := &fakeRunner{}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cfg := Config{
		RootDir:           tmp,
		TargetsFile:       targetsPath,
		PackageFile:       packagePath,
		RuntimeEnv:        filepath.Join(tmp, "missing.env"),
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

	if err := Execute(context.Background(), cfg, []string{"deploy", "all"}); err != nil {
		t.Fatal(err)
	}

	installCommands := sshRemoteCommandsContaining(runner.commands, "bash", "install-instance.sh")
	if len(installCommands) != 5 {
		t.Fatalf("install-instance ssh commands = %d, want 5\ncommands:\n%s", len(installCommands), formatCommands(runner.commands))
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

	packageUploads := commandArgsContaining(runner.commands, "scp", packagePath)
	if len(packageUploads) != 1 {
		t.Fatalf("package uploads = %d, want 1\ncommands:\n%s", len(packageUploads), formatCommands(runner.commands))
	}

	if !strings.Contains(stdout.String(), "instances=5") {
		t.Fatalf("plan output should show instances=5, got:\n%s", stdout.String())
	}
}

func TestSelectWebsocketAllReturnsThreeNodes(t *testing.T) {
	targetsFile := &TargetsFile{}
	if err := json.Unmarshal([]byte(testTargetsJSON), targetsFile); err != nil {
		t.Fatal(err)
	}

	targets, err := SelectTargets(targetsFile, Filter{Server: "all", Service: "websocket-node", Endpoint: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 3 {
		t.Fatalf("websocket targets = %d, want 3", len(targets))
	}
}

func TestSyncCtlUpdatesRemoteScriptWithoutDeployingServices(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	localScript := filepath.Join(tmp, "deploy", "socketctl.sh")
	if err := os.WriteFile(targetsPath, []byte(testTargetsJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(localScript), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(localScript, []byte("#!/usr/bin/env bash\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	runner := &fakeRunner{}
	var stdout bytes.Buffer
	cfg := Config{
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

	if err := Execute(context.Background(), cfg, []string{"syncctl", "all"}); err != nil {
		t.Fatal(err)
	}

	scriptUploads := commandArgsContaining(runner.commands, "scp", localScript)
	if len(scriptUploads) != 1 {
		t.Fatalf("socketctl uploads = %d, want 1\ncommands:\n%s", len(scriptUploads), formatCommands(runner.commands))
	}
	installCommands := sshRemoteCommandsContaining(runner.commands, "install-instance.sh")
	if len(installCommands) != 0 {
		t.Fatalf("syncctl must not install services\ncommands:\n%s", formatCommands(installCommands))
	}
	if commandsContain(runner.commands, "tar -xzf") || commandsContain(runner.commands, "systemctl") {
		t.Fatalf("syncctl must not build, deploy, or manage services\ncommands:\n%s", formatCommands(runner.commands))
	}
	if !commandsContain(runner.commands, "chmod 755") || !strings.Contains(stdout.String(), "socketctl updated") {
		t.Fatalf("syncctl should install the script and report success\ncommands:\n%s\noutput:\n%s", formatCommands(runner.commands), stdout.String())
	}
}

func TestListUsesSelectedInstancesOnly(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	controlDir := filepath.Join(tmp, "ssh")
	if err := os.WriteFile(targetsPath, []byte(testTargetsJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	runner := &fakeRunner{}
	var stdout bytes.Buffer
	cfg := Config{
		RootDir:           tmp,
		TargetsFile:       targetsPath,
		SSHMultiplex:      true,
		SSHControlPersist: "10m",
		SSHControlDir:     controlDir,
		Runner:            runner,
		Stdout:            &stdout,
		Stderr:            io.Discard,
	}
	if err := Execute(context.Background(), cfg, []string{"list", "ws-server-1"}); err != nil {
		t.Fatal(err)
	}

	listCommands := sshRemoteCommandsContaining(runner.commands, "service_state")
	if len(listCommands) != 2 {
		t.Fatalf("inline list commands = %d, want 2\ncommands:\n%s", len(listCommands), formatCommands(runner.commands))
	}
	for _, want := range []string{"ws-node-1", "ws-node-2"} {
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
	if !strings.Contains(stdout.String(), "STATE") {
		t.Fatalf("list should print a local header, got:\n%s", stdout.String())
	}
}

func TestRemoteListScriptDoesNotEmitLegacyFallbackState(t *testing.T) {
	script := remoteListScript(Target{
		ServerID:   "ws-server-1",
		DeployDir:  "/opt/socket-server",
		Service:    "websocket-node",
		EndpointID: "ws-node-1",
	})
	legacyState := "un" + "known"
	if strings.Contains(script, legacyState) {
		t.Fatalf("remote list script should not contain legacy fallback state:\n%s", script)
	}
	if strings.Contains(script, "socketctl") {
		t.Fatalf("remote list script should not call socketctl:\n%s", script)
	}
	if !strings.Contains(script, "systemctl-unavailable") {
		t.Fatalf("remote list script should expose systemctl-unavailable:\n%s", script)
	}
	if strings.Index(script, "service_state()") > strings.Index(script, `state="$(service_state "$unit")"`) {
		t.Fatalf("remote list script should define service_state before calling it:\n%s", script)
	}
}

func TestHelpListExplainsUnavailableStates(t *testing.T) {
	runner := &fakeRunner{}
	var stdout bytes.Buffer
	cfg := Config{
		RootDir: "/tmp",
		Runner:  runner,
		Stdout:  &stdout,
		Stderr:  io.Discard,
	}
	if err := Execute(context.Background(), cfg, []string{"help", "list"}); err != nil {
		t.Fatal(err)
	}
	output := stdout.String()
	for _, want := range []string{"unit-not-found", "systemctl-unavailable", "go run ./cmd/deployctl list"} {
		if !strings.Contains(output, want) {
			t.Fatalf("help output missing %q:\n%s", want, output)
		}
	}
	legacyState := "un" + "known"
	if strings.Contains(output, legacyState) {
		t.Fatalf("help output should not contain legacy fallback state:\n%s", output)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("help should not run commands, got:\n%s", formatCommands(runner.commands))
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
	cfg := Config{
		RootDir:     tmp,
		AppConfig:   appConfig,
		PackageFile: packagePath,
		BuildDir:    buildDir,
		Runner:      runner,
		Stdout:      &stdout,
		Stderr:      io.Discard,
	}
	deployer := NewDeployer(cfg)
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
	} {
		if _, ok := entries[want]; !ok {
			t.Fatalf("package missing %s; entries=%v", want, sortedKeys(entries))
		}
	}
}

func commandArgsContaining(commands []Command, name string, value string) []Command {
	var matches []Command
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

func sshRemoteCommandsContaining(commands []Command, values ...string) []Command {
	var matches []Command
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

func commandsContain(commands []Command, value string) bool {
	for _, cmd := range commands {
		if strings.Contains(strings.Join(cmd.Args, " "), value) {
			return true
		}
	}
	return false
}

func formatCommands(commands []Command) string {
	var lines []string
	for _, cmd := range commands {
		lines = append(lines, cmd.Name+" "+strings.Join(cmd.Args, " "))
	}
	return strings.Join(lines, "\n")
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
