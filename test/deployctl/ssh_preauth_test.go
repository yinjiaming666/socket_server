package deployctl_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"socket_server/internal/deployctl"
)

// outputSnapshotRunner 在每次外部命令启动前记录当前 deployctl 输出，便于验证密码预热顺序。
type outputSnapshotRunner struct {
	commands  []deployctl.Command
	snapshots []string
	output    *bytes.Buffer
}

func (r *outputSnapshotRunner) Run(_ context.Context, cmd deployctl.Command, _ io.Reader, _ io.Writer, _ io.Writer) error {
	r.commands = append(r.commands, cmd)
	if r.output == nil {
		r.snapshots = append(r.snapshots, "")
	} else {
		r.snapshots = append(r.snapshots, r.output.String())
	}
	return nil
}

func TestAuthenticateGroupsPrewarmsOneConnectionPerRemote(t *testing.T) {
	targetsFile := &deployctl.TargetsFile{}
	if err := parseTargetsForPreauth(targetsFile); err != nil {
		t.Fatal(err)
	}
	groups := deployctl.GroupTargets(mustSelectAllTargets(t, targetsFile))
	runner := &outputSnapshotRunner{}
	deployer := deployctl.NewDeployer(deployctl.Config{
		RootDir:       t.TempDir(),
		SSHMultiplex:  true,
		SSHControlDir: filepath.Join(t.TempDir(), "ssh"),
		Runner:        runner,
		Stdout:        io.Discard,
		Stderr:        io.Discard,
	})

	if err := deployer.AuthenticateGroups(context.Background(), groups); err != nil {
		t.Fatal(err)
	}
	if len(runner.commands) != 1 {
		t.Fatalf("prewarm commands = %d, want 1: %#v", len(runner.commands), runner.commands)
	}
	command := runner.commands[0]
	if command.Name != "ssh" || len(command.Args) == 0 || command.Args[len(command.Args)-1] != "true" {
		t.Fatalf("unexpected prewarm command: %#v", command)
	}
	if !strings.Contains(strings.Join(command.Args, " "), "ControlMaster=auto") {
		t.Fatalf("prewarm should create an SSH control master: %#v", command)
	}
}

func TestListPrewarmsSSHBeforeWritingTable(t *testing.T) {
	tmp := t.TempDir()
	targetsPath := filepath.Join(tmp, "targets.json")
	appConfigPath := filepath.Join(tmp, "configs", "app.json")
	if err := os.MkdirAll(filepath.Dir(appConfigPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(targetsPath, []byte(testTargetsJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(appConfigPath, []byte(testAppConfigJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	runner := &outputSnapshotRunner{output: &output}
	cfg := deployctl.Config{
		RootDir:           tmp,
		TargetsFile:       targetsPath,
		AppConfig:         appConfigPath,
		SSHMultiplex:      true,
		SSHControlPersist: "10m",
		SSHControlDir:     filepath.Join(tmp, "ssh"),
		Runner:            runner,
		Stdout:            &output,
		Stderr:            io.Discard,
	}
	if err := deployctl.Execute(context.Background(), cfg, []string{"list", "ws-server-1"}); err != nil {
		t.Fatal(err)
	}
	if len(runner.commands) < 2 {
		t.Fatalf("commands = %d, want prewarm and list commands", len(runner.commands))
	}
	if first := runner.commands[0]; first.Name != "ssh" || first.Args[len(first.Args)-1] != "true" {
		t.Fatalf("first command should prewarm SSH, got %#v", first)
	}
	if runner.snapshots[0] != "" {
		t.Fatalf("table output was written before SSH authentication: %q", runner.snapshots[0])
	}
	if !strings.Contains(runner.snapshots[1], "SERVER") {
		t.Fatalf("list table should be written after SSH prewarm, got %q", runner.snapshots[1])
	}
}

func parseTargetsForPreauth(targets *deployctl.TargetsFile) error {
	return json.Unmarshal([]byte(testTargetsJSON), targets)
}

func mustSelectAllTargets(t *testing.T, targets *deployctl.TargetsFile) []deployctl.Target {
	t.Helper()
	selected, err := deployctl.SelectTargets(targets, deployctl.Filter{Server: "all", Service: "all", Endpoint: "all"})
	if err != nil {
		t.Fatal(err)
	}
	return selected
}
