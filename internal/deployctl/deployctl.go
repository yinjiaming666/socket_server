package deployctl

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	defaultDeployDir = "/opt/socket-server"
	defaultSSHUser   = "root"
	defaultSSHPort   = "22"
)

var validServices = map[string]struct{}{
	"gateway":        {},
	"message-api":    {},
	"websocket-node": {},
}

type Config struct {
	RootDir           string
	TargetsFile       string
	AppConfig         string
	RuntimeEnv        string
	PackageFile       string
	BuildDir          string
	SkipBuild         bool
	SSHOpts           []string
	SSHMultiplex      bool
	SSHControlPersist string
	SSHControlDir     string
	Stdin             io.Reader
	Stdout            io.Writer
	Stderr            io.Writer
	Runner            Runner
	Now               func() time.Time
}

type Runner interface {
	Run(ctx context.Context, cmd Command, stdin io.Reader, stdout io.Writer, stderr io.Writer) error
}

type Command struct {
	Name string
	Args []string
	Dir  string
	Env  []string
}

type OSRunner struct{}

func (OSRunner) Run(ctx context.Context, c Command, stdin io.Reader, stdout io.Writer, stderr io.Writer) error {
	cmd := exec.CommandContext(ctx, c.Name, c.Args...)
	cmd.Dir = c.Dir
	if len(c.Env) > 0 {
		cmd.Env = c.Env
	}
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}

type TargetsFile struct {
	Servers []Server `json:"servers"`
}

type Server struct {
	ID        string     `json:"id"`
	Host      string     `json:"host"`
	SSHUser   string     `json:"ssh_user"`
	SSHPort   IntOrStr   `json:"ssh_port"`
	DeployDir string     `json:"deploy_dir"`
	Instances []Instance `json:"instances"`
}

type Instance struct {
	Service    string `json:"service"`
	EndpointID string `json:"endpoint_id"`
}

type IntOrStr string

func (v *IntOrStr) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*v = IntOrStr(strings.TrimSpace(s))
		return nil
	}
	var n int
	if err := json.Unmarshal(data, &n); err == nil {
		*v = IntOrStr(fmt.Sprintf("%d", n))
		return nil
	}
	return fmt.Errorf("ssh_port must be string or number")
}

type Target struct {
	ServerID   string
	Host       string
	SSHUser    string
	SSHPort    string
	DeployDir  string
	Service    string
	EndpointID string
}

type Filter struct {
	Server   string
	Service  string
	Endpoint string
}

type RemoteKey struct {
	Host      string
	SSHUser   string
	SSHPort   string
	DeployDir string
}

type RemoteGroup struct {
	Key       RemoteKey
	Label     string
	ServerIDs []string
	Targets   []Target
}

type Request struct {
	Command   string
	Filter    Filter
	Help      bool
	HelpTopic string
}

type Deployer struct {
	cfg     Config
	masters map[RemoteKey]struct{}
}

func Execute(ctx context.Context, cfg Config, args []string) error {
	if err := cfg.applyDefaults(); err != nil {
		return err
	}

	req, err := ParseArgs(args)
	if err != nil {
		Usage(cfg.Stderr)
		return err
	}
	if req.Help {
		PrintHelp(cfg.Stdout, req.HelpTopic)
		return nil
	}

	targetFile, err := LoadTargetsFile(cfg.TargetsFile)
	if err != nil {
		return err
	}
	targets, err := SelectTargets(targetFile, req.Filter)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return fmt.Errorf("no matching targets: server=%s service=%s endpoint=%s", req.Filter.Server, req.Filter.Service, req.Filter.Endpoint)
	}

	groups := GroupTargets(targets)
	deployer := NewDeployer(cfg)
	deployer.PrintPlan(targets, groups)

	if req.Command == "plan" {
		return nil
	}

	if cfg.SSHMultiplex {
		defer deployer.CloseMasters(context.Background())
	}

	if req.Command == "deploy" {
		if err := deployer.BuildPackage(ctx); err != nil {
			return err
		}
		for _, group := range groups {
			if err := deployer.DeployGroup(ctx, group); err != nil {
				return err
			}
		}
		return nil
	}

	if req.Command == "syncctl" {
		for _, group := range groups {
			if err := deployer.SyncControlScriptGroup(ctx, group); err != nil {
				return err
			}
		}
		return nil
	}

	if req.Command == "list" {
		deployer.PrintListHeader()
		for _, group := range groups {
			if err := deployer.ListGroup(ctx, group); err != nil {
				return err
			}
		}
		return nil
	}

	for _, group := range groups {
		if err := deployer.ManageGroup(ctx, group, req.Command); err != nil {
			return err
		}
	}
	return nil
}

func NewDeployer(cfg Config) *Deployer {
	_ = cfg.applyDefaults()
	return &Deployer{cfg: cfg, masters: make(map[RemoteKey]struct{})}
}

func ParseArgs(args []string) (Request, error) {
	if len(args) == 0 {
		return Request{Help: true}, nil
	}

	command := args[0]
	if command == "all" {
		return Request{
			Command: "deploy",
			Filter:  Filter{Server: "all", Service: "all", Endpoint: "all"},
		}, nil
	}
	if command == "help" {
		return Request{Help: true, HelpTopic: argOrDefault(args, 1, "")}, nil
	}
	if command == "-h" || command == "--help" {
		return Request{Help: true}, nil
	}

	switch command {
	case "plan", "deploy", "syncctl", "list", "start", "pause", "stop", "restart", "status", "logs":
	default:
		return Request{}, fmt.Errorf("unsupported command: %s", command)
	}

	return Request{
		Command: command,
		Filter: Filter{
			Server:   argOrDefault(args, 1, "all"),
			Service:  argOrDefault(args, 2, "all"),
			Endpoint: argOrDefault(args, 3, "all"),
		},
	}, nil
}

func Usage(w io.Writer) {
	PrintHelp(w, "")
}

func PrintHelp(w io.Writer, topic string) {
	if w == nil {
		return
	}
	topic = strings.TrimSpace(topic)
	switch topic {
	case "", "all":
	case "plan":
		fmt.Fprint(w, `Usage:
  go run ./cmd/deployctl plan [server_id|all] [service|all] [endpoint_id|all]

Preview the local deployment selection. It does not connect to remote servers.
`)
		return
	case "deploy":
		fmt.Fprint(w, `Usage:
  go run ./cmd/deployctl deploy [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl all

Build the Linux package, upload once per physical remote, then install every selected instance.
`)
		return
	case "syncctl":
		fmt.Fprint(w, `Usage:
  go run ./cmd/deployctl syncctl [server_id|all] [service|all] [endpoint_id|all]

Update only the remote socketctl management script. It does not build, deploy, restart, or pause services.
`)
		return
	case "list":
		fmt.Fprint(w, `Usage:
  go run ./cmd/deployctl list [server_id|all] [service|all] [endpoint_id|all]

Show selected deployed service records on remote servers.

State notes:
  active          systemd unit is running.
  inactive/failed systemd knows the unit, but it is stopped or failed.
  unit-not-found  runtime/services has a record, but the systemd unit file is missing.
  systemctl-unavailable systemctl did not return a usable state.
`)
		return
	case "start", "pause", "stop", "restart", "status", "logs":
		fmt.Fprintf(w, `Usage:
  go run ./cmd/deployctl %s [server_id|all] [service|all] [endpoint_id|all]

Run remote socketctl for each selected instance only.
`, topic)
		return
	default:
		fmt.Fprintf(w, "unsupported help topic: %s\n\n", topic)
	}
	fmt.Fprint(w, `Usage:
  go run ./cmd/deployctl help [command]
  go run ./cmd/deployctl plan [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl deploy [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl all
  go run ./cmd/deployctl syncctl [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl list [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl start [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl pause [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl stop [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl restart [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl status [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl logs [server_id|all] [service|all] [endpoint_id|all]

Environment:
  TARGETS_FILE=deploy/targets.json
  APP_CONFIG=configs/app.json
  RUNTIME_ENV=deploy/runtime.env
  PACKAGE_FILE=runtime/package/socket-server.tar.gz
  BUILD_DIR=runtime/build
  SKIP_BUILD=1
  SSH_OPTS="-i ~/.ssh/id_rsa"
  SSH_MULTIPLEX=1
  SSH_CONTROL_PERSIST=10m
  SSH_CONTROL_DIR=/tmp/ss-socket

Examples:
  go run ./cmd/deployctl help list
  go run ./cmd/deployctl plan all
  go run ./cmd/deployctl deploy all
  go run ./cmd/deployctl syncctl all
  go run ./cmd/deployctl list ws-server-1

List state notes:
  unit-not-found means the record exists but the systemd unit is missing. Redeploy that instance.
  systemctl-unavailable means systemctl did not return a usable state. Check systemd permissions or host state.
`)
}

func LoadTargetsFile(path string) (*TargetsFile, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read targets file %s: %w", path, err)
	}
	var file TargetsFile
	if err := json.Unmarshal(body, &file); err != nil {
		return nil, fmt.Errorf("parse targets file %s: %w", path, err)
	}
	if len(file.Servers) == 0 {
		return nil, errors.New("targets json field servers must be a non-empty list")
	}
	return &file, nil
}

func SelectTargets(file *TargetsFile, filter Filter) ([]Target, error) {
	filter = normalizeFilter(filter)
	var targets []Target
	for _, server := range file.Servers {
		serverID := strings.TrimSpace(server.ID)
		host := strings.TrimSpace(server.Host)
		user := strings.TrimSpace(server.SSHUser)
		port := strings.TrimSpace(string(server.SSHPort))
		deployDir := strings.TrimSpace(server.DeployDir)

		if serverID == "" {
			return nil, errors.New("server id is required")
		}
		if host == "" {
			return nil, fmt.Errorf("server %s host is required", serverID)
		}
		if user == "" {
			user = defaultSSHUser
		}
		if port == "" {
			port = defaultSSHPort
		}
		if deployDir == "" {
			deployDir = defaultDeployDir
		}
		if filter.Server != "all" && serverID != filter.Server {
			continue
		}
		if len(server.Instances) == 0 {
			return nil, fmt.Errorf("server %s instances must be a non-empty list", serverID)
		}

		for _, instance := range server.Instances {
			service := strings.TrimSpace(instance.Service)
			endpointID := strings.TrimSpace(instance.EndpointID)
			if _, ok := validServices[service]; !ok {
				return nil, fmt.Errorf("server %s has unsupported service: %s", serverID, service)
			}
			if endpointID == "" {
				return nil, fmt.Errorf("server %s %s endpoint_id is required", serverID, service)
			}
			if filter.Service != "all" && service != filter.Service {
				continue
			}
			if filter.Endpoint != "all" && endpointID != filter.Endpoint {
				continue
			}
			targets = append(targets, Target{
				ServerID:   serverID,
				Host:       host,
				SSHUser:    user,
				SSHPort:    port,
				DeployDir:  deployDir,
				Service:    service,
				EndpointID: endpointID,
			})
		}
	}
	return targets, nil
}

func GroupTargets(targets []Target) []RemoteGroup {
	var groups []RemoteGroup
	indexByKey := make(map[RemoteKey]int)
	seenServerIDs := make(map[RemoteKey]map[string]struct{})

	for _, target := range targets {
		key := RemoteKey{
			Host:      target.Host,
			SSHUser:   target.SSHUser,
			SSHPort:   target.SSHPort,
			DeployDir: target.DeployDir,
		}
		index, ok := indexByKey[key]
		if !ok {
			index = len(groups)
			indexByKey[key] = index
			seenServerIDs[key] = make(map[string]struct{})
			groups = append(groups, RemoteGroup{Key: key})
		}
		group := &groups[index]
		group.Targets = append(group.Targets, target)
		if _, ok := seenServerIDs[key][target.ServerID]; !ok {
			seenServerIDs[key][target.ServerID] = struct{}{}
			group.ServerIDs = append(group.ServerIDs, target.ServerID)
			group.Label = strings.Join(group.ServerIDs, ",")
		}
	}

	return groups
}

func (d *Deployer) PrintPlan(targets []Target, groups []RemoteGroup) {
	out := d.cfg.Stdout
	fmt.Fprintln(out)
	fmt.Fprintln(out, "Socket Server Deploy")
	fmt.Fprintf(out, "targets: %s\n", d.cfg.TargetsFile)
	fmt.Fprintln(out, "plan:")
	fmt.Fprintf(out, "%-28s %-16s %-22s %-15s %s\n", "SERVER", "SERVICE", "ENDPOINT", "HOST", "DEPLOY_DIR")
	for _, target := range targets {
		fmt.Fprintf(out, "%-28s %-16s %-22s %-15s %s\n", target.ServerID, target.Service, target.EndpointID, target.Host, target.DeployDir)
	}
	fmt.Fprintln(out, "remotes:")
	for _, group := range groups {
		fmt.Fprintf(out, "- %-40s %s@%s:%s %s instances=%d\n", group.Label, group.Key.SSHUser, group.Key.Host, group.Key.SSHPort, group.Key.DeployDir, len(group.Targets))
	}
}

func (d *Deployer) PrintListHeader() {
	fmt.Fprintf(d.cfg.Stdout, "%-28s %-16s %-24s %-48s %-24s %-24s\n", "SERVER", "SERVICE", "ENDPOINT", "UNIT", "UPDATED_AT", "STATE")
}

func (d *Deployer) BuildPackage(ctx context.Context) error {
	if d.cfg.SkipBuild {
		if _, err := os.Stat(d.cfg.PackageFile); err != nil {
			return fmt.Errorf("package file not found: %s", d.cfg.PackageFile)
		}
		return nil
	}
	if _, err := os.Stat(d.cfg.AppConfig); err != nil {
		return fmt.Errorf("config file not found: %s", d.cfg.AppConfig)
	}

	pkgDir := filepath.Join(d.cfg.BuildDir, "socket-server")
	if err := os.RemoveAll(pkgDir); err != nil {
		return fmt.Errorf("remove build dir: %w", err)
	}
	for _, dir := range []string{
		filepath.Join(pkgDir, "bin"),
		filepath.Join(pkgDir, "configs"),
		filepath.Join(pkgDir, "deploy"),
		filepath.Dir(d.cfg.PackageFile),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create dir %s: %w", dir, err)
		}
	}

	d.info("build linux package")
	for _, service := range []string{"gateway", "message-api", "websocket-node"} {
		output := filepath.Join(pkgDir, "bin", service)
		args := []string{"build", "-o", output, "./cmd/" + service}
		if err := d.run(ctx, Command{Name: "go", Args: args, Dir: d.cfg.RootDir, Env: linuxBuildEnv()}); err != nil {
			return fmt.Errorf("build %s: %w", service, err)
		}
	}

	if err := copyFile(d.cfg.AppConfig, filepath.Join(pkgDir, "configs", "app.json"), 0o644); err != nil {
		return err
	}
	for _, name := range []string{"install-instance.sh", "socketctl.sh"} {
		src := filepath.Join(d.cfg.RootDir, "deploy", name)
		dst := filepath.Join(pkgDir, "deploy", name)
		if err := copyFile(src, dst, 0o755); err != nil {
			return err
		}
	}
	if err := createTarGz(pkgDir, d.cfg.PackageFile); err != nil {
		return err
	}
	d.success("built package: %s", d.cfg.PackageFile)
	return nil
}

func (d *Deployer) DeployGroup(ctx context.Context, group RemoteGroup) error {
	releaseID := d.cfg.Now().UTC().Format("20060102150405")
	remotePkg := fmt.Sprintf("/tmp/socket-server-%s.tar.gz", releaseID)
	sudo := sudoPrefix(group.Key.SSHUser)

	d.info("upload package to %s (%s), instances=%d", group.Label, group.Key.Host, len(group.Targets))
	if err := d.scp(ctx, group.Key, d.cfg.PackageFile, remotePkg); err != nil {
		return fmt.Errorf("upload package to %s: %w", group.Key.Host, err)
	}

	releaseDir := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "releases", releaseID))
	commands := []string{
		fmt.Sprintf("%smkdir -p %s %s", sudo, shellQuote(releaseDir), shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime", "env")))),
		fmt.Sprintf("COPYFILE_DISABLE=1 %star -xzf %s -C %s", sudo, shellQuote(remotePkg), shellQuote(releaseDir)),
		fmt.Sprintf("%sln -sfn %s %s", sudo, shellQuote(releaseDir), shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current")))),
		fmt.Sprintf("%scp %s %s", sudo, shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current", "deploy", "socketctl.sh"))), shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "socketctl")))),
		fmt.Sprintf("%schmod +x %s %s %s", sudo,
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "socketctl"))),
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current", "deploy", "install-instance.sh"))),
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current", "deploy", "socketctl.sh")))),
	}
	for _, remoteCmd := range commands {
		if err := d.ssh(ctx, group.Key, remoteCmd); err != nil {
			return err
		}
	}

	if _, err := os.Stat(d.cfg.RuntimeEnv); err == nil {
		remoteEnv := fmt.Sprintf("/tmp/socket-server-runtime-%s.env", releaseID)
		d.info("upload shared runtime env to %s", group.Label)
		if err := d.scp(ctx, group.Key, d.cfg.RuntimeEnv, remoteEnv); err != nil {
			return fmt.Errorf("upload runtime env to %s: %w", group.Key.Host, err)
		}
		remoteCmd := fmt.Sprintf("%scp %s %s && %schmod 600 %s && rm -f %s",
			sudo,
			shellQuote(remoteEnv),
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime", "socket-server.env"))),
			sudo,
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime", "socket-server.env"))),
			shellQuote(remoteEnv))
		if err := d.ssh(ctx, group.Key, remoteCmd); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check runtime env %s: %w", d.cfg.RuntimeEnv, err)
	} else {
		d.warn("runtime env not found, skip: %s", d.cfg.RuntimeEnv)
	}

	for _, target := range group.Targets {
		d.info("deploy %s %s on %s", target.Service, target.EndpointID, group.Label)
		remoteCmd := fmt.Sprintf("%sbash %s %s %s %s",
			sudo,
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current", "deploy", "install-instance.sh"))),
			shellQuote(group.Key.DeployDir),
			shellQuote(target.Service),
			shellQuote(target.EndpointID))
		if err := d.ssh(ctx, group.Key, remoteCmd); err != nil {
			return err
		}
	}

	if err := d.ssh(ctx, group.Key, "rm -f "+shellQuote(remotePkg)); err != nil {
		return err
	}
	d.success("%s done", group.Label)
	return nil
}

func (d *Deployer) SyncControlScriptGroup(ctx context.Context, group RemoteGroup) error {
	localScript := filepath.Join(d.cfg.RootDir, "deploy", "socketctl.sh")
	if _, err := os.Stat(localScript); err != nil {
		return fmt.Errorf("read local socketctl script %s: %w", localScript, err)
	}

	updateID := d.cfg.Now().UTC().Format("20060102150405")
	remoteScript := fmt.Sprintf("/tmp/socket-server-socketctl-%s.sh", updateID)
	installedScript := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "socketctl"))
	sudo := sudoPrefix(group.Key.SSHUser)

	d.info("update socketctl on %s (%s)", group.Label, group.Key.Host)
	if err := d.scp(ctx, group.Key, localScript, remoteScript); err != nil {
		return fmt.Errorf("upload socketctl to %s: %w", group.Key.Host, err)
	}

	remoteCmd := fmt.Sprintf("%smkdir -p %s && %scp %s %s && %schmod 755 %s && rm -f %s",
		sudo,
		shellQuote(group.Key.DeployDir),
		sudo,
		shellQuote(remoteScript),
		shellQuote(installedScript),
		sudo,
		shellQuote(installedScript),
		shellQuote(remoteScript))
	if err := d.ssh(ctx, group.Key, remoteCmd); err != nil {
		return fmt.Errorf("install socketctl on %s: %w", group.Key.Host, err)
	}

	d.success("socketctl updated on %s", group.Label)
	return nil
}

func (d *Deployer) ListGroup(ctx context.Context, group RemoteGroup) error {
	seen := make(map[string]struct{})
	for _, target := range group.Targets {
		key := target.Service + "\x00" + target.EndpointID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		d.info("list %s/%s on %s (%s)", target.Service, target.EndpointID, group.Label, group.Key.Host)
		if err := d.ssh(ctx, group.Key, remoteListScript(target)); err != nil {
			return err
		}
	}
	return nil
}

func (d *Deployer) ManageGroup(ctx context.Context, group RemoteGroup, command string) error {
	sudo := sudoPrefix(group.Key.SSHUser)
	seen := make(map[string]struct{})
	for _, target := range group.Targets {
		key := target.Service + "\x00" + target.EndpointID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		d.info("%s %s/%s on %s (%s)", command, target.Service, target.EndpointID, group.Label, group.Key.Host)
		remoteCmd := fmt.Sprintf("%sbash %s %s %s %s",
			sudo,
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "socketctl"))),
			shellQuote(command),
			shellQuote(target.Service),
			shellQuote(target.EndpointID))
		if err := d.ssh(ctx, group.Key, remoteCmd); err != nil {
			return err
		}
	}
	return nil
}

func remoteListScript(target Target) string {
	unit := fmt.Sprintf("socket-server-%s-%s.service", target.Service, target.EndpointID)
	script := fmt.Sprintf(`
deploy_dir=%s
server_id=%s
service=%s
endpoint_id=%s
record_file="$deploy_dir/runtime/services/${service}-${endpoint_id}.json"
unit=%s
updated_at="-"

service_state() {
  unit_name="$1"
  if ! command -v systemctl >/dev/null 2>&1; then
    printf "%%s\n" "systemctl-unavailable"
    return
  fi
  if [ "$(id -u)" -eq 0 ]; then
    sudo_cmd=""
  else
    sudo_cmd="${SUDO_CMD:-sudo}"
  fi
  not_found_active_state="$(printf 'un%%s' 'known')"
  load_state="$($sudo_cmd systemctl show "$unit_name" --property=LoadState --value 2>/dev/null || true)"
  active_state="$($sudo_cmd systemctl show "$unit_name" --property=ActiveState --value 2>/dev/null || true)"
  if [ "$load_state" = "not-found" ] || [ "$active_state" = "$not_found_active_state" ]; then
    printf "%%s\n" "unit-not-found"
  elif [ -n "$active_state" ]; then
    printf "%%s\n" "$active_state"
  elif [ -n "$load_state" ]; then
    printf "%%s\n" "$load_state"
  else
    printf "%%s\n" "systemctl-unavailable"
  fi
}
if [ -f "$record_file" ]; then
  record_unit="$(sed -n 's/^[[:space:]]*"unit"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$record_file" | head -n 1)"
  record_updated_at="$(sed -n 's/^[[:space:]]*"updated_at"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$record_file" | head -n 1)"
  [ -z "$record_unit" ] || unit="$record_unit"
  [ -z "$record_updated_at" ] || updated_at="$record_updated_at"
  state="$(service_state "$unit")"
else
  state="record-not-found"
fi
printf "%%-28s %%-16s %%-24s %%-48s %%-24s %%-24s\n" "$server_id" "$service" "$endpoint_id" "$unit" "$updated_at" "$state"
`, shellQuote(target.DeployDir), shellQuote(target.ServerID), shellQuote(target.Service), shellQuote(target.EndpointID), shellQuote(unit))
	return "bash -lc " + shellQuote(script)
}

func (d *Deployer) CloseMasters(ctx context.Context) {
	if !d.cfg.SSHMultiplex {
		return
	}
	keys := make([]RemoteKey, 0, len(d.masters))
	for key := range d.masters {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return remoteKeyString(keys[i]) < remoteKeyString(keys[j])
	})
	for _, key := range keys {
		args := []string{"-p", key.SSHPort}
		args = append(args, d.cfg.SSHOpts...)
		args = append(args, "-o", "ControlPath="+d.sshControlPath(key), "-O", "exit", key.SSHUser+"@"+key.Host)
		_ = d.run(ctx, Command{Name: "ssh", Args: args})
	}
	_ = os.RemoveAll(d.cfg.SSHControlDir)
}

func (d *Deployer) ssh(ctx context.Context, key RemoteKey, remoteCmd string) error {
	args := []string{"-p", key.SSHPort}
	args = append(args, d.cfg.SSHOpts...)
	if d.cfg.SSHMultiplex {
		if err := os.MkdirAll(d.cfg.SSHControlDir, 0o700); err != nil {
			return fmt.Errorf("create ssh control dir: %w", err)
		}
		args = append(args, "-o", "ControlMaster=auto", "-o", "ControlPersist="+d.cfg.SSHControlPersist, "-o", "ControlPath="+d.sshControlPath(key))
	}
	args = append(args, key.SSHUser+"@"+key.Host, remoteCmd)
	err := d.run(ctx, Command{Name: "ssh", Args: args})
	if err == nil && d.cfg.SSHMultiplex {
		d.masters[key] = struct{}{}
	}
	return err
}

func (d *Deployer) scp(ctx context.Context, key RemoteKey, source string, target string) error {
	args := []string{"-P", key.SSHPort}
	args = append(args, d.cfg.SSHOpts...)
	if d.cfg.SSHMultiplex {
		if err := os.MkdirAll(d.cfg.SSHControlDir, 0o700); err != nil {
			return fmt.Errorf("create ssh control dir: %w", err)
		}
		args = append(args, "-o", "ControlMaster=auto", "-o", "ControlPersist="+d.cfg.SSHControlPersist, "-o", "ControlPath="+d.sshControlPath(key))
	}
	args = append(args, source, key.SSHUser+"@"+key.Host+":"+target)
	err := d.run(ctx, Command{Name: "scp", Args: args})
	if err == nil && d.cfg.SSHMultiplex {
		d.masters[key] = struct{}{}
	}
	return err
}

func (d *Deployer) run(ctx context.Context, cmd Command) error {
	return d.cfg.Runner.Run(ctx, cmd, d.cfg.Stdin, d.cfg.Stdout, d.cfg.Stderr)
}

func (d *Deployer) sshControlPath(key RemoteKey) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(remoteKeyString(RemoteKey{Host: key.Host, SSHUser: key.SSHUser, SSHPort: key.SSHPort})))
	return filepath.Join(d.cfg.SSHControlDir, fmt.Sprintf("c%08x", h.Sum32()))
}

func (d *Deployer) info(format string, args ...any) {
	fmt.Fprintf(d.cfg.Stdout, "==> "+format+"\n", args...)
}

func (d *Deployer) success(format string, args ...any) {
	fmt.Fprintf(d.cfg.Stdout, "[ok] "+format+"\n", args...)
}

func (d *Deployer) warn(format string, args ...any) {
	fmt.Fprintf(d.cfg.Stderr, "[warn] "+format+"\n", args...)
}

func (c *Config) applyDefaults() error {
	if c.RootDir == "" {
		root, err := FindRootDir(".")
		if err != nil {
			return err
		}
		c.RootDir = root
	}
	c.RootDir = absPath(c.RootDir, "")
	if c.TargetsFile == "" {
		c.TargetsFile = filepath.Join(c.RootDir, "deploy", "targets.json")
	} else {
		c.TargetsFile = absPath(c.TargetsFile, c.RootDir)
	}
	if c.AppConfig == "" {
		c.AppConfig = filepath.Join(c.RootDir, "configs", "app.json")
	} else {
		c.AppConfig = absPath(c.AppConfig, c.RootDir)
	}
	if c.RuntimeEnv == "" {
		c.RuntimeEnv = filepath.Join(c.RootDir, "deploy", "runtime.env")
	} else {
		c.RuntimeEnv = absPath(c.RuntimeEnv, c.RootDir)
	}
	if c.PackageFile == "" {
		c.PackageFile = filepath.Join(c.RootDir, "runtime", "package", "socket-server.tar.gz")
	} else {
		c.PackageFile = absPath(c.PackageFile, c.RootDir)
	}
	if c.BuildDir == "" {
		c.BuildDir = filepath.Join(c.RootDir, "runtime", "build")
	} else {
		c.BuildDir = absPath(c.BuildDir, c.RootDir)
	}
	if c.SSHControlPersist == "" {
		c.SSHControlPersist = "10m"
	}
	if c.SSHControlDir == "" {
		c.SSHControlDir = filepath.Join(os.TempDir(), fmt.Sprintf("ss-%d-%d", os.Getuid(), os.Getpid()))
	} else {
		c.SSHControlDir = absPath(c.SSHControlDir, c.RootDir)
	}
	if c.Stdin == nil {
		c.Stdin = os.Stdin
	}
	if c.Stdout == nil {
		c.Stdout = os.Stdout
	}
	if c.Stderr == nil {
		c.Stderr = os.Stderr
	}
	if c.Runner == nil {
		c.Runner = OSRunner{}
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return nil
}

func ConfigFromEnv(rootDir string) Config {
	return Config{
		RootDir:           rootDir,
		TargetsFile:       os.Getenv("TARGETS_FILE"),
		AppConfig:         os.Getenv("APP_CONFIG"),
		RuntimeEnv:        os.Getenv("RUNTIME_ENV"),
		PackageFile:       os.Getenv("PACKAGE_FILE"),
		BuildDir:          os.Getenv("BUILD_DIR"),
		SkipBuild:         os.Getenv("SKIP_BUILD") == "1",
		SSHOpts:           strings.Fields(os.Getenv("SSH_OPTS")),
		SSHMultiplex:      os.Getenv("SSH_MULTIPLEX") != "0",
		SSHControlPersist: os.Getenv("SSH_CONTROL_PERSIST"),
		SSHControlDir:     os.Getenv("SSH_CONTROL_DIR"),
	}
}

func FindRootDir(start string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", err
	}
	for {
		goMod := filepath.Join(dir, "go.mod")
		if body, err := os.ReadFile(goMod); err == nil && strings.Contains(string(body), "module socket_server") {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("cannot find socket_server go.mod")
		}
		dir = parent
	}
}

func createTarGz(sourceDir string, targetFile string) error {
	out, err := os.Create(targetFile)
	if err != nil {
		return fmt.Errorf("create package %s: %w", targetFile, err)
	}
	defer out.Close()

	gz := gzip.NewWriter(out)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	return filepath.WalkDir(sourceDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == sourceDir {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(rel)
		if entry.Type()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			header.Linkname = link
		}
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(tw, in)
		closeErr := in.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func copyFile(src string, dst string, mode fs.FileMode) error {
	body, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	if err := os.WriteFile(dst, body, mode); err != nil {
		return fmt.Errorf("write %s: %w", dst, err)
	}
	return nil
}

func linuxBuildEnv() []string {
	env := os.Environ()
	env = setEnv(env, "GOOS", envOrDefault("GOOS", "linux"))
	env = setEnv(env, "GOARCH", envOrDefault("GOARCH", "amd64"))
	env = setEnv(env, "CGO_ENABLED", envOrDefault("CGO_ENABLED", "0"))
	return env
}

func setEnv(env []string, key string, value string) []string {
	prefix := key + "="
	for i, item := range env {
		if strings.HasPrefix(item, prefix) {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}

func envOrDefault(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func normalizeFilter(filter Filter) Filter {
	filter.Server = defaultSelector(filter.Server)
	filter.Service = defaultSelector(filter.Service)
	filter.Endpoint = defaultSelector(filter.Endpoint)
	return filter
}

func defaultSelector(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "all"
	}
	return value
}

func argOrDefault(args []string, index int, fallback string) string {
	if len(args) <= index || strings.TrimSpace(args[index]) == "" {
		return fallback
	}
	return args[index]
}

func sudoPrefix(user string) string {
	if user == "root" {
		return ""
	}
	return "sudo "
}

func shellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func remoteKeyString(key RemoteKey) string {
	return key.SSHUser + "|" + key.Host + "|" + key.SSHPort + "|" + key.DeployDir
}

func absPath(path string, base string) string {
	if path == "" {
		return path
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	if base == "" {
		abs, err := filepath.Abs(path)
		if err == nil {
			return abs
		}
		return filepath.Clean(path)
	}
	return filepath.Clean(filepath.Join(base, path))
}
