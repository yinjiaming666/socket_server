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
	RootDir              string
	TargetsFile          string
	AppConfig            string
	RuntimeEnv           string
	PackageFile          string
	BuildDir             string
	ReleaseVersion       string // ReleaseVersion 指定跨服务器一致的不可变发布编号。
	GitCommit            string // GitCommit 记录构建对应的源码提交。
	HealthTimeoutSeconds int    // HealthTimeoutSeconds 限制单实例启动健康检查时间。
	DrainTimeoutSeconds  int    // DrainTimeoutSeconds 是 WebSocket 节点停止接收新连接后的等待时间。
	RetainReleases       int    // RetainReleases 控制每台服务器保留的历史发布数量。
	SkipBuild            bool
	SSHOpts              []string
	SSHMultiplex         bool
	SSHControlPersist    string
	SSHControlDir        string
	Stdin                io.Reader
	Stdout               io.Writer
	Stderr               io.Writer
	Runner               Runner
	Now                  func() time.Time
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
	HTTPPort   int    // HTTPPort 从应用配置解析，用于健康检查和列表展示。
	GRPCPort   int    // GRPCPort 仅 WebSocket Node 使用，用于列表展示。
	ListenHost string // ListenHost 是服务 HTTP 监听地址中的 IP/主机名，用于列表展示。
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
	cfg       Config
	releaseID string // releaseID 在一次 deployctl 执行中保持不变，保证所有服务器使用同一制品版本。
	masters   map[RemoteKey]struct{}
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
	if req.Command == "rollback" {
		if err := validateCompleteRollbackSelection(targetFile, targets); err != nil {
			return err
		}
	}
	// list/status 也读取应用配置，统一展示服务端口和监听 IP。
	if req.Command == "deploy" || req.Command == "rollback" || req.Command == "list" {
		if err := hydrateTargetHTTPPorts(targets, cfg.AppConfig); err != nil {
			return err
		}
	}

	groups := GroupTargets(targets)
	deployer := NewDeployer(cfg)

	// plan 仅展示本地目标，保持无网络连接和无密码提示的语义。
	if req.Command == "plan" {
		deployer.PrintPlan(targets, groups)
		return nil
	}

	if cfg.SSHMultiplex {
		defer func() {
			// 关闭复用连接不应无限阻塞发布命令退出。
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			deployer.CloseMasters(closeCtx)
		}()
	}

	// 所有远程命令先建立 SSH 连接。密码或主机指纹提示会在任何业务输出前完成，
	// 后续 ssh/scp 复用同一控制连接，不会把提示符插入表格、日志或远端命令输出中。
	if err := deployer.AuthenticateGroups(ctx, groups); err != nil {
		return err
	}

	if req.Command == "list" {
		deployer.PrintListHeader()
	} else {
		deployer.PrintPlan(targets, groups)
	}

	if req.Command == "deploy" {
		if err := deployer.BuildPackage(ctx); err != nil {
			return err
		}
		deployedGroups := make([]RemoteGroup, 0, len(groups))
		for _, group := range groups {
			if err := deployer.DeployGroup(ctx, group); err != nil {
				// 后续主机失败时逆序回滚已完成主机，避免集群长期运行混合版本。
				rollbackTimeout := deploymentRollbackTimeout(cfg, deployedGroups)
				rollbackCtx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
				rollbackErr := deployer.RollbackGroups(rollbackCtx, deployedGroups)
				cancel()
				if rollbackErr != nil {
					return errors.Join(err, fmt.Errorf("rollback completed deployment groups: %w", rollbackErr))
				}
				return err
			}
			deployedGroups = append(deployedGroups, group)
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
		for _, group := range groups {
			if err := deployer.ListGroup(ctx, group); err != nil {
				return err
			}
		}
		return nil
	}

	if req.Command == "history" {
		for _, group := range groups {
			if err := deployer.HistoryGroup(ctx, group); err != nil {
				return err
			}
		}
		return nil
	}

	if req.Command == "rollback" {
		rolledBackGroups := make([]RemoteGroup, 0, len(groups))
		for _, group := range groups {
			if err := deployer.RollbackGroup(ctx, group); err != nil {
				// 后续主机回滚失败时，将已完成主机再次切换回操作前版本。
				restoreTimeout := deploymentRollbackTimeout(cfg, rolledBackGroups)
				restoreCtx, cancel := context.WithTimeout(context.Background(), restoreTimeout)
				restoreErr := deployer.RollbackGroups(restoreCtx, rolledBackGroups)
				cancel()
				if restoreErr != nil {
					return errors.Join(err, fmt.Errorf("restore completed rollback groups: %w", restoreErr))
				}
				return err
			}
			rolledBackGroups = append(rolledBackGroups, group)
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
	return &Deployer{
		cfg:       cfg,
		releaseID: releaseIDFromConfig(cfg),
		masters:   make(map[RemoteKey]struct{}),
	}
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

	// status 已合并到 list；保留别名，避免现有运维脚本立即失效。
	if command == "status" {
		command = "list"
	}

	switch command {
	case "plan", "deploy", "rollback", "history", "syncctl", "list", "start", "pause", "stop", "restart", "logs":
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
		fmt.Fprint(w, `用法：
  go run ./cmd/deployctl plan [server_id|all] [service|all] [endpoint_id|all]

说明：仅预览本地部署目标和远程分组，不连接服务器、不执行部署。
`)
		return
	case "deploy":
		fmt.Fprint(w, `用法：
  go run ./cmd/deployctl deploy [server_id|all] [service|all] [endpoint_id|all]
  go run ./cmd/deployctl all

说明：构建同一份不可变 Linux 制品，校验 SHA256 后按实例滚动发布。
若后续物理主机发布失败，已完成发布的主机会按相反顺序自动回滚。
`)
		return
	case "rollback":
		fmt.Fprint(w, `用法：
  go run ./cmd/deployctl rollback [server_id|all]

说明：将每台选中物理主机从 current 切换到 previous，并依次排空、重启和校验全部实例。
为保证版本一致性，回滚必须覆盖该物理主机上的全部实例。
`)
		return
	case "history":
		fmt.Fprint(w, `用法：
  go run ./cmd/deployctl history [server_id|all]

说明：显示每台选中物理主机最近 50 条发布和回滚审计记录。
`)
		return
	case "syncctl":
		fmt.Fprint(w, `用法：
  go run ./cmd/deployctl syncctl [server_id|all] [service|all] [endpoint_id|all]

说明：仅更新远端 socketctl 和 install-instance 稳定脚本；不会构建、部署、重启或暂停服务。
`)
		return
	case "list":
		fmt.Fprint(w, `用法：
  go run ./cmd/deployctl list [server_id|all] [service|all] [endpoint_id|all]

说明：以统一表格查看选中实例的部署主机、服务端口、监听 IP、Docker 风格状态和最近更新时间。
WebSocket Node 的 PORT 列按 HTTP/gRPC 格式显示；旧的 status 命令已合并到此命令。

状态说明：
  running     正在运行（systemd active）。
  starting    正在启动（systemd activating）。
  restarting  正在自动重启或重载。
  stopping    正在停止（systemd deactivating）。
  stopped     已停止（systemd inactive）。
  failed      启动或运行失败。
  not-found   存在部署记录，但 systemd unit 文件不存在；通常重新部署即可修复。
  not-deployed 未找到该实例的部署记录。
  unknown     无法从 systemd 取得可识别状态，请检查服务器 systemd 状态或权限。
`)
		return
	case "status":
		fmt.Fprint(w, `status 已合并到 list，当前仍作为兼容别名保留。

请改用：
  go run ./cmd/deployctl list [server_id|all] [service|all] [endpoint_id|all]
`)
		return
	case "start", "pause", "stop", "restart", "logs":
		fmt.Fprintf(w, `用法：
  go run ./cmd/deployctl %s [server_id|all] [service|all] [endpoint_id|all]

说明：仅对选中的实例执行远端 socketctl %s 操作。
`, topic, topic)
		return
	default:
		fmt.Fprintf(w, "不支持的帮助主题：%s\n\n", topic)
	}
	fmt.Fprint(w, `用法：
  go run ./cmd/deployctl help [command]                                           查看指定命令的详细说明。
  go run ./cmd/deployctl plan [server_id|all] [service|all] [endpoint_id|all]    仅预览部署目标，不连接服务器。
  go run ./cmd/deployctl deploy [server_id|all] [service|all] [endpoint_id|all]  构建制品并滚动发布选中实例。
  go run ./cmd/deployctl all                                                      等同于 deploy all，发布全部实例。
  go run ./cmd/deployctl rollback [server_id|all]                                将选中物理主机回滚到上一版本。
  go run ./cmd/deployctl history [server_id|all]                                 查看发布和回滚审计记录。
  go run ./cmd/deployctl syncctl [server_id|all] [service|all] [endpoint_id|all] 同步远端 socketctl 和安装脚本。
  go run ./cmd/deployctl list [server_id|all] [service|all] [endpoint_id|all]    查看端口、监听 IP 和服务状态。
  go run ./cmd/deployctl start [server_id|all] [service|all] [endpoint_id|all]   启动选中服务。
  go run ./cmd/deployctl pause [server_id|all] [service|all] [endpoint_id|all]   暂停服务（等同于 stop）。
  go run ./cmd/deployctl stop [server_id|all] [service|all] [endpoint_id|all]    停止选中服务。
  go run ./cmd/deployctl restart [server_id|all] [service|all] [endpoint_id|all] 重启选中服务。
  go run ./cmd/deployctl logs [server_id|all] [service|all] [endpoint_id|all]    查看最近服务日志。

参数说明：
  server_id / service / endpoint_id  分别按服务器、服务类型、实例编号筛选。
  all                                选择该层级的全部目标；省略筛选参数时默认使用 all。
  status                             已合并到 list，当前仅保留为兼容别名。

环境变量：
  TARGETS_FILE=deploy/targets.json
  APP_CONFIG=configs/app.json
  RUNTIME_ENV=deploy/runtime.env
  PACKAGE_FILE=runtime/package/socket-server.tar.gz
  BUILD_DIR=runtime/build
  RELEASE_VERSION=v2026.07.11
  GIT_COMMIT=<git_commit_sha>
  DEPLOY_HEALTH_TIMEOUT_SECONDS=30
  DEPLOY_DRAIN_TIMEOUT_SECONDS=30
  DEPLOY_RETAIN_RELEASES=5
  SKIP_BUILD=1
  SSH_OPTS="-i ~/.ssh/id_rsa"
  SSH_MULTIPLEX=1
  SSH_CONTROL_PERSIST=10m
  SSH_CONTROL_DIR=/tmp/ss-socket

示例：
  go run ./cmd/deployctl help list
  go run ./cmd/deployctl plan all
  RELEASE_VERSION=v2026.07.11 GIT_COMMIT=abc123 go run ./cmd/deployctl deploy all
  go run ./cmd/deployctl history all
  go run ./cmd/deployctl rollback ws-server-1
  go run ./cmd/deployctl syncctl all
  go run ./cmd/deployctl list ws-server-1

状态说明：
  running / starting / restarting / stopping / stopped / failed
  not-found     有部署记录但 systemd unit 不存在，通常重新部署对应实例即可修复。
  not-deployed  未找到该实例的部署记录。
  unknown       无法从 systemd 取得可识别状态。
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
	// 同一物理部署目录内的 service + endpoint_id 必须唯一。
	seenInstances := make(map[RemoteKey]map[string]string)
	for _, server := range file.Servers {
		serverID := strings.TrimSpace(server.ID)
		host := strings.TrimSpace(server.Host)
		user := strings.TrimSpace(server.SSHUser)
		port := strings.TrimSpace(string(server.SSHPort))
		deployDir := strings.TrimSpace(server.DeployDir)

		if !validDeploymentIdentifier(serverID) {
			return nil, fmt.Errorf("server id must use 1-128 letters, digits, dot, dash, or underscore: %q", serverID)
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
		if !validSSHPort(port) {
			return nil, fmt.Errorf("server %s ssh_port must be between 1 and 65535: %q", serverID, port)
		}
		if deployDir == "" {
			deployDir = defaultDeployDir
		}
		if !filepath.IsAbs(deployDir) {
			return nil, fmt.Errorf("server %s deploy_dir must be absolute: %s", serverID, deployDir)
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
			if !validDeploymentIdentifier(endpointID) {
				return nil, fmt.Errorf("server %s %s endpoint_id must use 1-128 letters, digits, dot, dash, or underscore: %q", serverID, service, endpointID)
			}
			if filter.Service != "all" && service != filter.Service {
				continue
			}
			if filter.Endpoint != "all" && endpointID != filter.Endpoint {
				continue
			}
			remoteKey := RemoteKey{Host: host, SSHUser: user, SSHPort: port, DeployDir: deployDir}
			if seenInstances[remoteKey] == nil {
				seenInstances[remoteKey] = make(map[string]string)
			}
			instanceKey := service + "\x00" + endpointID
			if previousServer, exists := seenInstances[remoteKey][instanceKey]; exists {
				return nil, fmt.Errorf("duplicate instance %s/%s on physical host %s in servers %s and %s", service, endpointID, host, previousServer, serverID)
			}
			seenInstances[remoteKey][instanceKey] = serverID
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
	out := d.cfg.Stdout
	fmt.Fprintln(out)
	fmt.Fprintln(out, "Socket Server Deploy")
	fmt.Fprintf(out, "targets: %s\n", d.cfg.TargetsFile)
	fmt.Fprintln(out, "services:")
	fmt.Fprintf(
		out,
		"%-28s %-16s %-22s %-15s %-25s %-12s %-18s %-24s %s\n",
		"SERVER",
		"SERVICE",
		"ENDPOINT",
		"HOST",
		"DEPLOY_DIR",
		"PORT",
		"IP",
		"STATUS",
		"UPDATED_AT",
	)
}

func targetDisplayPort(target Target) string {
	if target.HTTPPort <= 0 {
		return "-"
	}
	if target.GRPCPort > 0 {
		return fmt.Sprintf("%d/%d", target.HTTPPort, target.GRPCPort)
	}
	return fmt.Sprintf("%d", target.HTTPPort)
}

func targetDisplayIP(target Target) string {
	if host := strings.TrimSpace(target.ListenHost); host != "" {
		return host
	}
	return "-"
}

func (d *Deployer) BuildPackage(ctx context.Context) error {
	if d.cfg.SkipBuild {
		if _, err := os.Stat(d.cfg.PackageFile); err != nil {
			return fmt.Errorf("package file not found: %s", d.cfg.PackageFile)
		}
		// 跳过构建时仍校验摘要，并优先复用归档内记录的不可变版本。
		if err := ensurePackageChecksum(d.cfg.PackageFile); err != nil {
			return err
		}
		manifest, err := readArtifactManifest(d.cfg.PackageFile)
		if errors.Is(err, errArtifactManifestNotFound) {
			d.warn("legacy package has no manifest, use generated release id: %s", d.releaseID)
			return nil
		}
		if err != nil {
			return err
		}
		manifestVersion := sanitizeReleaseID(manifest.Version)
		if manifestVersion == "" || manifestVersion != strings.TrimSpace(manifest.Version) {
			return fmt.Errorf("artifact manifest version is invalid: %q", manifest.Version)
		}
		if requestedVersion := sanitizeReleaseID(d.cfg.ReleaseVersion); requestedVersion != "" && requestedVersion != manifestVersion {
			return fmt.Errorf("artifact release mismatch: requested %s manifest %s", requestedVersion, manifestVersion)
		}
		if requestedCommit := strings.TrimSpace(d.cfg.GitCommit); requestedCommit != "" && requestedCommit != strings.TrimSpace(manifest.Commit) {
			return fmt.Errorf("artifact commit mismatch: requested %s manifest %s", requestedCommit, manifest.Commit)
		}
		d.releaseID = manifestVersion
		d.info("reuse package release=%s", d.releaseID)
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

	if err := copyFile(d.cfg.AppConfig, filepath.Join(pkgDir, "configs", "app.json"), 0o600); err != nil {
		return err
	}
	for _, name := range []string{"install-instance.sh", "socketctl.sh"} {
		src := filepath.Join(d.cfg.RootDir, "deploy", name)
		dst := filepath.Join(pkgDir, "deploy", name)
		if err := copyFile(src, dst, 0o755); err != nil {
			return err
		}
	}
	if err := d.writeArtifactManifest(pkgDir); err != nil {
		return err
	}
	if err := createTarGz(pkgDir, d.cfg.PackageFile); err != nil {
		return err
	}
	if err := writePackageChecksum(d.cfg.PackageFile); err != nil {
		return err
	}
	d.success("built package: %s release=%s", d.cfg.PackageFile, d.releaseID)
	return nil
}

func (d *Deployer) DeployGroup(ctx context.Context, group RemoteGroup) (returnErr error) {
	releaseID := d.releaseID
	remotePkg := fmt.Sprintf("/tmp/socket-server-%s.tar.gz", releaseID)
	remoteChecksum := remotePkg + ".sha256"
	remoteRuntimeEnv := fmt.Sprintf("/tmp/socket-server-runtime-%s.env", releaseID)
	sudo := sudoPrefix(group.Key.SSHUser)

	// 必须先加锁再上传，避免并发发布任务覆盖相同的远端临时文件。
	lockOwner := "deploy-" + releaseID
	if err := d.acquireDeploymentLock(ctx, group, lockOwner); err != nil {
		return err
	}
	deploySucceeded := false
	defer func() {
		status := "failed"
		if deploySucceeded {
			status = "success"
		}
		// 失败路径同样清除包含运行配置的临时归档，且各收尾动作使用独立超时。
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		cleanupCommand := runRemoteAsRoot(group.Key.SSHUser, "rm -f "+shellQuote(remotePkg)+" "+shellQuote(remoteChecksum)+" "+shellQuote(remoteRuntimeEnv))
		if err := d.ssh(cleanupCtx, group.Key, cleanupCommand); err != nil {
			d.warn("cleanup remote artifacts failed on %s: %v", group.Label, err)
		}
		cleanupCancel()
		historyCtx, historyCancel := context.WithTimeout(context.Background(), 5*time.Second)
		d.recordDeploymentHistory(historyCtx, group, "deploy", releaseID, status)
		historyCancel()
		lockCtx, lockCancel := context.WithTimeout(context.Background(), 5*time.Second)
		d.releaseDeploymentLock(lockCtx, group, lockOwner)
		lockCancel()
	}()

	currentActivated := false
	rollbackTargets := make([]Target, 0, len(group.Targets))
	defer func() {
		if returnErr == nil || !currentActivated {
			return
		}
		// current 切换后的任意失败都在持有部署锁期间恢复 previous。
		rollbackErr := d.rollbackTargetsAfterFailure(context.Background(), group, rollbackTargets)
		returnErr = joinDeploymentRollbackError(returnErr, rollbackErr)
	}()

	d.info("upload package to %s (%s), instances=%d", group.Label, group.Key.Host, len(group.Targets))
	if err := d.scp(ctx, group.Key, d.cfg.PackageFile, remotePkg); err != nil {
		return fmt.Errorf("upload package to %s: %w", group.Key.Host, err)
	}
	if err := d.scp(ctx, group.Key, d.cfg.PackageFile+".sha256", remoteChecksum); err != nil {
		return fmt.Errorf("upload package checksum to %s: %w", group.Key.Host, err)
	}

	releaseDir := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "releases", releaseID))
	releaseChecksum := filepath.ToSlash(filepath.Join(releaseDir, ".artifact.sha256"))
	currentLink := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current"))
	previousLink := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "previous"))
	commands := []string{
		// 收紧临时制品权限并再次计算 SHA256，在切换 current 前拦截传输损坏或替换。
		runRemoteAsRoot(group.Key.SSHUser, fmt.Sprintf("set -eu; chmod 600 %s %s; expected=$(awk 'NR==1 {print $1}' %s); actual=$(sha256sum %s | awk '{print $1}'); test -n \"$expected\" && test \"$expected\" = \"$actual\"",
			shellQuote(remotePkg), shellQuote(remoteChecksum), shellQuote(remoteChecksum), shellQuote(remotePkg))),
		// 已完成的同版本目录保持不可变，并拒绝同一版本号对应不同制品。
		runRemoteAsRoot(group.Key.SSHUser, fmt.Sprintf("set -eu; mkdir -p %s %s; expected=$(awk 'NR==1 {print $1}' %s); test -n \"$expected\"; if [ -f %s ]; then test -f %s; stored=$(cat %s); test \"$stored\" = \"$expected\"; else rm -rf %s; mkdir -p %s; COPYFILE_DISABLE=1 tar -xzf %s -C %s; printf '%%s\\n' \"$expected\" > %s; touch %s; fi",
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime", "env"))),
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "releases"))),
			shellQuote(remoteChecksum),
			shellQuote(filepath.ToSlash(filepath.Join(releaseDir, ".complete"))),
			shellQuote(releaseChecksum),
			shellQuote(releaseChecksum),
			shellQuote(releaseDir),
			shellQuote(releaseDir),
			shellQuote(remotePkg),
			shellQuote(releaseDir),
			shellQuote(releaseChecksum),
			shellQuote(filepath.ToSlash(filepath.Join(releaseDir, ".complete"))))),
		// 切换新版本前保存旧 current，首次部署没有 current 时跳过。
		runRemoteAsRoot(group.Key.SSHUser, fmt.Sprintf("if [ -L %s ]; then old_target=$(readlink -f %s); test -d \"$old_target\" && ln -sfn \"$old_target\" %s; fi",
			shellQuote(currentLink), shellQuote(currentLink), shellQuote(previousLink))),
		fmt.Sprintf("%sln -sfn %s %s", sudo, shellQuote(releaseDir), shellQuote(currentLink)),
		fmt.Sprintf("%scp %s %s && %scp %s %s",
			sudo,
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current", "deploy", "socketctl.sh"))),
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "socketctl"))),
			sudo,
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current", "deploy", "install-instance.sh"))),
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "install-instance")))),
		fmt.Sprintf("%schmod +x %s %s %s %s", sudo,
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "socketctl"))),
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "install-instance"))),
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current", "deploy", "install-instance.sh"))),
			shellQuote(filepath.ToSlash(filepath.Join(group.Key.DeployDir, "current", "deploy", "socketctl.sh")))),
	}
	const activateCommandIndex = 3
	for index, remoteCmd := range commands {
		if index == activateCommandIndex {
			// 即使 SSH 在执行后断线，也按“可能已切换”处理并尝试恢复。
			currentActivated = true
		}
		if err := d.ssh(ctx, group.Key, remoteCmd); err != nil {
			return err
		}
	}

	if _, err := os.Stat(d.cfg.RuntimeEnv); err == nil {
		d.info("upload shared runtime env to %s", group.Label)
		if err := d.scp(ctx, group.Key, d.cfg.RuntimeEnv, remoteRuntimeEnv); err != nil {
			return fmt.Errorf("upload runtime env to %s: %w", group.Key.Host, err)
		}
		releaseEnvDir := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "runtime", "release-env"))
		remoteEnvDestination := filepath.ToSlash(filepath.Join(releaseEnvDir, releaseID+".env"))
		remoteCmd := runRemoteAsRoot(group.Key.SSHUser, fmt.Sprintf(
			"set -eu; mkdir -p %s; chmod 600 %s; cp %s %s; chmod 600 %s; rm -f %s",
			shellQuote(releaseEnvDir),
			shellQuote(remoteRuntimeEnv),
			shellQuote(remoteRuntimeEnv),
			shellQuote(remoteEnvDestination),
			shellQuote(remoteEnvDestination),
			shellQuote(remoteRuntimeEnv),
		))
		if err := d.ssh(ctx, group.Key, remoteCmd); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("check runtime env %s: %w", d.cfg.RuntimeEnv, err)
	} else {
		d.warn("runtime env not found, skip: %s", d.cfg.RuntimeEnv)
	}

	deployedTargets := make([]Target, 0, len(group.Targets))
	for _, target := range group.Targets {
		d.info("deploy %s %s on %s", target.Service, target.EndpointID, group.Label)
		// 即使排空命令中途失败，也重启当前实例以解除可能已经写入的 draining 状态。
		rollbackTargets = append(append([]Target(nil), deployedTargets...), target)
		if err := d.drainTarget(ctx, group, target); err != nil {
			return err
		}

		if err := d.ssh(ctx, group.Key, installInstanceCommand(group, target)); err != nil {
			return err
		}
		if err := d.checkTargetHealth(ctx, group, target); err != nil {
			return err
		}
		deployedTargets = append(deployedTargets, target)
	}

	d.cleanupOldReleases(ctx, group)
	currentActivated = false
	deploySucceeded = true
	d.success("%s done release=%s", group.Label, releaseID)
	return nil
}

func (d *Deployer) SyncControlScriptGroup(ctx context.Context, group RemoteGroup) error {
	localSocketCtl := filepath.Join(d.cfg.RootDir, "deploy", "socketctl.sh")
	localInstaller := filepath.Join(d.cfg.RootDir, "deploy", "install-instance.sh")
	for _, script := range []string{localSocketCtl, localInstaller} {
		if _, err := os.Stat(script); err != nil {
			return fmt.Errorf("read local control script %s: %w", script, err)
		}
	}

	// 进程号避免同一秒内的并行同步任务复用远端临时文件。
	updateID := fmt.Sprintf("%s-%d", d.cfg.Now().UTC().Format("20060102150405"), os.Getpid())
	remoteSocketCtl := fmt.Sprintf("/tmp/socket-server-socketctl-%s.sh", updateID)
	remoteInstaller := fmt.Sprintf("/tmp/socket-server-install-instance-%s.sh", updateID)
	installedSocketCtl := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "socketctl"))
	installedInstaller := filepath.ToSlash(filepath.Join(group.Key.DeployDir, "install-instance"))
	stagedSocketCtl := installedSocketCtl + ".new-" + updateID
	stagedInstaller := installedInstaller + ".new-" + updateID

	// 任一上传或安装步骤失败时清理远端临时文件，避免长期同步产生垃圾文件。
	syncSucceeded := false
	defer func() {
		if syncSucceeded {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cleanupCommand := runRemoteAsRoot(group.Key.SSHUser, fmt.Sprintf(
			"rm -f %s %s %s %s",
			shellQuote(remoteSocketCtl),
			shellQuote(remoteInstaller),
			shellQuote(stagedSocketCtl),
			shellQuote(stagedInstaller),
		))
		if err := d.ssh(cleanupCtx, group.Key, cleanupCommand); err != nil {
			d.warn("cleanup control script staging failed on %s: %v", group.Label, err)
		}
	}()

	d.info("update stable control scripts on %s (%s)", group.Label, group.Key.Host)
	if err := d.scp(ctx, group.Key, localSocketCtl, remoteSocketCtl); err != nil {
		return fmt.Errorf("upload socketctl to %s: %w", group.Key.Host, err)
	}
	if err := d.scp(ctx, group.Key, localInstaller, remoteInstaller); err != nil {
		return fmt.Errorf("upload install-instance to %s: %w", group.Key.Host, err)
	}

	// 先在部署目录生成同文件系统临时文件，再通过 mv 原子替换稳定入口。
	remoteCmd := runRemoteAsRoot(group.Key.SSHUser, fmt.Sprintf(
		"set -eu; mkdir -p %s; cp %s %s; cp %s %s; chmod 755 %s %s; mv -f %s %s; mv -f %s %s; rm -f %s %s",
		shellQuote(group.Key.DeployDir),
		shellQuote(remoteSocketCtl),
		shellQuote(stagedSocketCtl),
		shellQuote(remoteInstaller),
		shellQuote(stagedInstaller),
		shellQuote(stagedSocketCtl),
		shellQuote(stagedInstaller),
		shellQuote(stagedSocketCtl),
		shellQuote(installedSocketCtl),
		shellQuote(stagedInstaller),
		shellQuote(installedInstaller),
		shellQuote(remoteSocketCtl),
		shellQuote(remoteInstaller),
	))
	if err := d.ssh(ctx, group.Key, remoteCmd); err != nil {
		return fmt.Errorf("install stable control scripts on %s: %w", group.Key.Host, err)
	}
	syncSucceeded = true

	d.success("stable control scripts updated on %s", group.Label)
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
	unit := targetUnit(target)
	port := targetDisplayPort(target)
	ip := targetDisplayIP(target)
	script := fmt.Sprintf(`
deploy_dir=%s
server_id=%s
service=%s
endpoint_id=%s
host=%s
port=%s
ip=%s
record_file="$deploy_dir/runtime/services/${service}-${endpoint_id}.json"
unit=%s
updated_at="-"

service_state() {
  unit_name="$1"
  if ! command -v systemctl >/dev/null 2>&1; then
    printf "%%s\n" "unknown"
    return
  fi
  if [ "$(id -u)" -eq 0 ]; then
    sudo_cmd=""
  else
    sudo_cmd="${SUDO_CMD:-sudo}"
  fi
  load_state="$($sudo_cmd systemctl show "$unit_name" --property=LoadState --value 2>/dev/null || true)"
  active_state="$($sudo_cmd systemctl show "$unit_name" --property=ActiveState --value 2>/dev/null || true)"
  sub_state="$($sudo_cmd systemctl show "$unit_name" --property=SubState --value 2>/dev/null || true)"
  if [ "$load_state" = "not-found" ]; then
    printf "%%s\n" "not-found"
    return
  fi

  # 将 systemd 的内部状态映射为容器风格的展示状态。
  case "$active_state:$sub_state" in
    active:*)                 printf "%%s\n" "running" ;;
    activating:auto-restart)  printf "%%s\n" "restarting" ;;
    activating:*)             printf "%%s\n" "starting" ;;
    deactivating:*)           printf "%%s\n" "stopping" ;;
    inactive:*)               printf "%%s\n" "stopped" ;;
    failed:*)                 printf "%%s\n" "failed" ;;
    reloading:*)              printf "%%s\n" "restarting" ;;
    *)                        printf "%%s\n" "unknown" ;;
  esac
}
if [ -f "$record_file" ]; then
  record_unit="$(sed -n 's/^[[:space:]]*"unit"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$record_file" | head -n 1)"
  record_updated_at="$(sed -n 's/^[[:space:]]*"updated_at"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$record_file" | head -n 1)"
  [ -z "$record_unit" ] || unit="$record_unit"
  [ -z "$record_updated_at" ] || updated_at="$record_updated_at"
  state="$(service_state "$unit")"
else
  state="not-deployed"
fi
printf "%%-28s %%-16s %%-22s %%-15s %%-25s %%-12s %%-18s %%-24s %%s\n" "$server_id" "$service" "$endpoint_id" "$host" "$deploy_dir" "$port" "$ip" "$state" "$updated_at"
`, shellQuote(target.DeployDir), shellQuote(target.ServerID), shellQuote(target.Service), shellQuote(target.EndpointID), shellQuote(target.Host), shellQuote(port), shellQuote(ip), shellQuote(unit))
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

// AuthenticateGroups 在输出计划、表格或远端日志前预热每台物理主机的 SSH 连接。
//
// SSH 复用启用时，首次 true 命令会完成密码/主机指纹交互并保留 ControlMaster，
// 随后的 ssh 和 scp 将直接复用该连接，避免密码提示符破坏正在输出的内容。
func (d *Deployer) AuthenticateGroups(ctx context.Context, groups []RemoteGroup) error {
	seen := make(map[RemoteKey]struct{}, len(groups))
	for _, group := range groups {
		if _, ok := seen[group.Key]; ok {
			continue
		}
		seen[group.Key] = struct{}{}
		if err := d.ssh(ctx, group.Key, "true"); err != nil {
			return fmt.Errorf("authenticate SSH for %s: %w", group.Label, err)
		}
	}
	return nil
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
	if c.HealthTimeoutSeconds <= 0 {
		c.HealthTimeoutSeconds = 30
	}
	if c.HealthTimeoutSeconds > 3600 {
		return fmt.Errorf("DEPLOY_HEALTH_TIMEOUT_SECONDS must not exceed 3600")
	}
	if c.DrainTimeoutSeconds <= 0 {
		c.DrainTimeoutSeconds = 30
	}
	if c.DrainTimeoutSeconds > 3600 {
		return fmt.Errorf("DEPLOY_DRAIN_TIMEOUT_SECONDS must not exceed 3600")
	}
	if c.RetainReleases <= 0 {
		c.RetainReleases = 5
	}
	if c.RetainReleases > 100 {
		return fmt.Errorf("DEPLOY_RETAIN_RELEASES must not exceed 100")
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
		RootDir:              rootDir,
		TargetsFile:          os.Getenv("TARGETS_FILE"),
		AppConfig:            os.Getenv("APP_CONFIG"),
		RuntimeEnv:           os.Getenv("RUNTIME_ENV"),
		PackageFile:          os.Getenv("PACKAGE_FILE"),
		BuildDir:             os.Getenv("BUILD_DIR"),
		ReleaseVersion:       os.Getenv("RELEASE_VERSION"),
		GitCommit:            os.Getenv("GIT_COMMIT"),
		HealthTimeoutSeconds: envIntValue("DEPLOY_HEALTH_TIMEOUT_SECONDS"),
		DrainTimeoutSeconds:  envIntValue("DEPLOY_DRAIN_TIMEOUT_SECONDS"),
		RetainReleases:       envIntValue("DEPLOY_RETAIN_RELEASES"),
		SkipBuild:            os.Getenv("SKIP_BUILD") == "1",
		SSHOpts:              strings.Fields(os.Getenv("SSH_OPTS")),
		SSHMultiplex:         os.Getenv("SSH_MULTIPLEX") != "0",
		SSHControlPersist:    os.Getenv("SSH_CONTROL_PERSIST"),
		SSHControlDir:        os.Getenv("SSH_CONTROL_DIR"),
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

func createTarGz(sourceDir string, targetFile string) (returnErr error) {
	// 发布包内含运行配置，使用仅当前用户可读写的权限创建归档。
	out, err := os.OpenFile(targetFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create package %s: %w", targetFile, err)
	}
	defer func() {
		if err := out.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("close package file: %w", err)
		}
	}()

	gz := gzip.NewWriter(out)
	defer func() {
		if err := gz.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("close gzip writer: %w", err)
		}
	}()
	tw := tar.NewWriter(gz)
	defer func() {
		if err := tw.Close(); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("close tar writer: %w", err)
		}
	}()

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

// validDeploymentIdentifier 限制会进入 systemd unit 和远端文件名的标识符。
func validDeploymentIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, current := range value {
		switch {
		case current >= 'a' && current <= 'z':
		case current >= 'A' && current <= 'Z':
		case current >= '0' && current <= '9':
		case current == '.', current == '-', current == '_':
		default:
			return false
		}
	}
	return true
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
