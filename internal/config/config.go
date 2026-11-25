package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

// AppConfig 表示应用完整配置。
type AppConfig struct {
	// Storage 表示 wskit 使用的 Redis 共享状态存储。
	Storage string `json:"storage"`
	// Redis 表示 Redis 连接配置。
	Redis RedisConfig `json:"redis"`
	// RouteToken 表示全局 route_token 配置，所有 Gateway 和 WebSocket Node 必须共用。
	RouteToken RouteTokenConfig `json:"route_token"`
	// Gateways 表示 Gateway 服务实例配置列表。
	Gateways []GatewayConfig `json:"gateways"`
	// MessageAPIs 表示 Message API 服务实例配置列表。
	MessageAPIs []MessageAPIConfig `json:"message_apis"`
	// Nodes 表示可用 WebSocket 节点配置列表。
	Nodes []WebSocketNodeConfig `json:"nodes"`
}

// RedisConfig 表示 Redis 连接配置。
type RedisConfig struct {
	// Addr 表示 Redis 地址，例如 127.0.0.1:6379。
	Addr string `json:"addr"`
	// Password 表示 Redis 密码，为空表示无密码。
	Password string `json:"password"`
	// DB 表示 Redis 数据库编号。
	DB int `json:"db"`
	// PoolSize 表示 Redis 连接池大小。
	PoolSize int `json:"pool_size"`
	// KeyPrefix 表示 Redis Key 前缀。
	KeyPrefix string `json:"key_prefix"`
}

// RouteTokenConfig 表示全局路由凭证配置。
type RouteTokenConfig struct {
	// Secret 表示 route_token 签名密钥。
	Secret string `json:"secret"`
	// TTLSeconds 表示 route_token 有效秒数。
	TTLSeconds int `json:"ttl_seconds"`
}

// GatewayConfig 表示 Go Gateway 服务实例配置。
type GatewayConfig struct {
	// EndpointID 表示 Gateway 服务实例唯一编号。
	EndpointID string `json:"endpoint_id"`
	// Addr 表示 Gateway 内部访问地址。
	Addr string `json:"addr"`
	// Port 表示 Gateway HTTP 监听端口。
	Port int `json:"port"`
	// PublicHost 表示客户端访问 WebSocket 使用的公网域名。
	PublicHost string `json:"public_host"`
	// RouteTokenSecret 表示旧版 route_token 签名密钥，兼容读取后会合并到全局配置。
	RouteTokenSecret string `json:"route_token_secret"`
	// RouteTokenTTLSeconds 表示旧版 route_token 有效秒数，兼容读取后会合并到全局配置。
	RouteTokenTTLSeconds int `json:"route_token_ttl_seconds"`
	// HeartbeatTimeoutSeconds 表示节点心跳超时秒数。
	HeartbeatTimeoutSeconds int `json:"heartbeat_timeout_seconds"`
	// LastEndpointTTLSeconds 表示用户最近连接节点缓存秒数。
	LastEndpointTTLSeconds int `json:"last_endpoint_ttl_seconds"`
}

// MessageAPIConfig 表示 Message API 服务实例配置。
type MessageAPIConfig struct {
	// EndpointID 表示 Message API 服务实例唯一编号。
	EndpointID string `json:"endpoint_id"`
	// Addr 表示 Message API 内部访问地址。
	Addr string `json:"addr"`
	// Port 表示 Message API HTTP 监听端口。
	Port int `json:"port"`
}

// WebSocketNodeConfig 表示单个 WebSocket 节点配置。
type WebSocketNodeConfig struct {
	// EndpointID 表示节点唯一编号。
	EndpointID string `json:"endpoint_id"`
	// WSPath 表示 Nginx 转发 WebSocket 使用的路径。
	WSPath string `json:"ws_path"`
	// WSAddr 表示 WebSocket Node 对外或内网监听地址。
	WSAddr string `json:"ws_addr"`
	// GRPCAddr 表示 WebSocket Node 内部推送服务地址。
	GRPCAddr string `json:"grpc_addr"`
	// Port 表示 WebSocket Node HTTP 监听端口。
	Port int `json:"port"`
	// GRPCPort 表示 WebSocket Node gRPC 监听端口。
	GRPCPort int `json:"grpc_port"`
	// MaxConn 表示节点最大连接数量。
	MaxConn int64 `json:"max_conn"`
	// SendBuffer 表示单连接发送队列长度。
	SendBuffer int `json:"send_buffer"`
	// HeartbeatIntervalSeconds 表示节点心跳上报间隔秒数。
	HeartbeatIntervalSeconds int `json:"heartbeat_interval_seconds"`
}

// Load 读取配置文件并合并环境变量覆盖项。
func Load(path string) (*AppConfig, error) {
	if path == "" {
		path = os.Getenv("CONFIG_FILE")
	}
	if path == "" {
		path = "configs/app.json"
	}

	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg AppConfig
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return nil, errors.New("config must contain a single JSON object")
		}
		return nil, err
	}

	cfg.ApplyDefaults()
	cfg.ApplyEnv()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// ApplyDefaults 填充配置默认值。
func (c *AppConfig) ApplyDefaults() {
	if c.Storage == "" {
		c.Storage = "redis"
	}
	if c.Redis.Addr == "" {
		c.Redis.Addr = "127.0.0.1:6379"
	}
	if c.Redis.PoolSize <= 0 {
		c.Redis.PoolSize = 20
	}
	if c.RouteToken.Secret == "" {
		for _, gateway := range c.Gateways {
			if gateway.RouteTokenSecret != "" {
				c.RouteToken.Secret = gateway.RouteTokenSecret
				break
			}
		}
	}
	if c.RouteToken.Secret == "" {
		c.RouteToken.Secret = "dev-route-token-secret"
	}
	if c.RouteToken.TTLSeconds <= 0 {
		for _, gateway := range c.Gateways {
			if gateway.RouteTokenTTLSeconds > 0 {
				c.RouteToken.TTLSeconds = gateway.RouteTokenTTLSeconds
				break
			}
		}
	}
	if c.RouteToken.TTLSeconds <= 0 {
		c.RouteToken.TTLSeconds = 30
	}

	for index := range c.Gateways {
		gateway := &c.Gateways[index]
		if gateway.Port <= 0 {
			gateway.Port = 7000 + index
		}
		if gateway.PublicHost == "" {
			gateway.PublicHost = "example.com"
		}
		if gateway.HeartbeatTimeoutSeconds <= 0 {
			gateway.HeartbeatTimeoutSeconds = 60
		}
		if gateway.LastEndpointTTLSeconds <= 0 {
			gateway.LastEndpointTTLSeconds = 600
		}
		if gateway.Addr == "" && gateway.Port > 0 {
			gateway.Addr = "127.0.0.1:" + strconv.Itoa(gateway.Port)
		}
	}
	for index := range c.MessageAPIs {
		messageAPI := &c.MessageAPIs[index]
		if messageAPI.Port <= 0 {
			messageAPI.Port = 7100 + index
		}
		if messageAPI.Addr == "" && messageAPI.Port > 0 {
			messageAPI.Addr = "127.0.0.1:" + strconv.Itoa(messageAPI.Port)
		}
	}
	for index := range c.Nodes {
		node := &c.Nodes[index]
		if node.Port <= 0 {
			node.Port = 8081 + index
		}
		if node.GRPCPort <= 0 {
			node.GRPCPort = 9001 + index
		}
		if node.WSPath == "" && node.EndpointID != "" {
			node.WSPath = "/ws/" + node.EndpointID
		}
		if node.WSAddr == "" {
			node.WSAddr = "127.0.0.1:" + strconv.Itoa(node.Port)
		}
		if node.GRPCAddr == "" {
			node.GRPCAddr = "127.0.0.1:" + strconv.Itoa(node.GRPCPort)
		}
		if node.MaxConn <= 0 {
			node.MaxConn = 50000
		}
		if node.SendBuffer <= 0 {
			node.SendBuffer = 128
		}
		if node.HeartbeatIntervalSeconds <= 0 {
			node.HeartbeatIntervalSeconds = 15
		}
	}
}

// ApplyEnv 使用环境变量覆盖配置文件。
func (c *AppConfig) ApplyEnv() {
	c.Redis.Addr = envString("REDIS_ADDR", c.Redis.Addr)
	c.Redis.Password = envString("REDIS_PASSWORD", c.Redis.Password)
	c.Redis.DB = envInt("REDIS_DB", c.Redis.DB)
	c.Redis.PoolSize = envInt("REDIS_POOL_SIZE", c.Redis.PoolSize)
	c.Redis.KeyPrefix = envString("REDIS_KEY_PREFIX", c.Redis.KeyPrefix)
	c.RouteToken.Secret = envString("ROUTE_TOKEN_SECRET", c.RouteToken.Secret)
	c.RouteToken.TTLSeconds = envInt("ROUTE_TOKEN_TTL_SECONDS", c.RouteToken.TTLSeconds)
}

// Validate 校验配置是否合法。
func (c *AppConfig) Validate() error {
	if c.Storage != "redis" {
		return fmt.Errorf("unsupported storage mode: %s (wskit requires redis)", c.Storage)
	}
	if c.Redis.Addr == "" {
		return errors.New("redis addr is empty")
	}
	if c.RouteToken.Secret == "" {
		return errors.New("route_token secret is empty")
	}
	if len(c.Gateways) == 0 && len(c.MessageAPIs) == 0 && len(c.Nodes) == 0 {
		return errors.New("service config is empty")
	}
	seen := make(map[string]string)
	for _, gateway := range c.Gateways {
		if gateway.Port < 1 || gateway.Port > 65535 {
			return fmt.Errorf("gateway %s port is invalid", gateway.EndpointID)
		}
		if gateway.EndpointID == "" {
			return errors.New("gateway endpoint_id is empty")
		}
		if gateway.Addr == "" {
			return fmt.Errorf("gateway %s addr is empty", gateway.EndpointID)
		}
		if err := rememberEndpoint(seen, gateway.EndpointID, "gateway"); err != nil {
			return err
		}
	}
	for _, messageAPI := range c.MessageAPIs {
		if messageAPI.Port < 1 || messageAPI.Port > 65535 {
			return fmt.Errorf("message_api %s port is invalid", messageAPI.EndpointID)
		}
		if messageAPI.EndpointID == "" {
			return errors.New("message_api endpoint_id is empty")
		}
		if messageAPI.Addr == "" {
			return fmt.Errorf("message_api %s addr is empty", messageAPI.EndpointID)
		}
		if err := rememberEndpoint(seen, messageAPI.EndpointID, "message-api"); err != nil {
			return err
		}
	}
	for _, node := range c.Nodes {
		if node.Port < 1 || node.Port > 65535 || node.GRPCPort < 1 || node.GRPCPort > 65535 {
			return fmt.Errorf("node %s port is invalid", node.EndpointID)
		}
		if node.EndpointID == "" {
			return errors.New("node endpoint_id is empty")
		}
		if node.WSPath == "" {
			return fmt.Errorf("node %s ws_path is empty", node.EndpointID)
		}
		if node.WSAddr == "" {
			return fmt.Errorf("node %s ws_addr is empty", node.EndpointID)
		}
		if node.GRPCAddr == "" {
			return fmt.Errorf("node %s grpc_addr is empty", node.EndpointID)
		}
		if err := rememberEndpoint(seen, node.EndpointID, "websocket-node"); err != nil {
			return err
		}
	}
	return nil
}

// rememberEndpoint 记录 endpoint_id 并校验全局唯一。
func rememberEndpoint(seen map[string]string, endpointID string, service string) error {
	if old, ok := seen[endpointID]; ok {
		return fmt.Errorf("endpoint_id %s duplicated between %s and %s", endpointID, old, service)
	}
	seen[endpointID] = service
	return nil
}

// SelectEndpoint 根据 ENDPOINT_ID 选择启动实例，未设置时使用第一个实例。
func SelectEndpoint[T any](service string, instances []T, find func(string) (*T, bool)) (*T, error) {
	endpointID := os.Getenv("ENDPOINT_ID")
	if endpointID == "" && len(instances) > 0 {
		return &instances[0], nil
	}
	if instance, ok := find(endpointID); ok {
		return instance, nil
	}
	return nil, fmt.Errorf("%s endpoint %q: %w", service, endpointID, os.ErrNotExist)
}

// FindGateway 根据 endpoint_id 查找 Gateway 实例配置。
func (c *AppConfig) FindGateway(endpointID string) (*GatewayConfig, bool) {
	for index := range c.Gateways {
		if c.Gateways[index].EndpointID == endpointID {
			return &c.Gateways[index], true
		}
	}
	return nil, false
}

// FindMessageAPI 根据 endpoint_id 查找 Message API 实例配置。
func (c *AppConfig) FindMessageAPI(endpointID string) (*MessageAPIConfig, bool) {
	for index := range c.MessageAPIs {
		if c.MessageAPIs[index].EndpointID == endpointID {
			return &c.MessageAPIs[index], true
		}
	}
	return nil, false
}

// FindNode 根据 endpoint_id 查找 WebSocket 节点配置。
func (c *AppConfig) FindNode(endpointID string) (*WebSocketNodeConfig, bool) {
	for index := range c.Nodes {
		if c.Nodes[index].EndpointID == endpointID {
			return &c.Nodes[index], true
		}
	}
	return nil, false
}

// RouteTokenTTL 返回 route_token 有效时间。
func (c *AppConfig) RouteTokenTTL() time.Duration {
	return time.Duration(c.RouteToken.TTLSeconds) * time.Second
}

// HeartbeatTimeout 返回节点心跳超时时间。
func (g *GatewayConfig) HeartbeatTimeout() time.Duration {
	return time.Duration(g.HeartbeatTimeoutSeconds) * time.Second
}

// LastEndpointTTL 返回用户最近连接节点缓存时间。
func (g *GatewayConfig) LastEndpointTTL() time.Duration {
	return time.Duration(g.LastEndpointTTLSeconds) * time.Second
}

// NodeHeartbeatInterval 返回指定节点心跳上报间隔。
func (n *WebSocketNodeConfig) NodeHeartbeatInterval() time.Duration {
	return time.Duration(n.HeartbeatIntervalSeconds) * time.Second
}

// envString 读取字符串环境变量，未配置时返回默认值。
func envString(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// envInt 读取整数环境变量，未配置或解析失败时返回默认值。
func envInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsed
}
