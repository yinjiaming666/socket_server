package redisclient

import (
	"context"
	"fmt"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"
)

// Config 表示 Redis 客户端配置。
type Config struct {
	Addr      string
	Password  string
	DB        int
	PoolSize  int
	KeyPrefix string
	Timeout   time.Duration
}

// Client 是基于 go-redis 的连接池客户端。
type Client struct {
	config Config
	client *redis.Client
}

// New 创建 Redis 客户端。
func New(config Config) *Client {
	if config.Addr == "" {
		config.Addr = "127.0.0.1:6379"
	}
	if config.Timeout <= 0 {
		config.Timeout = 3 * time.Second
	}
	if config.PoolSize <= 0 {
		config.PoolSize = 20
	}
	c := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		DialTimeout:  config.Timeout,
		ReadTimeout:  config.Timeout,
		WriteTimeout: config.Timeout,
	})
	return &Client{config: config, client: c}
}

// Key 返回带统一前缀的 Redis Key。
func (c *Client) Key(key string) string {
	if c.config.KeyPrefix == "" {
		return key
	}
	return c.config.KeyPrefix + key
}

// Do 执行 Redis 命令。
func (c *Client) Do(ctx context.Context, args ...string) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("redis command is empty")
	}
	values := make([]any, 0, len(args))
	for _, arg := range args {
		values = append(values, arg)
	}
	result, err := c.client.Do(ctx, values...).Result()
	if err == redis.Nil {
		return nil, nil
	}
	return result, err
}

// String 执行命令并返回字符串。
func (c *Client) String(ctx context.Context, args ...string) (string, error) {
	value, err := c.Do(ctx, args...)
	if err != nil {
		return "", err
	}
	return anyToString(value), nil
}

// Strings 执行命令并返回字符串列表。
func (c *Client) Strings(ctx context.Context, args ...string) ([]string, error) {
	value, err := c.Do(ctx, args...)
	if err != nil {
		return nil, err
	}
	return anyToStrings(value), nil
}

// StringLists 使用同一个 pipeline 批量执行命令，并按输入顺序返回各命令的字符串列表结果。
// 该方法主要用于批量读取独立 Key，避免调用方为每个用户或连接产生一次网络往返。
func (c *Client) StringLists(ctx context.Context, commands ...[]string) ([][]string, error) {
	results := make([][]string, len(commands))
	pipe := c.client.Pipeline()
	cmds := make([]*redis.Cmd, len(commands))
	queued := false

	for index, args := range commands {
		if len(args) == 0 {
			continue
		}
		values := make([]any, 0, len(args))
		for _, arg := range args {
			values = append(values, arg)
		}
		cmds[index] = pipe.Do(ctx, values...)
		queued = true
	}
	if !queued {
		return results, nil
	}

	// redis.Nil 仅表示某个读取目标不存在，不应丢弃同一批次内其它命令的有效结果。
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	for index, cmd := range cmds {
		if cmd == nil {
			continue
		}
		value, err := cmd.Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, err
		}
		results[index] = anyToStrings(value)
	}
	return results, nil
}

// Pipeline 使用普通 pipeline 批量执行无需返回值的命令。
func (c *Client) Pipeline(ctx context.Context, commands ...[]string) error {
	pipe := c.client.Pipeline()
	queued := false
	for _, args := range commands {
		if len(args) == 0 {
			continue
		}
		values := make([]any, 0, len(args))
		for _, arg := range args {
			values = append(values, arg)
		}
		pipe.Do(ctx, values...)
		queued = true
	}
	if !queued {
		return nil
	}
	_, err := pipe.Exec(ctx)
	return err
}

// SubscribePattern 使用 Redis PSUBSCRIBE 订阅频道模式。
func (c *Client) SubscribePattern(ctx context.Context, pattern string, handler func(channel string, payload []byte)) error {
	pubSub := c.client.PSubscribe(ctx, c.Key(pattern))
	defer pubSub.Close()

	if _, err := pubSub.Receive(ctx); err != nil {
		return err
	}
	channel := pubSub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-channel:
			if !ok {
				return nil
			}
			redisChannel := strings.TrimPrefix(message.Channel, c.config.KeyPrefix)
			handler(redisChannel, []byte(message.Payload))
		}
	}
}

// Transaction 在一个 MULTI/EXEC 事务中执行多条 Redis 命令。
func (c *Client) Transaction(ctx context.Context, commands ...[]string) error {
	pipe := c.client.TxPipeline()
	for _, args := range commands {
		if len(args) == 0 {
			continue
		}
		values := make([]any, 0, len(args))
		for _, arg := range args {
			values = append(values, arg)
		}
		pipe.Do(ctx, values...)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// ExpireKeys 使用 pipeline 批量刷新一组已加前缀 Redis Key 的 TTL。
func (c *Client) ExpireKeys(ctx context.Context, keys []string, ttl time.Duration) error {
	if len(keys) == 0 {
		return nil
	}
	pipe := c.client.Pipeline()
	for _, key := range keys {
		pipe.Expire(ctx, key, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Ping 验证 Redis 连接和命令执行链路是否可用。
func (c *Client) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// Close 关闭 Redis 连接池。
func (c *Client) Close() error {
	return c.client.Close()
}

// anyToString 转换 Redis 返回值为字符串。
func anyToString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

// anyToStrings 转换 Redis 返回值为字符串列表。
func anyToStrings(value any) []string {
	switch typed := value.(type) {
	case nil:
		return nil
	case []string:
		return typed
	case []any:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			result = append(result, anyToString(item))
		}
		return result
	default:
		return []string{anyToString(typed)}
	}
}
