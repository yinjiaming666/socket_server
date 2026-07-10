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
