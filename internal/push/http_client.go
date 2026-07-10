package push

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"socket_server/internal/domain"
	"socket_server/internal/endpoint"
)

// HTTPClient 使用 WebSocket Node HTTP 内部接口跨进程推送消息。
type HTTPClient struct {
	// endpoints 表示 endpoint 仓储，用于查询节点地址。
	endpoints endpoint.Repository
	// client 表示 HTTP 客户端。
	client *http.Client
}

// PushRequest 表示推送到节点的请求体。
type PushRequest struct {
	ConnIDs []string        `json:"conn_ids"`
	UserIDs []string        `json:"user_ids"`
	Message *domain.Message `json:"message"`
}

// NewHTTPClient 创建 HTTP 推送客户端。
func NewHTTPClient(endpoints endpoint.Repository) *HTTPClient {
	return &HTTPClient{
		endpoints: endpoints,
		client:    &http.Client{Timeout: 5 * time.Second},
	}
}

// PushToConns 推送消息到指定 endpoint 上的连接。
func (c *HTTPClient) PushToConns(ctx context.Context, endpointID string, connIDs []string, msg *domain.Message) (*domain.PushResponse, error) {
	req := PushRequest{ConnIDs: connIDs, Message: msg}
	return c.post(ctx, endpointID, "/api/ws-node/push-conns", req)
}

// PushToUsers 推送消息到指定 endpoint 上的用户。
func (c *HTTPClient) PushToUsers(ctx context.Context, endpointID string, userIDs []string, msg *domain.Message) (*domain.PushResponse, error) {
	req := PushRequest{UserIDs: userIDs, Message: msg}
	return c.post(ctx, endpointID, "/api/ws-node/push-users", req)
}

// post 请求指定节点内部推送接口。
func (c *HTTPClient) post(ctx context.Context, endpointID string, path string, body any) (*domain.PushResponse, error) {
	ep, err := c.endpoints.GetEndpoint(ctx, endpointID)
	if err != nil {
		return nil, err
	}
	addr := ep.WSAddr
	if addr == "" {
		return nil, errors.New("endpoint ws_addr is empty")
	}
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode >= 300 {
		return nil, errors.New(httpResp.Status)
	}

	var response struct {
		Data domain.PushResponse `json:"data"`
	}
	if err := json.NewDecoder(httpResp.Body).Decode(&response); err != nil {
		return nil, err
	}
	return &response.Data, nil
}
