package gateway

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"socket_server/internal/domain"
)

// RouteTokenManager 负责生成和校验 Gateway 下发的临时路由凭证。
type RouteTokenManager struct {
	// secret 表示 HMAC 签名密钥。
	secret []byte
	// ttl 表示路由凭证有效期。
	ttl time.Duration
}

// NewRouteTokenManager 创建路由凭证管理器。
func NewRouteTokenManager(secret string, ttl time.Duration) *RouteTokenManager {
	if secret == "" {
		secret = "dev-route-token-secret"
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &RouteTokenManager{secret: []byte(secret), ttl: ttl}
}

// Generate 为指定用户和节点生成临时路由凭证。
func (m *RouteTokenManager) Generate(userID string, endpointID string) (string, error) {
	claims := domain.RouteTokenClaims{
		UserID:     userID,
		EndpointID: endpointID,
		ExpiresAt:  time.Now().Add(m.ttl).Unix(),
	}
	body, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	payload := base64.RawURLEncoding.EncodeToString(body)
	signature := m.sign(payload)
	return payload + "." + signature, nil
}

// Verify 校验临时路由凭证并返回凭证内容。
func (m *RouteTokenManager) Verify(token string) (*domain.RouteTokenClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return nil, errors.New("invalid route token")
	}

	if !hmac.Equal([]byte(parts[1]), []byte(m.sign(parts[0]))) {
		return nil, errors.New("invalid route token signature")
	}

	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, err
	}

	var claims domain.RouteTokenClaims
	if err := json.Unmarshal(body, &claims); err != nil {
		return nil, err
	}
	if time.Now().Unix() > claims.ExpiresAt {
		return nil, errors.New("route token expired")
	}
	return &claims, nil
}

// sign 对路由凭证载荷进行 HMAC 签名。
func (m *RouteTokenManager) sign(payload string) string {
	mac := hmac.New(sha256.New, m.secret)
	_, _ = mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
