package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/websocket"

	"socket_server/internal/domain"
)

type routeResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		EndpointID string `json:"endpoint_id"`
		WSURL      string `json:"ws_url"`
	} `json:"data"`
}

type simulatedConn struct {
	userID, connID string
	ws             *websocket.Conn
	writeMu        sync.Mutex
}

func (c *simulatedConn) sendControl(method string, data any) error {
	body, err := domain.MarshalClientControl(method, data)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return websocket.Message.Send(c.ws, string(body))
}

func (s *simulator) connectAll(ctx context.Context) {
	sem := make(chan struct{}, s.options.connectConcurrency)
	var group sync.WaitGroup
	for _, user := range s.users {
		for index := 0; index < s.options.connectionsPerUser; index++ {
			user, index := user, index
			group.Add(1)
			go func() {
				defer group.Done()
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					return
				}
				if err := s.connectOne(ctx, user, index); err != nil {
					s.stats.connFails.Add(1)
					s.logError("connect user=%s index=%d: %v", user, index, err)
				}
			}()
		}
	}
	group.Wait()
}

func (s *simulator) connectOne(ctx context.Context, user string, index int) error {
	route, err := s.fetchRoute(ctx, user)
	if err != nil {
		return err
	}
	connID := fmt.Sprintf("%s-%d-%s", user, index+1, randomSuffix())
	wsURL, err := s.buildWebSocketURL(route.Data.WSURL, connID)
	if err != nil {
		return err
	}
	config, err := websocket.NewConfig(wsURL, s.options.origin)
	if err != nil {
		return err
	}
	ws, err := websocket.DialConfig(config)
	if err != nil {
		return err
	}
	conn := &simulatedConn{userID: user, connID: connID, ws: ws}
	if err := conn.sendControl("join_room", domain.ClientRoomData{RoomID: s.options.roomID}); err != nil {
		_ = ws.Close()
		return err
	}
	s.mu.Lock()
	s.conns = append(s.conns, conn)
	s.mu.Unlock()
	s.stats.connected.Add(1)
	s.readers.Add(1)
	go s.readLoop(conn)
	return nil
}

func (s *simulator) fetchRoute(ctx context.Context, user string) (*routeResponse, error) {
	endpoint := strings.TrimRight(s.options.gatewayURL, "/") + "/api/ws/route"
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-User-ID", user)
		resp, err := s.http.Do(req)
		if err != nil {
			lastErr = err
		} else {
			var result routeResponse
			decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result)
			_ = resp.Body.Close()
			if decodeErr == nil && resp.StatusCode < 300 && result.Code == 0 && result.Data.WSURL != "" {
				return &result, nil
			}
			if decodeErr != nil {
				lastErr = decodeErr
			} else {
				lastErr = fmt.Errorf("route status=%d code=%d message=%s", resp.StatusCode, result.Code, result.Message)
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
		}
	}
	return nil, lastErr
}

func (s *simulator) buildWebSocketURL(routeURL, connID string) (string, error) {
	parsed, err := url.Parse(routeURL)
	if err != nil {
		return "", err
	}
	if s.options.websocketBaseURL != "" {
		override, err := url.Parse(s.options.websocketBaseURL)
		if err != nil {
			return "", err
		}
		if override.Scheme != "" {
			parsed.Scheme = override.Scheme
		}
		if override.Host != "" {
			parsed.Host = override.Host
		}
	}
	query := parsed.Query()
	query.Set("conn_id", connID)
	query.Set("device_id", "simulator")
	query.Set("platform", "go")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (s *simulator) readLoop(conn *simulatedConn) {
	defer s.readers.Done()
	for {
		var raw string
		if err := websocket.Message.Receive(conn.ws, &raw); err != nil {
			return
		}
		s.stats.received.Add(1)
		envelope, err := domain.DecodeClientEnvelope([]byte(raw))
		if err != nil {
			s.logError("decode conn=%s: %v", conn.connID, err)
			continue
		}
		if envelope.M != domain.ClientMessageMethod {
			continue
		}

		var message domain.ClientMessageData
		if err := json.Unmarshal(envelope.Data, &message); err != nil {
			s.logError("decode message data conn=%s: %v", conn.connID, err)
			continue
		}
		if message.AckRequired && message.MessageID != "" {
			if err := conn.sendControl("ack", domain.ClientAckData{MessageID: message.MessageID}); err != nil {
				s.logError("ack conn=%s message=%s: %v", conn.connID, message.MessageID, err)
				continue
			}
			s.stats.acked.Add(1)
		}
	}
}

func (s *simulator) closeAll() {
	s.mu.Lock()
	conns := append([]*simulatedConn(nil), s.conns...)
	s.mu.Unlock()
	for _, conn := range conns {
		_ = conn.ws.Close()
	}
}
