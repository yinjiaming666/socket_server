package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

func (s *simulator) startPeriodicSender(ctx context.Context, group *sync.WaitGroup, interval time.Duration, send func(context.Context, int64) error) {
	if interval <= 0 {
		return
	}
	group.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var sequence int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sequence++
				if err := send(ctx, sequence); err != nil && !errors.Is(err, context.Canceled) {
					s.stats.requestFails.Add(1)
					s.logError("send sequence=%d: %v", sequence, err)
				}
			}
		}
	})
}

func (s *simulator) sendSingle(ctx context.Context, sequence int64) error {
	receiver := s.users[int(sequence)%len(s.users)]
	body := map[string]any{
		"from_user_id": s.users[0],
		"to_user_id":   receiver,
		"content":      map[string]any{"text": fmt.Sprintf("single-%d", sequence), "sequence": sequence},
	}
	if err := s.postJSON(ctx, "/api/messages/single", body); err != nil {
		return err
	}
	s.stats.singleSent.Add(1)
	return nil
}

func (s *simulator) sendGroup(ctx context.Context, sequence int64) error {
	body := map[string]any{
		"from_user_id":    s.users[0],
		"group_id":        "sim-group",
		"member_user_ids": append([]string(nil), s.users[:s.options.groupSize]...),
		"content":         map[string]any{"text": fmt.Sprintf("group-%d", sequence), "sequence": sequence},
	}
	if err := s.postJSON(ctx, "/api/messages/group", body); err != nil {
		return err
	}
	s.stats.groupSent.Add(1)
	return nil
}

func (s *simulator) sendLive(ctx context.Context, sequence int64) error {
	body := map[string]any{
		"from_user_id": s.users[0],
		"type":         "danmu",
		"content":      map[string]any{"text": fmt.Sprintf("live-%d", sequence), "sequence": sequence},
	}
	path := "/api/live/rooms/" + url.PathEscape(s.options.roomID) + "/messages"
	if err := s.postJSON(ctx, path, body); err != nil {
		return err
	}
	s.stats.liveSent.Add(1)
	return nil
}

func (s *simulator) postJSON(ctx context.Context, path string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	endpoint := strings.TrimRight(s.options.messageAPIURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	var envelope struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(responseBody, &envelope); err != nil {
		return err
	}
	if envelope.Code != 0 {
		return fmt.Errorf("code=%d message=%s", envelope.Code, envelope.Message)
	}
	return nil
}
