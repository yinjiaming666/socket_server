package host

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/yinjiaming666/wskit"

	"socket_server/internal/config"
)

func NewBackend(cfg config.RedisConfig) *wskit.Backend {
	return wskit.NewRedisBackend(wskit.RedisConfig{
		Addr: cfg.Addr, Password: cfg.Password, DB: cfg.DB,
		PoolSize: cfg.PoolSize, KeyPrefix: cfg.KeyPrefix,
	})
}

func ServeHTTP(ctx context.Context, addr string, handler http.Handler) error {
	if ctx.Err() != nil {
		return nil
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	defer server.Close()
	done := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return errors.Join(server.Shutdown(shutdownCtx), <-done)
	}
}
