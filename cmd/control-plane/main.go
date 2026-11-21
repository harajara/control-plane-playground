package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"tenant-control-plane/internal/applog"
	"tenant-control-plane/internal/domain"
	"tenant-control-plane/internal/httpapi"
	"tenant-control-plane/internal/messaging"
	"tenant-control-plane/internal/secure"
	"tenant-control-plane/internal/store"
)

// enabled reports whether an env flag is on. Anything other than the usual
// true spellings is off, so a typo cannot silently change the default.
func enabled(key string) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "1", "t", "true", "yes", "on":
		return true
	}
	return false
}

func main() {
	applog.Init()
	if err := run(); err != nil {
		slog.Error("control-plane stopped", "error_type", fmt.Sprintf("%T", err))
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	s, err := store.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer s.DB.Close()
	signer, err := secure.SignerFromEnv()
	if err != nil {
		return err
	}
	sealer, err := secure.SealerFromEnv()
	if err != nil {
		return err
	}
	b, err := messaging.Open(os.Getenv("KAFKA_BROKERS"), "control-plane", []string{messaging.Results, messaging.Heartbeats}, signer)
	if err != nil {
		return err
	}
	defer b.Client.Close()
	// CP_FAST_PUBLISH=false disables only the in-request hand-off to the
	// relay, leaving the sweeper to publish. That is what makes the sweeper
	// path observable instead of timing-dependent.
	fast := enabled("CP_FAST_PUBLISH")
	wake := make(chan string, 128)
	api := &httpapi.Server{Store: s, Sealer: sealer, Wake: wake, FastPublish: fast, SecureCookies: enabled("COOKIE_SECURE")}
	if err = api.Bootstrap(ctx, os.Getenv("ADMIN_EMAIL"), os.Getenv("ADMIN_PASSWORD")); err != nil {
		return err
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	defer stop()
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.Run(ctx, func(ctx context.Context, e messaging.Event) error {
			if e.Type == "worker.heartbeat" {
				var w domain.Worker
				if json.Unmarshal(e.Data, &w) != nil || w.ID == "" {
					return messaging.ErrPoison
				}
				return s.Heartbeat(ctx, w)
			}
			var r domain.Result
			if json.Unmarshal(e.Data, &r) != nil {
				return messaging.ErrPoison
			}
			if _, err := uuid.Parse(r.TaskID); err != nil {
				return messaging.ErrPoison
			}
			if _, err := uuid.Parse(r.TenantID); err != nil {
				return messaging.ErrPoison
			}
			if e.TenantID != r.TenantID || e.Type != "task."+r.Status || (r.Status != "in_progress" && r.Status != "done" && r.Status != "failed") {
				return messaging.ErrPoison
			}
			err := s.Apply(ctx, e.ID, r)
			var p store.Problem
			if errors.As(err, &p) {
				slog.Warn("result rejected", "code", string(p), "task_type", r.Type, "status", r.Status)
				return messaging.ErrPoison
			}
			return err
		})
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		sweep(ctx, s, b, wake, fast)
	}()
	server := &http.Server{Addr: ":8080", Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	slog.Info("control-plane listening", "address", ":8080", "fast_publish", fast)
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// sweep drains newly committed tasks (fast path) and, once a second, any row
// still unpublished after five seconds. Both go through the same locked
// publish so replicas cannot race each other.
func sweep(ctx context.Context, s *store.Store, b *messaging.Broker, wake <-chan string, fast bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		id := ""
		select {
		case <-ctx.Done():
			return
		case id = <-wake:
			if !fast {
				id = ""
			}
		case <-ticker.C:
		}
		for n := 0; n < 100; n++ {
			sendCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			sent, err := s.PublishOne(sendCtx, id, b.Command)
			cancel()
			if err != nil {
				slog.Warn("outbox publish pending retry")
				break
			}
			if !sent || id != "" {
				break
			}
		}
	}
}
