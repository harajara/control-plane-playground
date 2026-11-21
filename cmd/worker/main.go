package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"
	"tenant-control-plane/internal/applog"
	"tenant-control-plane/internal/domain"
	"tenant-control-plane/internal/messaging"
	"tenant-control-plane/internal/secretstore"
	"tenant-control-plane/internal/secure"
	"tenant-control-plane/internal/worker"
)

func envFloat(k string, d float64) float64 {
	v, e := strconv.ParseFloat(os.Getenv(k), 64)
	if e == nil {
		return v
	}
	return d
}
func main() {
	applog.Init()
	if err := run(); err != nil {
		slog.Error("worker stopped", "error_type", fmt.Sprintf("%T", err))
		os.Exit(1)
	}
}
func run() error {
	idDefault, _ := os.Hostname()
	id := flag.String("worker-id", idDefault, "Worker identity")
	fail := flag.Float64("fail-rate", envFloat("WORKER_FAIL_RATE", 0), "Injected failure probability (0–1)")
	min := flag.Int("min-delay-ms", int(envFloat("WORKER_MIN_DELAY_MS", 500)), "Minimum work delay")
	max := flag.Int("max-delay-ms", int(envFloat("WORKER_MAX_DELAY_MS", 2000)), "Maximum work delay")
	flag.Parse()
	if *fail < 0 || *fail > 1 || *min < 0 || *max < *min {
		return fmt.Errorf("invalid worker options")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// The tenant resource server's administrator URL comes from the worker's
	// own environment, never from the control-plane database.
	adminURL := os.Getenv("TENANT_DB_ADMIN_URL")
	if adminURL == "" {
		return fmt.Errorf("TENANT_DB_ADMIN_URL is required; run task setup")
	}
	signer, err := secure.SignerFromEnv()
	if err != nil {
		return err
	}
	opener, err := secure.OpenerFromEnv()
	if err != nil {
		return err
	}
	secrets, err := secretstore.FromEnv()
	if err != nil {
		return err
	}
	b, err := messaging.Open(os.Getenv("KAFKA_BROKERS"), "workers", []string{messaging.Commands}, signer)
	if err != nil {
		return err
	}
	defer b.Client.Close()
	w := &worker.Worker{ID: *id, AdminURL: adminURL, Secrets: secrets, Opener: opener, Broker: b, FailRate: *fail, MinDelay: time.Duration(*min) * time.Millisecond, MaxDelay: time.Duration(*max) * time.Millisecond}
	if err = w.Init(ctx); err != nil {
		return err
	}
	defer w.Pool.Close()
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			h := domain.Worker{ID: *id, LastSeen: domain.Now(), Online: true}
			hbCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := b.Send(hbCtx, messaging.Heartbeats, *id, messaging.New(uuid.NewString(), "worker.heartbeat", "worker/"+*id, "", h))
			cancel()
			if err == nil {
				_ = os.WriteFile("/tmp/worker-heartbeat", []byte(time.Now().Format(time.RFC3339)), 0600)
			} else if ctx.Err() == nil {
				slog.Warn("heartbeat publish failed", "error_type", fmt.Sprintf("%T", err))
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	slog.Info("worker started", "id", *id, "fail_rate", *fail)
	b.Run(ctx, w.Handle)
	return nil
}
