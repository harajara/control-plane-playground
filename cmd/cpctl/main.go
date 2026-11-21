// cpctl emits broker messages for the documented failure, replay and
// dead-letter scenarios. Signing comes from M2M_SECRET in the environment, so
// a message published after a restart is still accepted.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"tenant-control-plane/internal/applog"
	"tenant-control-plane/internal/domain"
	"tenant-control-plane/internal/messaging"
	"tenant-control-plane/internal/secure"
)

func main() {
	applog.Init()
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	task := flag.String("task", "", "task UUID")
	tenantID := flag.String("tenant", "", "tenant UUID")
	kind := flag.String("type", "deploy", "deploy/update/destroy")
	status := flag.String("status", "done", "in_progress/done/failed")
	repeat := flag.Int("repeat", 1, "duplicate count")
	poison := flag.Bool("poison", false, "publish undecodable bytes")
	unsigned := flag.Bool("unsigned", false, "publish a well-formed envelope with no signature")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if *poison {
		return raw(ctx, []byte("invalid-json"), messaging.Results, "")
	}
	if *task == "" || *tenantID == "" {
		return fmt.Errorf("--task and --tenant are required (or use --poison)")
	}
	r := domain.Result{TaskID: *task, TenantID: *tenantID, Type: *kind, Status: *status}
	if *unsigned {
		// Shaped exactly like a real result, and the consumer still refuses it:
		// an unauthenticated envelope cannot move state.
		e := messaging.New(messaging.ResultID(r.TaskID, r.Status), "task."+r.Status, "worker/scenario", r.TenantID, r)
		e.Token = ""
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		return raw(ctx, b, messaging.Results, r.TenantID)
	}
	signer, err := secure.SignerFromEnv()
	if err != nil {
		return err
	}
	b, err := messaging.Open(os.Getenv("KAFKA_BROKERS"), "", nil, signer)
	if err != nil {
		return err
	}
	defer b.Client.Close()
	for i := 0; i < *repeat; i++ {
		if err := b.Result(ctx, r, "scenario"); err != nil {
			return err
		}
	}
	fmt.Printf("published %d %s update(s) for task %s\n", *repeat, r.Status, r.TaskID)
	return nil
}

// raw produces bytes with no signing key at all.
func raw(ctx context.Context, value []byte, topic, key string) error {
	c, err := kgo.NewClient(kgo.SeedBrokers(seedBrokers()...), kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return err
	}
	defer c.Close()
	return c.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: value}).FirstErr()
}

func seedBrokers() []string {
	v := os.Getenv("KAFKA_BROKERS")
	if v == "" {
		return []string{"kafka-1:9092", "kafka-2:9092"}
	}
	return strings.Split(v, ",")
}
