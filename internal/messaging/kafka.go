package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"tenant-control-plane/internal/domain"
)

type Broker struct {
	Client *kgo.Client
	Signer Signer
}

func Open(brokers, group string, topics []string, signer Signer) (*Broker, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordDeliveryTimeout(15 * time.Second)}
	if group != "" {
		opts = append(opts, kgo.ConsumerGroup(group), kgo.ConsumeTopics(topics...), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	}
	c, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Broker{c, signer}, nil
}
func (b *Broker) Send(ctx context.Context, topic, key string, e Event) error {
	raw, err := Sign(ctx, b.Signer, e)
	if err != nil {
		return err
	}
	return b.Client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: raw}).FirstErr()
}
func (b *Broker) Command(ctx context.Context, c domain.Command) error {
	return b.Send(ctx, Commands, c.Task.TenantID, New(c.Task.ID, "task."+c.Task.Type, "control-plane", c.Task.TenantID, c))
}
func (b *Broker) Result(ctx context.Context, r domain.Result, worker string) error {
	return b.Send(ctx, Results, r.TenantID, New(ResultID(r.TaskID, r.Status), "task."+r.Status, "worker/"+worker, r.TenantID, r))
}
func Backoff(ctx context.Context, attempt int) bool {
	if attempt > 8 {
		attempt = 8
	}
	d := 100 * time.Millisecond * time.Duration(1<<attempt)
	d += time.Duration(rand.IntN(100)) * time.Millisecond
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (b *Broker) Run(ctx context.Context, handle func(context.Context, Event) error) {
	for ctx.Err() == nil {
		fetches := b.Client.PollRecords(ctx, 1)
		if fetches.IsClientClosed() {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			slog.Warn("broker poll failed; retrying", "count", len(errs))
			b.Client.AllowRebalance()
			if !Backoff(ctx, 2) {
				return
			}
			continue
		}
		fetches.EachRecord(func(r *kgo.Record) {
			for attempt := 0; ctx.Err() == nil; attempt++ {
				e, err := Decode(ctx, b.Signer, r.Value)
				if err == nil {
					err = handle(ctx, e)
				}
				if errors.Is(err, ErrPoison) {
					// Original ciphertext is retained, never copied into logs or the UI.
					d, _ := json.Marshal(map[string]any{"topic": r.Topic, "partition": r.Partition, "offset": r.Offset, "value": r.Value, "code": "invalid_event"})
					err = b.Client.ProduceSync(ctx, &kgo.Record{Topic: r.Topic + ".dlq", Key: r.Key, Value: d}).FirstErr()
				}
				if err == nil {
					err = b.Client.CommitRecords(ctx, r)
				}
				if err == nil {
					break
				}
				slog.Warn("message processing will retry", "topic", r.Topic, "attempt", attempt)
				if !Backoff(ctx, attempt) {
					break
				}
			}
		})
		b.Client.AllowRebalance()
	}
}
