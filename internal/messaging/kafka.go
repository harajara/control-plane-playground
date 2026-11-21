package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
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

// A record that keeps failing is retried in place with capped backoff for
// inPlaceRetries attempts (~2.5 minutes), then parked: the exact bytes go to
// the tail of the same partition, tagged with an attempt count, and the
// original offset commits so the partition moves again. A parked record that
// still fails after maxParks rounds is dead-lettered with code
// retry_exhausted. Parking is safe because the state machine never queues a
// second command for a tenant behind an unprocessed one, and the control
// plane buffers or dedupes results by task id, so log order cannot corrupt
// state - only delay it.
const (
	inPlaceRetries = 12
	maxParks       = 20
	attemptsKey    = "cp-attempts"
)

func parkCount(r *kgo.Record) int {
	for _, h := range r.Headers {
		if h.Key == attemptsKey {
			n, _ := strconv.Atoi(string(h.Value))
			return n
		}
	}
	return 0
}

// dlq preserves the original ciphertext as evidence and never copies it into
// logs. A failed DLQ write returns an error, so the offset cannot advance.
func (b *Broker) dlq(ctx context.Context, r *kgo.Record, code string) error {
	d, _ := json.Marshal(map[string]any{"topic": r.Topic, "partition": r.Partition, "offset": r.Offset, "value": r.Value, "code": code})
	return b.Client.ProduceSync(ctx, &kgo.Record{Topic: r.Topic + ".dlq", Key: r.Key, Value: d}).FirstErr()
}

func (b *Broker) Run(ctx context.Context, handle func(context.Context, Event) error) {
	for ctx.Err() == nil {
		fetches := b.Client.PollRecords(ctx, 1)
		if fetches.IsClientClosed() {
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			slog.Warn("broker poll failed; retrying", "count", len(errs), "error_type", fmt.Sprintf("%T", errs[0].Err))
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
				switch {
				case errors.Is(err, ErrPoison):
					err = b.dlq(ctx, r, "invalid_event")
				case err != nil && attempt >= inPlaceRetries:
					if n := parkCount(r); n >= maxParks {
						err = b.dlq(ctx, r, "retry_exhausted")
					} else if perr := b.Client.ProduceSync(ctx, &kgo.Record{
						Topic: r.Topic, Key: r.Key, Value: r.Value,
						Headers: []kgo.RecordHeader{{Key: attemptsKey, Value: []byte(strconv.Itoa(n + 1))}},
					}).FirstErr(); perr == nil {
						slog.Warn("message parked at partition tail", "topic", r.Topic, "attempts", n+1)
						err = nil
					}
				}
				if err == nil {
					err = b.Client.CommitRecords(ctx, r)
				}
				if err == nil {
					break
				}
				slog.Warn("message processing will retry", "topic", r.Topic, "attempt", attempt, "error_type", fmt.Sprintf("%T", err))
				if !Backoff(ctx, attempt) {
					break
				}
			}
		})
		b.Client.AllowRebalance()
	}
}
