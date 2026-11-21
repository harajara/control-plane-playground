//go:build bdd

package bdd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"tenant-control-plane/internal/domain"
	"tenant-control-plane/internal/messaging"
	"tenant-control-plane/internal/secure"
)

// The broker helpers speak the production protocol: they sign with the same
// M2M_SECRET and encode with the same internal/messaging code the services
// use, so a crafted message only counts as a test input if the real contract
// accepts it.
//
// Every entry point creates its own bounded context rather than taking one
// from the caller: steps pass a context that never expires, and a test that
// cannot reach the cluster has to report that instead of blocking until the
// suite timeout.

const (
	dialTimeout  = 5 * time.Second
	brokerWindow = 40 * time.Second
	drainQuiet   = 1200 * time.Millisecond
	drainWindow  = 20 * time.Second
)

func brokerCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), brokerWindow)
}

func seedBrokers() []string {
	v := os.Getenv("KAFKA_BROKERS")
	if v == "" {
		return []string{"kafka-1:9092", "kafka-2:9092", "kafka-3:9092"}
	}
	return strings.Split(v, ",")
}

func baseOptions(extra ...kgo.Opt) []kgo.Opt {
	base := []kgo.Opt{
		kgo.SeedBrokers(seedBrokers()...),
		// A cluster that is not answering must produce an error inside the
		// call's own deadline, never a blocked test.
		kgo.DialTimeout(dialTimeout),
	}
	return append(base, extra...)
}

func brokerSigner() (*secure.Signer, error) { return secure.SignerFromEnv() }

// publishBroker opens a signing, publish-only client.
func publishBroker() (*messaging.Broker, error) {
	s, err := brokerSigner()
	if err != nil {
		return nil, err
	}
	return messaging.Open(os.Getenv("KAFKA_BROKERS"), "", nil, s)
}

func rawClient() (*kgo.Client, error) {
	return kgo.NewClient(baseOptions(kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordDeliveryTimeout(20*time.Second))...)
}

// recorded is one message on a topic, decoded when it is a valid envelope.
type recorded struct {
	Partition int32
	Offset    int64
	Envelope  messaging.Event
	Raw       []byte
}

// partitionsOf lists a topic's partitions with one metadata request, so a
// reader never has to join a consumer group.
func partitionsOf(topic string) ([]int32, error) {
	ctx, cancel := brokerCtx()
	defer cancel()
	c, err := rawClient()
	if err != nil {
		return nil, err
	}
	defer c.Close()
	req := kmsg.NewMetadataRequest()
	item := kmsg.NewMetadataRequestTopic()
	name := topic
	item.Topic = &name
	req.Topics = []kmsg.MetadataRequestTopic{item}
	resp, err := c.Request(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("metadata for %s: %w", topic, err)
	}
	md, ok := resp.(*kmsg.MetadataResponse)
	if !ok {
		return nil, fmt.Errorf("metadata for %s: unexpected response %T", topic, resp)
	}
	parts := []int32{}
	for _, t := range md.Topics {
		if t.Topic == nil || *t.Topic != topic {
			continue
		}
		if t.ErrorCode != 0 {
			return nil, fmt.Errorf("metadata for %s: kafka error %d", topic, t.ErrorCode)
		}
		for _, p := range t.Partitions {
			parts = append(parts, p.Partition)
		}
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("topic %s has no partitions", topic)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	return parts, nil
}

// readTopic drains a topic from its beginning. The dev topics stay small, so
// absolute counts are cheaper and less error-prone than offset arithmetic.
//
// A non-group consumer with explicit partition assignments is deliberate: a
// group needs a join and a rebalance before it can read anything, and several
// reads happen per scenario.
func readTopic(topic string) ([]recorded, error) {
	ctx, cancel := brokerCtx()
	defer cancel()
	s, err := brokerSigner()
	if err != nil {
		return nil, err
	}
	parts, err := partitionsOf(topic)
	if err != nil {
		return nil, err
	}
	assign := map[string]map[int32]kgo.Offset{topic: {}}
	for _, p := range parts {
		assign[topic][p] = kgo.NewOffset().AtStart()
	}
	c, err := kgo.NewClient(baseOptions(kgo.ConsumePartitions(assign))...)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	var out []recorded
	lastData := time.Now()
	deadline := time.Now().Add(drainWindow)
	for time.Now().Before(deadline) {
		// A short slice per poll: PollRecords blocks until something arrives
		// or its context ends, so one long context here would turn an empty
		// topic into a stalled test.
		pollCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
		fetches := c.PollRecords(pollCtx, 500)
		err := fetches.Err()
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			// Nothing to read in this slice, which is normal: an empty
			// partition is not an error.
			err = nil
		}
		if fetches.IsClientClosed() {
			return nil, errors.New("consumer closed")
		}
		if err != nil {
			return nil, fmt.Errorf("consume %s: %w", topic, err)
		}
		var batch []*kgo.Record
		fetches.EachError(func(t string, p int32, e error) {
			if !errors.Is(e, context.DeadlineExceeded) {
				batch = nil
				err = fmt.Errorf("consume %s/%d: %w", t, p, e)
			}
		})
		if err != nil {
			return nil, err
		}
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
		if len(batch) == 0 {
			if time.Since(lastData) > drainQuiet {
				break
			}
			continue
		}
		lastData = time.Now()
		for _, r := range batch {
			e, derr := messaging.Decode(ctx, s, r.Value)
			if errors.Is(derr, messaging.ErrPoison) {
				// Keep the position: an unreadable record is data too.
				out = append(out, recorded{Partition: r.Partition, Offset: r.Offset, Raw: r.Value})
				continue
			}
			if derr != nil {
				return nil, derr
			}
			out = append(out, recorded{Partition: r.Partition, Offset: r.Offset, Envelope: e, Raw: r.Value})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Partition != out[j].Partition {
			return out[i].Partition < out[j].Partition
		}
		return out[i].Offset < out[j].Offset
	})
	return out, nil
}

// tenantEventsOn returns decoded events for one tenant, in topic order.
func tenantEventsOn(topic, tenantID string) ([]recorded, error) {
	all, err := readTopic(topic)
	if err != nil {
		return nil, err
	}
	out := []recorded{}
	for _, e := range all {
		if e.Envelope.TenantID == tenantID {
			out = append(out, e)
		}
	}
	return out, nil
}

// commandFor finds the task command event for one task id.
func commandFor(topic, taskID string) (*recorded, error) {
	all, err := readTopic(topic)
	if err != nil {
		return nil, err
	}
	for i := range all {
		if all[i].Envelope.ID == taskID {
			return &all[i], nil
		}
	}
	return nil, fmt.Errorf("no command for task %s on %s", taskID, topic)
}

// publishResult sends a signed worker result, repeated to model redelivery.
func publishResult(r domain.Result, times int) error {
	b, err := publishBroker()
	if err != nil {
		return err
	}
	defer b.Client.Close()
	ctx, cancel := brokerCtx()
	defer cancel()
	for i := 0; i < times; i++ {
		if err := b.Result(ctx, r, "scenario"); err != nil {
			return err
		}
	}
	return nil
}

// publishResultTracked signs one result through the production code path and
// reports where the record landed, so a later step can wait until the consumer
// has read exactly that message.
func publishResultTracked(r domain.Result) (posted, error) {
	s, err := brokerSigner()
	if err != nil {
		return posted{}, err
	}
	ctx, cancel := brokerCtx()
	defer cancel()
	e := messaging.New(messaging.ResultID(r.TaskID, r.Status), "task."+r.Status, "worker/scenario", r.TenantID, r)
	raw, err := messaging.Sign(ctx, s, e)
	if err != nil {
		return posted{}, err
	}
	return publishRaw(resultsTopicName, r.TenantID, raw)
}

const resultsTopicName = "tenant.task-updates.v1"

// publishRaw produces pre-encoded bytes and returns their position.
func publishRaw(topic, key string, value []byte) (posted, error) {
	ctx, cancel := brokerCtx()
	defer cancel()
	c, err := rawClient()
	if err != nil {
		return posted{}, err
	}
	defer c.Close()
	res := c.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: value})
	if err := res.FirstErr(); err != nil {
		return posted{}, fmt.Errorf("produce %s: %w", topic, err)
	}
	rec := res[0].Record
	return posted{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset}, nil
}

// publishUnsigned sends a well-formed envelope with no token. Its shape alone
// must never be enough to move state.
func publishUnsigned(r domain.Result) error {
	e := messaging.New(messaging.ResultID(r.TaskID, r.Status), "task."+r.Status, "worker/scenario", r.TenantID, r)
	e.Token = ""
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = publishRaw(resultsTopicName, r.TenantID, raw)
	return err
}

// publishPoison writes bytes that are not an envelope, and returns them so the
// next step can find the same bytes on the dead-letter topic.
func publishPoison(topic string) ([]byte, error) {
	value := []byte(fmt.Sprintf("poison-%d-%d", time.Now().UnixNano(), os.Getpid()))
	_, err := publishRaw(topic, "", value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// redeliver re-publishes the original command bytes n extra times. This is a
// broker redelivery as a worker sees it: same key, same payload, same
// signature.
func redeliver(topic, taskID string, extra int) error {
	found, err := commandFor(topic, taskID)
	if err != nil {
		return err
	}
	ctx, cancel := brokerCtx()
	defer cancel()
	c, err := rawClient()
	if err != nil {
		return err
	}
	defer c.Close()
	batch := make([]*kgo.Record, 0, extra)
	for i := 0; i < extra; i++ {
		batch = append(batch, &kgo.Record{Topic: topic, Key: []byte(found.Envelope.TenantID), Value: found.Raw})
	}
	if err := c.ProduceSync(ctx, batch...).FirstErr(); err != nil {
		return fmt.Errorf("redeliver task %s: %w", taskID, err)
	}
	return nil
}

// dlqEntry is the dead-letter document internal/messaging writes. Value is
// the original message bytes: encoding/json renders a []byte as a base64
// string, which is exactly how it is stored, and decoding it back gives the
// bytes the rejected message had.
type dlqEntry struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Value     []byte `json:"value"`
	Code      string `json:"code"`
}

// readDLQ returns the dead-letter documents on a topic.
func readDLQ(topic string) ([]dlqEntry, error) {
	records, err := readTopic(topic)
	if err != nil {
		return nil, err
	}
	out := []dlqEntry{}
	for _, r := range records {
		var d dlqEntry
		if err := json.Unmarshal(r.Raw, &d); err != nil {
			return nil, fmt.Errorf("dead-letter record on %s is not the documented shape: %w (raw: %s)", topic, err, trim(string(r.Raw), 200))
		}
		out = append(out, d)
	}
	return out, nil
}

// dlqMentionsTenant counts dead-letter documents whose original envelope
// referenced a tenant.
func dlqMentionsTenant(topic, tenantID string) (int, error) {
	entries, err := readDLQ(topic)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, d := range entries {
		var e messaging.Event
		if json.Unmarshal(d.Value, &e) == nil && e.TenantID == tenantID {
			n++
		}
	}
	return n, nil
}

// dlqHolds reports whether a dead-letter topic carries a document whose stored
// original bytes are exactly value.
func dlqHolds(topic string, value []byte) (bool, error) {
	entries, err := readDLQ(topic)
	if err != nil {
		return false, err
	}
	for _, d := range entries {
		if string(d.Value) == string(value) {
			return true, nil
		}
	}
	return false, nil
}
