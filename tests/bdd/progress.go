//go:build bdd

package bdd

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Consumer progress. Several scenarios publish a message that must change
// nothing - a duplicate terminal update, a stale progress report, an unsigned
// envelope. "Nothing happened" is only proven once the consumer has actually
// read the message, so the suite waits on the group's committed offset instead
// of guessing with a sleep. Records for one tenant share a partition key, so
// one committed offset per partition is a total order.

type posted struct {
	Topic     string
	Partition int32
	Offset    int64
}

// waitConsumed blocks until the consumer group has committed past `through`.
func waitConsumed(topic, group string, partition int32, through int64, within time.Duration) error {
	return wait(within, func() (bool, error) {
		got, err := committedOffset(topic, group, partition)
		if err != nil {
			return false, err
		}
		return got > through, nil
	})
}

// committedOffset asks the group coordinator where one group stands on one
// partition. The partition is the one the suite just produced to, so no
// metadata discovery is needed: records for a tenant key always land on the
// same partition, which is what makes the committed offset a total order for
// the assertions that follow.
func committedOffset(topic, group string, partition int32) (int64, error) {
	ctx, cancel := brokerCtx()
	defer cancel()
	c, err := rawClient()
	if err != nil {
		return -1, err
	}
	defer c.Close()
	req := kmsg.NewOffsetFetchRequest()
	req.Group = group
	reqTopic := kmsg.NewOffsetFetchRequestTopic()
	reqTopic.Topic = topic
	reqTopic.Partitions = []int32{partition}
	req.Topics = []kmsg.OffsetFetchRequestTopic{reqTopic}
	resp, err := c.Request(ctx, &req)
	if err != nil {
		return -1, fmt.Errorf("offset fetch for group %s: %w", group, err)
	}
	out, ok := resp.(*kmsg.OffsetFetchResponse)
	if !ok {
		return -1, fmt.Errorf("offset fetch returned %T", resp)
	}
	for _, t := range out.Topics {
		if t.Topic != topic {
			continue
		}
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				return -1, fmt.Errorf("offset fetch %s/%d: kafka error %d", topic, p.Partition, p.ErrorCode)
			}
			if p.Partition != partition {
				continue
			}
			if p.Offset < 0 {
				// No committed offset yet: the group has not finished a
				// record on this partition.
				return -1, nil
			}
			return p.Offset, nil
		}
	}
	return -1, fmt.Errorf("offset fetch returned nothing for %s/%d", topic, partition)
}
