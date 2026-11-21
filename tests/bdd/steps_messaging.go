//go:build bdd

// Hand-written implementations of the steps in features/messaging.feature.
// Function names and argument lists come from godog's generated stubs.

package bdd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"tenant-control-plane/internal/domain"
	"tenant-control-plane/internal/messaging"
)

const (
	commandsTopic = "tenant.tasks.v1"
	resultsTopic  = "tenant.task-updates.v1"
	dlqSuffix     = ".dlq"
)

// exactlyOneTaskdeployEventForTheTenantAppearsOnAfterCommit counts the command
// events this scenario's tenant has on the topic. Publication happens after the
// database commit, so exactly one is the outbox contract.
func exactlyOneTaskdeployEventForTheTenantAppearsOnAfterCommit(ctx context.Context, topic string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.lastTask("deploy")
	if err != nil {
		return err
	}
	var seen []recorded
	err = wait(30*time.Second, func() (bool, error) {
		events, err := tenantEventsOn(topic, t.ID)
		if err != nil {
			return false, err
		}
		deploy := []recorded{}
		for _, e := range events {
			if e.Envelope.Type == "task."+task.Type {
				deploy = append(deploy, e)
			}
		}
		seen = deploy
		return len(deploy) >= 1, nil
	})
	if err != nil {
		return fmt.Errorf("no %s event for tenant %s on %s: %w", task.Type, t.ID, topic, err)
	}
	if len(seen) != 1 {
		return fmt.Errorf("%d events for task type %s on %s, wanted exactly 1", len(seen), task.Type, topic)
	}
	s.recordedEvent = &seen[0]
	s.eventCount[topic] = len(seen)
	return nil
}

// theEventCarriesTheTaskIdTypeTenantIdAndStatus inspects the command payload,
// which is the task itself: the task is the event.
func theEventCarriesTheTaskIdTypeTenantIdAndStatus(ctx context.Context, kind, status string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	if s.recordedEvent == nil {
		return fmt.Errorf("no event was captured by a previous step")
	}
	e := s.recordedEvent.Envelope
	if e.Type != "task."+kind {
		return fmt.Errorf("event type is %q, wanted task.%s", e.Type, kind)
	}
	if e.TenantID != t.ID {
		return fmt.Errorf("event tenant is %s, wanted %s", e.TenantID, t.ID)
	}
	cmd, err := commandOf(s.recordedEvent)
	if err != nil {
		return err
	}
	if cmd.Task.ID != t.Tasks[kind].ID {
		return fmt.Errorf("event carries task %s, wanted %s", cmd.Task.ID, t.Tasks[kind].ID)
	}
	if cmd.Task.Status != status {
		return fmt.Errorf("task in the event is %q, wanted %q", cmd.Task.Status, status)
	}
	if cmd.Slug != t.Slug {
		return fmt.Errorf("event slug is %q, wanted %q", cmd.Slug, t.Slug)
	}
	if cmd.SecretID != infraID(t.Slug) {
		return fmt.Errorf("event secret id is %q, wanted %q", cmd.SecretID, infraID(t.Slug))
	}
	return nil
}

// theEventIdEqualsTheTaskId is the command idempotency rule.
func theEventIdEqualsTheTaskId(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	if s.recordedEvent == nil {
		return fmt.Errorf("no event was captured by a previous step")
	}
	cmd, err := commandOf(s.recordedEvent)
	if err != nil {
		return err
	}
	if s.recordedEvent.Envelope.ID != cmd.Task.ID {
		return fmt.Errorf("event id %s is not the task id %s", s.recordedEvent.Envelope.ID, cmd.Task.ID)
	}
	if _, ok := t.Tasks[cmd.Task.Type]; !ok {
		return fmt.Errorf("event refers to task type %s the scenario never saw", cmd.Task.Type)
	}
	return nil
}

// workersAreStopped really stops the three worker containers.
func workersAreStopped(ctx context.Context) error {
	return stopWorkers()
}

// iResumeWorkers starts them again and waits for their heartbeats, so a later
// step cannot race a container that is still booting.
func iResumeWorkers(ctx context.Context) error {
	return startWorkers()
}

// noAdditionalEventWasPublishedOn compares against the count the previous step
// recorded. A rolled-back request must not have produced a task row, so there
// must be nothing new to publish.
func noAdditionalEventWasPublishedOn(ctx context.Context, topic string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	before, ok := s.eventCount[topic]
	if !ok {
		return fmt.Errorf("no baseline was recorded on %s; the fixture step that stuck a task must count first", topic)
	}
	events, err := tenantEventsOn(topic, t.ID)
	if err != nil {
		return err
	}
	if len(events) != before {
		return fmt.Errorf("%d events on %s for tenant %s, was %d before the rejected request", len(events), topic, t.ID, before)
	}
	// Give the sweeper a full recovery window: an unpublished task would show
	// up within five seconds, and five is precisely what the request rolled
	// back.
	time.Sleep(6 * time.Second)
	after, err := tenantEventsOn(topic, t.ID)
	if err != nil {
		return err
	}
	if len(after) != before {
		return fmt.Errorf("the sweeper published %d extra event(s) for a rolled-back request", len(after)-before)
	}
	s.eventCount[topic] = len(after)
	return nil
}

// theFastPublishPathIsDisabled turns off only the in-request publish, so the
// five-second sweeper becomes the only path and can be measured.
func theFastPublishPathIsDisabled(ctx context.Context) error {
	return setFastPublish("false")
}

// theDeployTaskIsPublishedWithinSeconds watches the topic for the command.
func theDeployTaskIsPublishedWithinSeconds(ctx context.Context, seconds int) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.lastTask("deploy")
	if err != nil {
		return err
	}
	err = wait(time.Duration(seconds)*time.Second, func() (bool, error) {
		_, err := commandFor(commandsTopic, task.ID)
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("the sweeper did not publish task %s within %d seconds: %w", task.ID, seconds, err)
	}
	return nil
}

// iPublishTheSameTaskdoneUpdateForTheTenantTwice sends one logical result
// twice. The result id is derived from task and status, so both messages are
// the same event as far as the consumer is concerned.
func iPublishTheSameTaskdoneUpdateForTheTenantTwice(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.lastTask("deploy")
	if err != nil {
		return err
	}
	v, err := s.api.Tenant(t.ID)
	if err != nil {
		return err
	}
	s.versionBefore = v.Version
	r := domain.Result{TaskID: task.ID, TenantID: t.ID, Type: task.Type, Status: "done"}
	if err := publishAndSettle(r, 2); err != nil {
		return err

	}
	return nil
}

// iPublishATaskin_ProgressUpdateForTheDoneDeployTask is the stale report:
// progress for a task that finished long ago.
func iPublishATaskin_ProgressUpdateForTheDoneDeployTask(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.lastTask("deploy")
	if err != nil {
		return err
	}
	return publishAndSettle(domain.Result{TaskID: task.ID, TenantID: t.ID, Type: task.Type, Status: "in_progress"}, 1)
}

// iPublishATaskdoneUpdateForTheAcceptedDeployTask is the out-of-order case:
// the terminal result arrives while the task is still accepted.
func iPublishATaskdoneUpdateForTheAcceptedDeployTask(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.openTask()
	if err != nil {
		return err
	}
	if task.Status != "accepted" {
		return fmt.Errorf("the scenario expected an accepted deploy task, found %q", task.Status)
	}
	r := domain.Result{TaskID: task.ID, TenantID: t.ID, Type: task.Type, Status: "done"}
	posted, err := publishResultTracked(r)
	if err != nil {
		return err
	}
	// The control plane must buffer it, not apply it: the task stays accepted.
	if err := waitConsumed(posted.Topic, "control-plane", posted.Partition, posted.Offset, 30*time.Second); err != nil {
		return err
	}
	v, err := s.api.Task(task.ID)
	if err != nil {
		return err
	}
	if v.Status != "accepted" {
		return fmt.Errorf("a terminal result applied out of order: task is %q", v.Status)
	}
	return nil
}

// iPublishATaskin_ProgressUpdateForTheAcceptedDeployTask releases the buffered
// terminal result.
func iPublishATaskin_ProgressUpdateForTheAcceptedDeployTask(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.openTask()
	if err != nil {
		return err
	}
	r := domain.Result{TaskID: task.ID, TenantID: t.ID, Type: task.Type, Status: "in_progress"}
	posted, err := publishResultTracked(r)
	if err != nil {
		return err
	}
	if err := waitConsumed(posted.Topic, "control-plane", posted.Partition, posted.Offset, 30*time.Second); err != nil {
		return err
	}
	copied := *task
	copied.Status = "in_progress"
	t.Tasks[task.Type] = &copied
	return nil
}

// iPublishAnUndecodableMessageTo writes bytes that are not an envelope.
func iPublishAnUndecodableMessageTo(ctx context.Context, topic string) error {
	s, err := getState()
	if err != nil {
		return err
	}
	value, err := publishPoison(topic)
	if err != nil {
		return err
	}
	s.poisonPayload = value
	return nil
}

// theMessageAppearsOnWithinSeconds looks for the exact bytes the previous step
// published, on the topic named in the feature file.
func theMessageAppearsOnWithinSeconds(ctx context.Context, topic string, seconds int) error {
	s, err := getState()
	if err != nil {
		return err
	}
	value := s.poisonPayload
	if value == nil {
		value = s.unsignedPayload
	}
	if value == nil {
		return fmt.Errorf("nothing was published to look for")
	}
	return wait(time.Duration(seconds)*time.Second, func() (bool, error) {
		found, err := dlqHolds(topic, value)
		if err != nil {
			return false, err
		}
		return found, nil
	})
}

// theConsumerIsStillProcessingSubsequentValidUpdates proves the dead-letter
// path did not stop the consumer: after the poison, a real tenant still walks
// its lifecycle through the same topic.
func theConsumerIsStillProcessingSubsequentValidUpdates(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := createTenantNow(s, "after-poison", "After poison tenant")
	if err != nil {
		return err
	}
	if _, err := s.api.WaitTenant(t.ID, 3*time.Minute, "active", "failed"); err != nil {
		return err
	}
	v, err := s.api.Tenant(t.ID)
	if err != nil {
		return err
	}
	if v.Status != "active" {
		return fmt.Errorf("the consumer stalled after a poison message: tenant ended %q", v.Status)
	}
	return nil
}

// iRedeliverTheDeployTaskEventTwiceAndResumeWorkers replays the original
// command bytes and then lets real workers at it.
func iRedeliverTheDeployTaskEventTwiceAndResumeWorkers(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.lastTask("deploy")
	if err != nil {
		return err
	}
	if err := redeliver(commandsTopic, task.ID, 2); err != nil {
		return err
	}
	return startWorkers()
}

// iPublishAnUnsignedTaskdoneUpdateForATenant publishes a correctly shaped,
// correctly typed result with no authentic signature.
func iPublishAnUnsignedTaskdoneUpdateForATenant(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := ensureActiveTenant(s, "unsigned-tenant")
	if err != nil {
		return err
	}
	task, err := t.lastTask("deploy")
	if err != nil {
		return err
	}
	r := domain.Result{TaskID: task.ID, TenantID: t.ID, Type: task.Type, Status: "done"}
	e := messaging.New(messaging.ResultID(r.TaskID, r.Status), "task."+r.Status, "worker/scenario", r.TenantID, r)
	e.Token = ""
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	s.unsignedPayload = raw
	posted, err := publishRaw(resultsTopic, r.TenantID, raw)
	if err != nil {
		return err
	}
	s.dlqTopic = resultsTopic + dlqSuffix
	s.lastPosted = &posted
	return nil
}

// theMessageIsRejectedToTheDeadletterTopic asserts both that the message was
// dead-lettered and that it changed nothing.
func theMessageIsRejectedToTheDeadletterTopic(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	if s.lastPosted == nil || len(s.unsignedPayload) == 0 {
		return fmt.Errorf("no unsigned message was published")
	}
	if err := waitConsumed(s.lastPosted.Topic, "control-plane", s.lastPosted.Partition, s.lastPosted.Offset, 30*time.Second); err != nil {
		return fmt.Errorf("the consumer never read the unsigned message: %w", err)
	}
	found, err := dlqHolds(s.dlqTopic, s.unsignedPayload)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("the unsigned message was not on %s", s.dlqTopic)
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	v, err := s.api.Tenant(t.ID)
	if err != nil {
		return err
	}
	if v.Status != "active" {
		return fmt.Errorf("an unsigned message changed the tenant to %q", v.Status)
	}
	return nil
}

// twoPublishedUpdatesWithTheSameTaskAndStatusShareOneId is the idempotency
// key contract: derived identity, not a random one.
func twoPublishedUpdatesWithTheSameTaskAndStatusShareOneId(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	task, err := t.lastTask("deploy")
	if err != nil {
		return err
	}
	r := domain.Result{TaskID: task.ID, TenantID: t.ID, Type: task.Type, Status: "done"}
	if err := publishResult(r, 2); err != nil {
		return err
	}
	events, err := tenantEventsOn(resultsTopic, t.ID)
	if err != nil {
		return err
	}
	ids := []string{}
	for _, e := range events {
		if e.Envelope.Type != "task.done" {
			continue
		}
		var got domain.Result
		if json.Unmarshal(e.Envelope.Data, &got) != nil || got.TaskID != task.ID {
			continue
		}
		ids = append(ids, e.Envelope.ID)
	}
	if len(ids) < 2 {
		return fmt.Errorf("only %d done update(s) are on the topic for task %s", len(ids), task.ID)
	}
	want := messaging.ResultID(task.ID, "done")
	for _, id := range ids {
		if id != want {
			return fmt.Errorf("done update id %s is not the derived id %s", id, want)
		}
	}
	return nil
}

// nothingWasRoutedToTheDeadletterTopic is the negative assertion for the
// duplicate and stale cases: dedupe is normal work, not poison.
func nothingWasRoutedToTheDeadletterTopic(ctx context.Context) error {
	s, err := getState()
	if err != nil {
		return err
	}
	t, err := s.primaryTenant()
	if err != nil {
		return err
	}
	n, err := dlqMentionsTenant(resultsTopic+dlqSuffix, t.ID)
	if err != nil {
		return err
	}
	if n != 0 {
		return fmt.Errorf("%d message(s) about tenant %s were dead-lettered", n, t.ID)
	}
	return nil
}

// publishAndSettle sends a result and waits until the control-plane consumer
// has committed past it, so the next assertion reads the settled state.
func publishAndSettle(r domain.Result, times int) error {
	posted, err := publishResultTracked(r)
	if err != nil {
		return err
	}
	if times > 1 {
		for i := 1; i < times; i++ {
			if _, err := publishResultTracked(r); err != nil {
				return err
			}
		}
	}
	return waitConsumed(posted.Topic, "control-plane", posted.Partition, posted.Offset+int64(times-1), 30*time.Second)
}

// commandOf decodes a command event's payload.
func commandOf(e *recorded) (domain.Command, error) {
	var c domain.Command
	if err := json.Unmarshal(e.Envelope.Data, &c); err != nil {
		return c, fmt.Errorf("command payload is not the documented shape: %w", err)
	}
	return c, nil
}
