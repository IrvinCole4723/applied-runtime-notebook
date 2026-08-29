# Node.js Scheduled Data Cleanup: Cron Trigger Queues for Old Postgres Logs

**Short answer:** schedule a short Node.js dispatch, put one bounded renewal-reminder batch on a queue, and let an idempotent worker perform the PostgreSQL read and write. Treat recovery, rather than timer precision, as the primary design constraint.

This is the pattern I use when a healthtech system must delay a renewal reminder until a business deadline. The scheduler should not own the reminder. It should create recoverable work. I've been paged by missed jobs and duplicate deliveries; the operational difference is whether either event produces a clear next action.

## What data must the deadline record prove?

Store the business deadline on the subscription or renewal record, together with the reminder policy version. A cron trigger can ask for records whose deadline has arrived, but it should not be the only record of what was due: schedules drift, deployments pause them, and an invocation can be accepted while downstream work is still waiting. The dispatch query needs a stable ordering and a finite limit, while each batch should carry IDs, the policy version, and the deadline used for selection rather than the full patient or account payload. The database remains the source of truth, and the worker rechecks eligibility before sending anything. That recheck matters because a renewal can be cancelled, put on hold, or moved under a legal or business rule after the queue message was created; the worker should turn an ineligible item into a recorded no-op, not send an obsolete reminder because an old message arrived late. For old-log deletion, the same rule applies: preserve the original retention cutoff and ID window in the batch, then re-evaluate those predicates during the retry. A batch of 100 or 500 records is a starting point for a load test, not a universal setting. The right value is the largest unit that can be retried without creating unacceptable lock, latency, or notification pressure, and that value must be measured against the normal query paths sharing the PostgreSQL primary rather than chosen from a queue dashboard.

Missed work is expected.

## How should Node.js cron, queue batches, and PostgreSQL workers recover missed cleanup-style jobs?

Use the Node.js process as a dispatcher: claim a bounded set of due records, create deterministic work IDs, and publish descriptors. The worker owns the transaction. A descriptor can look like this:

```go
package reminders

import "fmt"

type Batch struct {
	ID       string
	StartID  int64
	EndID    int64
	Deadline string
}

func ValidateBatch(b Batch) error {
	if b.ID == "" || b.StartID < 1 || b.EndID <= b.StartID || b.Deadline == "" {
		return fmt.Errorf("invalid reminder batch")
	}
	return nil
}
```

The worker opens a transaction, locks or claims only the selected range, and checks each row's current state. It inserts a delivery key such as `renewal:<record-id>:<policy-version>` into a unique table before the external send. If that key already exists, the delivery is a no-op. Commit the database state before acknowledging the queue message.

There is an unavoidable boundary around an external notification: a process can send the message and die before its database commit, or commit and die before acknowledgement. Exactly-once behavior across two independent systems is not a scheduling feature. The practical control is an idempotency key accepted by the notification side, or an outbox whose sender has the same deduplication contract.

A duplicate delivery is normal input. So is an empty batch after another worker finished it.

For deletion of old logs, use the same shape but a narrower predicate: delete only the recorded ID window and rows older than the original retention cutoff. Do not use a newly calculated cutoff during a retry; otherwise one message can mutate a wider slice of data than the dispatcher intended. The delete is successful when the transaction commits, including when the affected-row count is zero.

## Test duplicate deliveries before production

Acknowledgement belongs after the transaction, never before it. Queue systems that use explicit consumer acknowledgements document this ordering because it is what allows an unacknowledged delivery to be retried. A retry must be bounded: malformed descriptors should move to a dead-letter path with the error and policy version attached, rather than consuming the same worker forever.

The runbook should answer five questions during an incident: how many deadlines are overdue, how old the oldest one is, how many batches are in flight, how many deliveries were deduplicated, and what happened to the last failed batch. A timer-success metric answers none of these.

For rollback, stop new dispatches first. Let in-flight workers finish or expire under their normal lease, then inspect the outbox and delivery-key table before replaying anything. If a policy change made the selected set wrong, roll back the policy version and leave already-sent notifications governed by their recorded delivery keys. Do not delete the queue to make the alert quiet.

Test the unpleasant sequence. Force a worker to exit after the notification call, replay the same descriptor, and verify that the recipient gets one reminder. Force a database error before commit and verify that a retry can process the same range. Run the dispatcher twice at once and confirm that the claim or unique constraint prevents two independent sends.

I also check the HTTP result from the scheduler call: a `2xx` response means only that the dispatcher accepted the request if that is the documented contract. It is not evidence that the batch was delivered or completed. Your mileage may vary with the queue's lease and retry settings, so those settings belong in the runbook and in a failure-injection test.

## Rollout: paused dispatch recovery

This design fits a deadline-driven reminder or retention sweep where each item can be processed independently and replay is safe. It is not suitable when the work requires a long dependency graph, global ordering, or a coordinated fan-out and fan-in result. Use a workflow engine for that shape, or keep the simpler queue and worker when the real requirement is bounded retries around independent rows.

The catch is that a queue does not supply a retention policy, a consent decision, or a notification idempotency contract. Those remain application responsibilities. If a healthtech team cannot explain how a legal hold suppresses a reminder, adding another scheduler will only make the failure harder to inspect.

| Decision | Prefer the simpler scheduled worker when | Change the design when |
| --- | --- | --- |
| Trigger | A missed run can be recovered by querying due rows | Each trigger carries an irreplaceable event that needs durable replay |
| Work unit | Rows are independently eligible and bounded by ID or deadline | Steps depend on one another or require global ordering |
| Delivery | The sender accepts an idempotency key or an outbox is available | The external side effect cannot be deduplicated and duplicates are unsafe |
| Recovery | An operator can pause dispatch and replay a descriptor | Recovery needs a long-lived, human-visible workflow |

The operational recommendation is therefore narrow: keep the cron trigger boring, make each queue message finite, and put the invariant in PostgreSQL and the sender contract. Select a different orchestration boundary when those invariants cannot be made explicit.

## References

- https://www.rabbitmq.com/docs/confirms
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
