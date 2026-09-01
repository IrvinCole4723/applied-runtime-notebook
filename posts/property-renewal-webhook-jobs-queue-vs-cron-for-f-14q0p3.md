# Property Renewal Webhook Jobs: Queue vs Cron for Failed Retry Delivery

Short answer: use a queue as the retry state machine for a property-management renewal reminder, and use cron only for bounded maintenance such as redrive or cleanup. The worker should retry an idempotent HTTP delivery with explicit delay, then move an unresolvable item to a dead-letter queue (DLQ). A clock can start work; it should not be the place where delivery state lives.

This is a delivery-guarantee decision. A reminder sent twice is noisy, but a reminder that never reaches the property manager before a business deadline is worse. The design therefore needs a durable delivery identity, an attempt record, a next-eligible time, a maximum age, and an operator-visible terminal state.

I have been paged for missed jobs and duplicate deliveries. The alert is rarely caused by an exotic scheduler. It is usually the gap between “the worker received the item” and “the external side effect was completed.” That gap is where the runbook starts.

## Should a Node.js HTTP worker use a queue or cron for delayed webhook retries and a DLQ?

For a failed webhook job, choose the queue-shaped model. The event already exists, and its retry time belongs to that event: attempt 1 may be due now, attempt 2 later, and the final attempt may need review. A queue can carry that unit of work through ready, delayed, retried, and dead-letter states.

Cron plus a database table is viable for a small system. It is not automatically simpler. The application must claim due rows without two workers sending the same reminder, update the attempt count, calculate backoff, recover rows left in flight, and make a final decision about poison payloads. That can be the right trade when a database is already the only operational dependency and the volume is modest. It is still a queue implementation built from tables and locks.

The invariant matters more than the scheduler: a successful acknowledgement means the side effect is complete, not merely that a process fetched the record. Standard queue delivery is normally at least once, so duplicate input is part of the contract. A retry system that assumes exactly-once delivery will eventually turn a process restart into two reminders.

## The incident lesson: a business deadline needs a delivery record

For a lease-renewal reminder, store a delivery record keyed by a stable reminder ID. Include the property or lease reference, the intended deadline, the destination, the payload reference, attempt number, next retry time, and terminal reason. Do not derive identity from the current wall-clock time; a redrive must keep the same identity.

The production failure mode is bounded but familiar: the worker sends the HTTP request, the destination commits it, and the worker loses its connection before it can acknowledge the queue item. The queue makes the item visible again. The second attempt must be harmless because the receiver, or a durable ledger in front of it, recognizes the reminder ID. For example, the reminder might be scheduled for the last business day before renewal, while the first attempt times out after the property system has accepted it. A later worker sees the same reminder ID, checks the ledger, and suppresses the duplicate. If the ledger only records attempts, rather than completed side effects, the retry count looks healthy while the tenant receives two notices. That is the kind of split-brain state I want an alert to expose before an operator redrives anything.

Missed deadlines hurt.

Here is the narrow code path I want in review. `Ledger.Remember` stands for a durable, unique write; it must not be an in-memory map. The sender's contract should make the external operation idempotent or provide a receiver-side idempotency key.

```go
package main

import "fmt"

type Reminder struct {
	ID      string
	Payload []byte
}

type Ledger interface {
	// The unique key is the reminder ID, not an attempt ID.
	AlreadyCompleted(id string) (bool, error)
}

type HTTPDelivery interface {
	Send(payload []byte, idempotencyKey string) error
}

func deliver(r Reminder, ledger Ledger, sender HTTPDelivery) error {
	completed, err := ledger.AlreadyCompleted(r.ID)
	if err != nil {
		return fmt.Errorf("read delivery ledger: %w", err)
	}
	if completed {
		return nil
	}

	if err := sender.Send(r.Payload, r.ID); err != nil {
		return fmt.Errorf("send renewal reminder: %w", err)
	}

	// The queue acknowledgement belongs after this function returns nil.
	return nil
}
```

There is a subtle boundary here. If the sender succeeds but the ledger write or queue acknowledgement fails, the item may be delivered again. That is why the receiver-side key and durable lookup are more important than a clever acknowledgement setting. A transaction cannot make an arbitrary external HTTP side effect and a queue acknowledgement one atomic operation.

## How should delayed retries, HTTP workers, and a DLQ divide responsibility?

Let the queue own eligibility and redelivery. Let the worker own validation, one HTTP attempt, response classification, and metrics. Let the DLQ own work that needs a human or a deliberate replay policy. Let cron wake up maintenance; it should not run the delivery loop itself.

For each attempt, classify the result before choosing the next state. A timeout or a temporary destination response is retryable. A malformed payload, an invalid destination, or a rejected authorization state needs a distinct terminal reason rather than endless backoff. The exact status mapping belongs to the destination contract, so I would keep it in configuration and test it with recorded response classes rather than burying it in scheduler code.

Backoff needs a ceiling and a jitter rule. Without jitter, a destination outage turns thousands of due reminders into a synchronized second outage. Without a maximum age, a renewal reminder can arrive after the business decision has already been made. The policy should be written as data, for example: retry at increasing intervals, stop when the reminder expires, and send the final state to the DLQ with the original ID and the last response class.

The DLQ is not a trash can. It needs an owner, retention, a redrive permission, and a reason that can be searched. A redrive should create a new attempt for the same delivery identity, preserve the original failure history, and avoid bypassing idempotency checks. If the destination contract changed, repair the record first; pressing “retry all” is not an incident response plan.

The worker should also be boring about acknowledgements. A successful HTTP response is not enough if the response body says the request was rejected by the application. Conversely, a connection error after the destination may have accepted the request is ambiguous, so retry it under the idempotency contract. Metrics should separate accepted, retryable, terminal, duplicate-suppressed, and expired outcomes.

## When does cron remain the simplest architecture?

Cron remains a good fit for periodic work whose meaning comes from the schedule: nightly reconciliation, an hourly expiry sweep, or a bounded scan that finds reminders that need enqueueing. It can also trigger a redrive endpoint or cleanup job. The endpoint should publish due work and return; it should not process an unbounded batch inline.

The boundary is operational. A cron invocation that scans and sends every renewal reminder owns concurrency, locking, pagination, partial failure, and restart recovery all at once. A cron invocation that finds due records and hands them to workers has one clear job. That split makes missed schedules visible without making the scheduler responsible for delivery guarantees.

I would stay with cron and a table when the dataset is small, the schedule is naturally periodic, and the team is willing to own row claiming and recovery tests. I would move toward a queue when individual reminders need different delays, many workers need to share work, or an outage can create a large backlog. Your mileage may vary: the right threshold is set by the business deadline and the team's tolerance for operating another stateful component, not by a fashionable architecture diagram.

| Option | Good fit | Main operational cost | Delivery boundary |
| --- | --- | --- | --- |
| Cron plus a database table | Periodic scans and small redrive batches | Row claiming, locks, polling, and recovery state | The application owns retry timing and duplicate control. |
| Queue plus an HTTP worker | Per-reminder delay, shared workers, and backlog isolation | Queue capacity, worker concurrency, and DLQ review | At-least-once delivery still requires idempotency. |
| Queue-triggered maintenance job | Cleanup, reconciliation, or bounded replay | A separate maintenance policy and operator access | The trigger starts work; workers perform the side effect. |

This comparison is deliberately plain. A queue is not a guarantee that an external HTTP service accepted the reminder, and cron is not a guarantee that a due row was claimed. The useful choice is the one whose failure state the team can inspect and repair.

## A runbook for missed renewal reminders

Start with the delivery ID, not the tenant name. Search its attempt history, last response class, next eligible time, and terminal reason. Then answer three questions in order: was the job published, did a worker claim it, and did the destination accept the side effect?

Start there.

If publication is missing, inspect the transaction that creates the reminder and the enqueue record. If a worker claimed it but no outcome exists, inspect lease expiry and process termination. If the destination accepted it but the queue item reappeared, verify duplicate suppression before redriving. Each branch has a different repair; treating all of them as “retry” creates duplicate reminders.

Test the unhappy paths before production. Kill the worker after the HTTP response but before acknowledgement. Return a temporary failure, then make the destination healthy. Send the same delivery twice. Put a malformed payload in the DLQ. Assert that the business side effect occurs once, that retry timestamps follow policy, and that an operator can explain every terminal item from its record alone.

At the inbound boundary, authenticate webhook requests with a keyed construction appropriate to the contract. RFC 2104 describes HMAC and its security requirements; it is a standard to consult, not a substitute for checking the provider's signature format and replay rules. Log identifiers and classifications, not sensitive lease data.

The trade-off is explicit. A queue-centered design spends operational attention on queue capacity, worker concurrency, and DLQ review. A cron-and-table design spends it on locking, polling, and recovery semantics. Neither provides an absolute delivery guarantee for an arbitrary HTTP destination. The recommendation is about making the guarantee you can provide visible and testable before the renewal deadline passes.

## References

- [RFC 2104: HMAC keyed-hashing for message authentication](https://www.rfc-editor.org/rfc/rfc2104)
- [BullMQ documentation](https://docs.bullmq.io/)
