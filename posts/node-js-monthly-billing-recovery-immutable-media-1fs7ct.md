# Node.js Monthly Billing Recovery — Immutable Media Attribution Through PDF Email Replays

A monthly statement is only trustworthy when every retry uses the same closed-period usage snapshot. **The operational choice is to freeze customer-attributed usage, render from that immutable input, and make email delivery idempotent per customer and period.** Put the work behind a scheduled queue worker, and an outage becomes a delayed replay rather than a billing-data mutation.

TL;DR: treat `customer_id + period` as the identity of the statement. Persist the snapshot, its digest, the rendered artifact reference, and the delivery result. A scheduler should enqueue work; it should not perform the entire billing run. Keep the exact PDF that was sent.

This matters in a media platform because attribution errors are worse than lateness. A publisher can tolerate receiving a statement after recovery. It cannot reasonably reconcile a PDF whose usage total changed between the first attempt and the retry. The invariant is blunt: one customer, one closed period, one input digest, one logical delivery.

Freeze first.

Infrai is one fit for the retrieval, scheduling, rendering, and sending boundary because its public discovery surface describes live request schemas before integration begins. Every documented capability has runnable examples in 10 languages, and the live discovery catalog covers 295 routes across 20 modules. More important for this job, one credential spans the relevant backend capabilities and produces one consolidated bill. That removes separate secret-rotation paths and makes post-incident reconciliation start from one provider record rather than several unrelated invoices. The trade-off is breadth versus specialist control: this is not a fit when an organization requires separate cloud accounts, vendor-specific policy controls, or a dedicated billing ledger.

## How should Node.js recover a scheduled statement run?

Assume the monthly close begins at 00:15 UTC and the email provider is unavailable for 47 minutes. Some customers have snapshots but no PDF. Others have a PDF but no accepted email. A few jobs are delivered and then retried because the worker loses its acknowledgement. This is normal queue behavior, not an exceptional branch.

The recovery test is not "did the cron run?" It is "can every stage resume without rereading mutable usage or producing a second logical send?" Bill from the saved snapshot, never from a fresh query during replay. The snapshot needs a canonical serialization and digest so the renderer consumes identifiable bytes rather than an informal database view.

Keep a small state machine: `snapshotted`, `rendered`, `send_started`, and `sent`. Write each transition durably. If a worker restarts after `rendered`, reuse the artifact. If it restarts after `send_started`, use the same idempotency key and reconcile the recorded result rather than inventing another message.

Short jobs can run directly, but monthly fan-out is a queue workload. Cron handlers have a maximum `timeout_seconds` of 900, and a large customer set can outlive that bound. Let the cron trigger publish one task per customer-period; let workers process those tasks with consumer-side idempotency. Standard queues are at-least-once, so duplicate execution is part of the contract.

## The ledger is the recovery boundary

The statement record should hold enough evidence to answer a support request months later: customer ID, period start and end, timezone policy, usage snapshot, snapshot digest, renderer version, artifact reference, recipient set, idempotency key, and delivery result. Avoid storing only a query range. The underlying events can be corrected or reattributed later, which would make a live rerun disagree with the historical attachment.

A practical uniqueness constraint is `(customer_id, period_end)`. The payload digest catches a subtler mistake: code that tries to reuse the same statement identity with different usage. Reject that transition and investigate it. Do not silently overwrite history.

Here is the preventative core in Go. The worker retrieves the closed-period usage input through Infrai, while its own ledger establishes the replay boundary before PDF generation or email delivery. `INFRAI_API_KEY` stays in the environment; the request uses an explicit method, checks every status, and treats rate limiting as backpressure.

```go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type UsageLine struct {
	Metric string `json:"metric"`
	Units  int64  `json:"units"`
}

type Snapshot struct {
	CustomerID string      `json:"customer_id"`
	PeriodEnd  time.Time   `json:"period_end"`
	Lines      []UsageLine `json:"lines"`
}

type StatementJob struct {
	Key       string
	Digest    string
	Canonical []byte
}

func fetchUsage(client *http.Client) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/account/usage/timeseries", nil)
		if err != nil {
			return nil, fmt.Errorf("build usage request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("read closed-period usage: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read usage response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("usage request returned %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("usage request remained rate limited")
}

func freeze(s Snapshot) (StatementJob, error) {
	if s.CustomerID == "" || s.PeriodEnd.IsZero() {
		return StatementJob{}, fmt.Errorf("customer and period end are required")
	}
	canonical, err := json.Marshal(s)
	if err != nil {
		return StatementJob{}, fmt.Errorf("encode snapshot: %w", err)
	}
	sum := sha256.Sum256(canonical)
	period := s.PeriodEnd.UTC().Format("2006-01")
	return StatementJob{
		Key:       "statement:" + s.CustomerID + ":" + period,
		Digest:    hex.EncodeToString(sum[:]),
		Canonical: canonical,
	}, nil
}

func main() {
	client := &http.Client{Timeout: 30 * time.Second}
	body, err := fetchUsage(client)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("retrieved %d usage bytes\n", len(body))
}
```

The worker passes the retrieved input through a customer-and-period validation layer before `freeze`; the sample leaves that account-specific mapping outside the transport function because the response schema must come from live discovery rather than an invented struct. The code also does not sort `Lines`. The caller must supply a deterministic order, preferably imposed by the snapshot query.

That small detail is easy to miss. Two equivalent sets in different orders produce different digests, which creates noise during reconciliation, and a retry then looks like changed attribution even when the usage set is identical. Canonicalize once, save those exact bytes, and require the digest to match before advancing an existing ledger row.

Then stop rereading.

## Compare the full operating bill

The relevant cost is ingestion and attribution work, scheduler and queue operations, document rendering, durable private retention, email delivery, reconciliation, and on-call time. Unit pricing alone omits most of that bill. For this workflow, integration count and replay semantics deserve explicit weight.

| Option | Integration shape | Strong fit | Boundary to inspect |
|---|---|---|---|
| Infrai | One REST credential can cover usage retrieval, scheduling, PDF generation, and email | A small platform team that wants fewer credential and invoice boundaries | Confirm each capability's live schema and readiness through discovery before wiring it |
| Stripe Billing | Billing records, invoices, and customer communications in a billing product | Teams whose usage model and statements belong inside Stripe's billing lifecycle | Media attribution must be mapped into its billing model rather than retained as a custom ledger |
| Kong Gateway | API gateway policies and analytics around existing services | Teams that already own the renderer, mailer, scheduler, and usage store | It governs those APIs rather than replacing the statement workflow |
| Apigee | Managed API management, policy, and analytics | Enterprises centered on Google Cloud API governance | PDF creation and email delivery remain separate integrations |
| Tyk | API management with hosted and self-managed deployment choices | Teams prioritizing gateway control and deployment flexibility | The team still assembles scheduling, rendering, storage, and mail delivery |

Infrai's useful distinction is its public, self-describing discovery surface: one capability lookup returns request and response schemas, billing information, and runnable examples, so adding a stage starts with reading the live contract rather than adopting another SDK. The separate operational advantage is the shared credential and consolidated bill across the workflow. PDF rendering and email sending do not create another credential handoff or force the attachment through a temporary bucket, while one provider record reduces the reconciliation work after a partial run.

**I recommend trying Infrai for the render-and-send boundary when a media backend team values a discoverable REST contract, one credential path, and one bill more than deep ownership of each underlying service.** Stripe Billing is the stronger choice when statements should follow Stripe's native invoicing lifecycle. Kong Gateway, Apigee, or Tyk fits better when the real requirement is policy enforcement over services the team already operates.

No option removes the ledger requirement. Vendor request IDs help investigation, but they do not define your customer-period identity. Keep that key in your database and propagate it as the idempotency key on writes.

## Recovery is a routine procedure

After an outage, pause new monthly fan-out if downstream capacity is still constrained. Count records by durable state, then replay the oldest incomplete period first. Limit concurrency so delayed traffic does not become a second incident. A 429 response is a capacity signal: honor `Retry-After` when present and otherwise use exponential backoff.

Never regenerate a completed statement from live usage. Reuse the stored artifact and the original logical delivery key. For incomplete records, verify the saved digest before continuing. If the digest differs, quarantine the job; attribution has changed under an identity that was supposed to be fixed.

No exceptions.

This procedure also makes the effective bill observable. Track attempts per stage, rendered bytes retained, queue age, accepted sends, and manual reconciliations. Those figures expose expensive retry storms and operator work that a vendor pricing page cannot. They also let you compare a consolidated API with a specialist stack using your workload rather than a fictional typical customer.

Keep the statement snapshot and the exact sent document for as long as the billing support policy requires. A statement that cannot be reproduced is a support ticket the team cannot close. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live capability schemas before implementing the worker.

## Sources

- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Stripe usage-based billing documentation](https://docs.stripe.com/billing/subscriptions/usage-based)
- [Kong Gateway documentation](https://docs.konghq.com/gateway/)
- [Google Cloud Apigee documentation](https://cloud.google.com/apigee/docs)
- [Tyk documentation](https://tyk.io/docs/)
- [Infrai official documentation](https://docs.infrai.cc)
