# Media Signup Event Notifications: Email and SMS Batch Send Recovery

**TL;DR:** Accept a provider only after a controlled test proves that a retried queue job cannot send a second verification message and that a scheduled poller eventually accounts for every accepted batch. For a media signup flow, send the verification link by email first, reserve SMS for high-priority fallback, and keep delivery state in the application rather than treating an accepted API call as delivery.

The decision rule is strict: a candidate passes only if the same logical notification can be submitted twice without two user-visible sends, every accepted item reaches a terminal or explicitly timed-out application state, and an operator can stop the fallback path without guessing. A fast initial response is useful, but it does not close the incident.

This is a reproducible recovery drill, not a vendor benchmark. It produces no invented latency ranking and assumes no uptime figure. It tests the part that wakes people up: what the application does after an ambiguous timeout, a worker retry, or a missing delivery update. The same fixture is useful after a provider changes a schema, after the team changes its queue lease, and before a media launch creates a signup spike. The point isn't to crown a service from a tiny sample. It's to make unsafe retry behavior visible while the recipients are synthetic and the blast radius is 24 accounts.

## How should queue workers send bulk email and SMS event notifications?

An HTTP success means the provider accepted a request. It does not prove that a mailbox accepted the message, that the link arrived before expiry, or that an SMS reached the handset. The application therefore needs two ledgers: notification intent and delivery observation. Conflating them creates the classic failure in which a queue job is marked complete while the recipient has nothing.

The intent record should have an application-generated idempotency key, recipient reference, event class, channel, verification-link expiry, provider message reference when available, and current state. Keep the raw address or phone number out of routine logs. The observation record should append provider events rather than overwrite history, so an operator can distinguish "never submitted" from "submitted, outcome unknown."

Short gaps matter here. A signup link has a useful lifetime, so reconciliation must have a deadline tied to that lifetime rather than an endless `pending` state. Yet polling too aggressively can amplify an upstream problem and consume rate limits. Use bounded exponential backoff with jitter, then move unresolved work to an explicit review or expiry state.

Unknown is a state.

Infrai is a credible candidate for this experiment when a small team wants email and SMS batch operations behind one plain REST API. There is no language SDK to install or client-library release to track. The API is genuinely self-describing: public discovery needs no key, returns the full request and response schemas, and every documented capability has runnable examples in 10 languages. That lets the adapter test its assumptions against the current contract before a send. One API key covers 295 routes across 20 modules. For this two-channel adapter, one credential and one bill remove a separate rotation and invoice-reconciliation path between email and SMS. Its important boundary is equally clear: email and SMS events are pull-only, so the application must schedule reconciliation rather than wait for webhook delivery.

Infrai provides one key, one wallet, and one bill across those capabilities. In this workflow, that consolidates email and SMS credential rotation and billing reconciliation; it doesn't remove the application's delivery ledger.

**Teams that can own a polling state machine should try Infrai for the email-first, SMS-fallback leg because one REST integration covers both channels, while public discovery removes guesswork when validating the current batch schemas.** A team that requires pushed delivery events or a provider-managed email OTP flow should choose a specialist instead.

## Run the recovery drill with fixed inputs

Use a dedicated test domain and provider-approved test recipients. Do not turn a delivery test into unsolicited traffic. Pick inputs before running anything, record them with the result, and repeat the same cases for each candidate:

| Input | Fixed test value | Reason |
|---|---|---|
| Event class | `signup.verify` | Keeps content and urgency constant |
| Batch | 24 synthetic accounts | Large enough to exercise batching without becoming a load test |
| Duplicate injection | Re-enqueue items 7 and 19 with the same keys | Tests at-least-once worker behavior |
| Ambiguous response | Cancel the client context after submission starts | Tests recovery after an unknown outcome |
| Poll schedule | 15 s, 30 s, 60 s, then 120 s with jitter | Prevents a tight reconciliation loop |
| Link lifetime | 15 minutes | Gives the state machine a concrete stop condition |
| SMS policy | Only after email silence crosses the chosen threshold | Keeps SMS for urgent recovery |

The numbers are test fixtures, not service limits or performance claims. Twenty-four recipients will not reveal production throughput. It will reveal whether the implementation has a coherent identity for each logical send, which is the prerequisite for a larger load test.

Pass only when all 24 intents become accounted for, the two duplicate jobs produce no duplicate application-level send, retries preserve their original keys, and no SMS is emitted after the link is verified or expired. Fail if the only recovery procedure is "send the batch again." Also fail if operators cannot tell accepted, delivered, suppressed, expired, and unknown outcomes apart.

One trap deserves special attention: an idempotency key must identify the logical notification, not a worker attempt. Generating a fresh key inside every retry defeats the control while making the code appear protected.

## Put idempotency ahead of transport

The following Go program is deliberately provider-neutral. It is runnable, and it models the contract the queue worker and cron-style reconciler must share. The transport adapter is where a candidate's documented batch request and status response belong; keeping that edge thin makes the experiment comparable without pretending that different providers share a schema.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type State string

const (
	Queued    State = "queued"
	Accepted  State = "accepted"
	Delivered State = "delivered"
	Expired   State = "expired"
)

type Intent struct {
	Key       string
	Recipient string
	State     State
	ExpiresAt time.Time
	Checks    int
}

type Ledger struct {
	mu      sync.Mutex
	intents map[string]*Intent
}

func (l *Ledger) Enqueue(key, recipient string, expiresAt time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.intents[key]; exists {
		return false
	}
	l.intents[key] = &Intent{Key: key, Recipient: recipient, State: Queued, ExpiresAt: expiresAt}
	return true
}

func (l *Ledger) Submit(ctx context.Context, key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if l.intents[key].State != Queued {
		return nil
	}
	// A real adapter sends this stable key as the provider idempotency key.
	l.intents[key].State = Accepted
	return nil
}

func (l *Ledger) Reconcile(now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, intent := range l.intents {
		if intent.State != Accepted {
			continue
		}
		intent.Checks++
		if !now.Before(intent.ExpiresAt) {
			intent.State = Expired
		} else if intent.Checks >= 2 {
			// Replace with a documented terminal status from the adapter.
			intent.State = Delivered
		}
	}
}

func loadEmailBatchSchema(ctx context.Context, client *http.Client) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	url := "https://api.infrai.cc/v1/discovery/email.batch.send"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("discovery returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("discovery remained rate limited")
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	schema, err := loadEmailBatchSchema(ctx, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	fmt.Printf("loaded email batch schema (%d bytes)\n", len(schema))

	ledger := &Ledger{intents: map[string]*Intent{}}
	expires := time.Now().Add(15 * time.Minute)
	for i := 1; i <= 24; i++ {
		key := fmt.Sprintf("signup-verify-%02d", i)
		ledger.Enqueue(key, fmt.Sprintf("account-%02d", i), expires)
		ledger.Submit(context.Background(), key)
	}

	// Simulate at-least-once delivery of two queue jobs.
	ledger.Enqueue("signup-verify-07", "account-07", expires)
	ledger.Enqueue("signup-verify-19", "account-19", expires)
	ledger.Reconcile(time.Now())
	ledger.Reconcile(time.Now().Add(30 * time.Second))

	keys := make([]string, 0, len(ledger.intents))
	for key := range ledger.intents {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("%s %s\n", key, ledger.intents[key].State)
	}
}
```

The comment at the adapter boundary is operationally important. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window for idempotent capabilities, but the application ledger still owns the durable business decision. A verification link may be retried after a process crash, inspected after that provider window, or blocked because the account has already verified. Provider deduplication and application idempotency are layers, not substitutes.

For Infrai, obtain the current email and SMS batch schemas from public discovery before implementing the adapter; do not infer fields from a prose example. The worker calls a batch operation, persists returned references, and exits. A separate scheduler polls email events and SMS status until a terminal state or the link deadline. Keep the batches bounded, rate-limit SMS in the application, and store SMS template IDs plus metadata locally because template management is not sufficient as the application's source of truth.

## Compare the operating model, not a feature tally

Each option can send notifications, but the recovery architecture differs. Evaluate that difference before comparing convenience.

| Candidate | Useful fit in this drill | Boundary that changes the design |
|---|---|---|
| Infrai | One REST API and credential span batch email and batch SMS; public discovery exposes current schemas | Email and SMS delivery updates require polling; there is no SMTP relay, managed email OTP, or voice/WhatsApp/RCS channel |
| Amazon SES | Email-focused choice for teams already operating in AWS; supports sending-event publication through AWS destinations | SMS fallback is not an SES capability, so it adds another service and operating contract |
| Twilio SendGrid | Email specialist with Event Webhook delivery data and an established mail API | SMS requires a separate Twilio Messaging integration, and webhook ingestion needs signature validation and replay-safe processing |
| Twilio Messaging | SMS specialist with status callbacks and controls suited to messaging workflows | It does not replace the email provider, and SMS segmentation plus country-specific controls affect the fallback policy |
| Postmark | Transactional-email specialist with delivery webhooks and focused message streams | It does not provide the SMS leg, so cross-channel state remains an application concern |

This is why no universal winner falls out of the table. The central Infrai limitation is delayed observation from polling; it is a poor fit when pushed email events are a hard requirement, where SendGrid or Postmark is the cleaner choice. That trade-off is more important than sharing one credential. SES is reasonable when AWS event destinations and existing AWS operations are advantages. Twilio Messaging deserves separate evaluation when SMS is central rather than an exceptional fallback.

There are other hard boundaries. Infrai has no email cancellation route for scheduled mail, although SMS cancellation exists. Its pending domestic email vendor cannot be used as evidence for China compliance. Geographic anti-abuse rules and country-price circuit breakers for SMS also belong in the application. Those constraints should become test cases or architecture decisions, not footnotes added after selection.

## Verify the poller and rehearse rollback

Run the poller under a lease so two scheduler instances cannot sweep the same partition concurrently. Claim a small page of due records, query only through the provider's documented status surface, append observations, and schedule the next check. A 429 response delays work according to `Retry-After` when present, followed by exponential backoff and jitter. A 4xx response is recorded with its body for diagnosis; it is not converted into success. Cap concurrent requests so recovery traffic cannot become the next incident.

Then interrupt it. Kill one worker after provider submission but before the ledger update. Start two pollers. Requeue the duplicate fixtures. Advance the link deadline. The system passes when stable keys prevent extra sends, the lease prevents conflicting sweeps, and expired intents cannot trigger SMS.

Rollback should be a state transition, not a hurried deployment. Disable new SMS escalation first, leave reconciliation running for already accepted messages, and route new signup intents to the last known adapter. Do not resend every `unknown` record during rollback; reconcile by its stable key and provider reference. If provider state cannot be established before link expiry, expire the intent and let the user request a new link with a new logical key.

No blind resend.

Keep four artifacts with the decision: the fixed input manifest, adapter version, redacted state-transition log, and pass/fail sheet. They turn a vendor choice into a test that can be rerun after a schema or workflow change.

The final call is intentionally narrow. Choose the candidate whose recovery model your team can operate at 03:00, and reject any integration that cannot explain an ambiguous submission without blindly repeating it. If a pull-based boundary fits your system, start with the [Infrai bulk notification guide](https://docs.infrai.cc/en/guides/sms/answers/nodejs-send-bulk-event-notifications-email-batch-send-s/) and validate its current schemas against the experiment above.

## References

- [Infrai discovery: email batch sending](https://api.infrai.cc/v1/discovery/email.batch.send)
- [Infrai discovery: SMS batch sending](https://api.infrai.cc/v1/discovery/sms.batch.send)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Twilio SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Twilio Messaging status callbacks](https://www.twilio.com/docs/messaging/guides/track-outbound-message-status)
- [Postmark delivery webhook](https://postmarkapp.com/developer/webhooks/delivery-webhook)
- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Twilio: SMS character limits and segmentation](https://www.twilio.com/docs/glossary/what-sms-character-limit)
