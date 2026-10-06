# Owning Web App SMS Notification Service Templates (Without Webhook Status Events)

Short answer: keep signup-verification copy and link generation inside the application boundary, then give the SMS service a rendered message plus an opaque attempt ID. For US and EU batch invitations without webhooks, persist every recipient as a small state machine, poll with bounded backoff, and apply suppressions before each send attempt. The least complex workable design is a database-backed worker, not a second workflow engine.

The page fires at 09:12: `signup_verification_completion_rate` has fallen below its rolling baseline for 15 minutes. On-call sees 1,840 invitations accepted by the batch API, 1,803 still marked `submitted`, and no transport callback to explain the gap. The customer-facing symptom is simpler: a verification link never arrives, so an invited administrator cannot finish creating an account.

Do not page on `submitted` volume. Page on verification outcomes, and expose transport age as the earlier warning. A provider accepting a request proves only that it accepted the request; it does not prove handset delivery or account verification.

Accepted is not delivered.

## How should a web app batch SMS notification service expose status?

The leading signal is the age distribution of nonterminal attempts. Track `submitted_at`, `last_polled_at`, `next_poll_at`, and the latest normalized status for each recipient. A rising p95 age for `submitted` records should open a low-urgency alert before the user-outcome page fires. It is closer to the fault and leaves time to investigate.

Three counters make the first five minutes of triage useful: attempts created, attempts entering a terminal delivery state, and verification tokens redeemed. Split them by destination region, message template version, and batch ID. Do not put phone numbers, tokens, or full message bodies in metric labels.

I treat the page as an instruction to reconcile, not an instruction to resend. Duplicate verification messages confuse users, and a newer link can invalidate the context they are opening. The idempotency key should therefore be derived from the account, verification purpose, and logical invitation version. A worker retry reuses that key; a deliberate new invitation increments the version.

The polling loop needs a deadline. Poll quickly enough to catch an ordinary transition, then back off with jitter and stop after the product's verification window closes. There is no universal interval: documented rate limits, batch size, status semantics, and the business deadline determine it. Measure poll lag separately from provider-reported status age so an overloaded worker cannot masquerade as a slow carrier.

This design has a real limitation. Polling spends request capacity when nothing changes and detects transitions later than a healthy push channel can. It is a reasonable trade-off when webhooks are unavailable and the batch deadline tolerates that delay; it is the wrong choice for sub-second decisions, very large status fan-out, or a transport whose lookup interface cannot return stable per-message state. In those cases, choose a transport with authenticated event delivery or place a queue consumer at the boundary. Do not compensate by polling faster than documented limits.

## Keep template ownership at the application boundary

Template ownership is the primary design decision because the text contains product meaning: who is signing up, why the recipient received the message, and where the verification URL leads. The application should own that meaning, version it with the release, and render it before enqueueing transport work. The transport adapter should own encoding, submission, status lookup, and normalization.

This boundary also keeps regional policy explicit. A suppression record belongs to the application's communication policy, even if a transport maintains its own suppression mechanism. Check both where available, but persist the application decision and reason locally. Evaluate a batch recipient by recipient immediately before submission; checking once when the batch is created leaves a race between a consent change and a delayed worker.

Verification is security-sensitive. NIST's digital identity guidance says out-of-band secrets should be single use and completed within 10 minutes, while treating the public switched telephone network as a restricted authenticator channel. That does not make every SMS verification link a NIST authenticator, but it warns against long-lived, reusable links. Bind the token to one purpose, store only a digest, expire it, and record redemption atomically.

Keep the message transactional and specific. The FTC's CAN-SPAM guidance covers commercial email rather than SMS, so it should not be stretched into an SMS compliance claim. For US messaging, consult FCC rules and counsel; for EU processing, document the lawful basis, retention, and data-processing roles under the GDPR. Engineering controls support that review. They do not replace it.

Policy stays explicit.

## A poller that cannot accidentally send twice

The focused Go example claims due records with a lease, checks suppression after the claim, and submits with a stable key. Storage and transport remain interfaces because their guarantees and status vocabulary must be tested, not hidden in an SDK.

```go
package verification

import (
    "context"
    "time"
)

type Attempt struct {
    ID, Destination, RenderedBody, IdempotencyKey string
}

type Store interface {
    ClaimDue(context.Context, int, time.Duration) ([]Attempt, error)
    IsSuppressed(context.Context, string) (bool, error)
    MarkSuppressed(context.Context, string) error
    MarkSubmitted(context.Context, string, string, time.Time) error
    Release(context.Context, string, time.Time) error
}

type Transport interface {
    Submit(context.Context, string, string, string) (string, error)
}

func Dispatch(ctx context.Context, now time.Time, store Store, tx Transport) error {
    attempts, err := store.ClaimDue(ctx, 100, 30*time.Second)
    if err != nil { return err }
    for _, a := range attempts {
        blocked, err := store.IsSuppressed(ctx, a.Destination)
        if err != nil {
            _ = store.Release(ctx, a.ID, now.Add(time.Minute))
            continue
        }
        if blocked {
            _ = store.MarkSuppressed(ctx, a.ID)
            continue
        }
        remoteID, err := tx.Submit(ctx, a.Destination, a.RenderedBody, a.IdempotencyKey)
        if err != nil {
            _ = store.Release(ctx, a.ID, now.Add(time.Minute))
            continue
        }
        if err := store.MarkSubmitted(ctx, a.ID, remoteID, now.Add(30*time.Second)); err != nil {
            return err
        }
    }
    return nil
}
```

A production implementation must close one uncomfortable gap: submission may succeed while `MarkSubmitted` fails. The stable idempotency key lets a retry converge if the transport honors idempotent submission. If it does not, store an outbox record transactionally and reconcile uncertain submissions before retrying. Classify that state as `unknown`, not `failed`.

Preserve the raw transport status and timestamp, then map them into a small internal vocabulary such as `submitted`, `delivered`, `undeliverable`, `suppressed`, and `unknown`. Never translate an unfamiliar value into success. Polling should stop only on a documented terminal value or the local deadline.

Unknown means stop and inspect.

## Deploy the instrumentation before the page

Ship state recording first, then dashboards, then alerts. For one release, compare the new leading signal with verification redemption without paging anyone. This shadow period exposes ordinary batch shapes, quiet hours, regional differences, and polling backlog behavior.

The deployment test should exercise a suppression inserted after batch creation, two workers racing for one record, a successful submission followed by a database timeout, an unknown remote status, and expiry before delivery. The invariant is sharper than a happy-path assertion: no logical invitation may produce more than one submission key, and no suppressed destination may be submitted after its suppression becomes effective.

During an incident, query by batch ID and template version first. If nonterminal age rises in one region while the poll scheduler remains current, inspect transport status and regional routing. If scheduler lag rises everywhere, restore polling capacity before increasing send throughput. If delivery looks normal but redemption falls, inspect token validation, link construction, and the signup endpoint. The transport is only one suspect.

This is why template version belongs in every attempt record. A bad link host or an overlong localized body can affect one release without implicating polling. Roll back by creating a new version; do not mutate historical bodies and erase reconciliation evidence.

Resending is not recovery.

## Choose the threshold by its interruption cost

Close the loop with two alerts. The early alert watches sustained nonterminal age while confirming that enough attempts exist to make the signal meaningful. The page watches the user outcome: verification completion relative to an established baseline, with batch and region context attached. Both need a runbook query and an owner.

A threshold that is too tight pages on ordinary carrier delay, large scheduled batches, and low-volume statistical noise. Each unnecessary page trains the responder to wait, which is the wrong reflex when a real verification outage starts. A threshold that is too loose moves detection to support tickets. Tune from observed distributions, review it after major traffic or routing changes, and record the reason for every adjustment.

The durable decision rule is straightforward: own product language and suppression policy in the application; require stable submission identity and queryable status from the transport; and alert first on aging work, then on failed user outcomes. Polling can be operationally sound without webhooks, but only when uncertainty is represented as state rather than treated as permission to send again.

Restraint is part of reliability.

## Further reading

- https://pages.nist.gov/800-63-4/sp800-63b.html
- https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
- https://www.fcc.gov/rules-political-campaign-calls-and-texts
- https://eur-lex.europa.eu/eli/reg/2016/679/oj
- https://www.rfc-editor.org/rfc/rfc3986
