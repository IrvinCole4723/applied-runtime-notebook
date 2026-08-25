# GDPR Right to Be Forgotten Explained: EU Application Logging Risks and Retention

**Short answer:** Treat application logs as a searchable personal-data store whenever an entry can be tied back to a person. For an EU startup rolling out an edtech pricing rule behind a flag, the rollback-safe pattern is to prevent direct identifiers from entering logs, keep a pseudonymous subject reference only where deletion requires it, split short-lived diagnostic events from longer-lived aggregate evidence, and make erasure a tested, idempotent workflow. Retention alone won't satisfy a right-to-erasure request if copies remain in archives, exports, or incident bundles.

Rollbacks don't erase logs.

The uncomfortable trade-off is that more history can make a rollback investigation easier, while the same history raises exposure and deletion cost. Keep the decision evidence, not the learner's identity. A useful pricing event says which rule version and flag variant ran, whether evaluation succeeded, and which trace carried the request. It doesn't need an email address, full request body, access token, or free-form support note.

I ran into missed jobs and duplicate deliveries while operating production queues. An erasure pipeline has the same failure shape: a missed task leaves data behind; a duplicate task must be harmless. That's why a retention document is not enough. The control has to survive retries, partial progress, and a rollback of the application release that created the records.

## How should an EU startup delete user data from application logs?

Start by mapping where a subject-linked event can travel. The obvious destination is the primary log index. The less obvious destinations are hot-to-cold tier transitions, object-storage archives, copied incident excerpts, analytics exports, dead-letter queues, local debug downloads, and backups. A delete request that touches only the search index creates a clean dashboard and an incomplete erasure.

Under the GDPR, data minimization and storage limitation are principles in Article 5, while Article 17 describes the right to erasure and its exceptions. Those exceptions matter: erasure is not an unconditional command to destroy every record regardless of another legal obligation. The operational conclusion is narrower and more useful. Classify each log field and copy by purpose, document the applicable retention or exception, and involve qualified counsel in the legal determination. This engineering note can't decide the lawful basis for a particular startup.

Use a data-flow inventory with an owner and a deletion capability for every destination. A small system can track four classes without buying a governance platform:

| Data class | Example from the pricing rollout | Default handling | Erasure behavior |
| --- | --- | --- | --- |
| Direct identifier | Email, account ID, IP address | Exclude or redact before emission | Delete any exceptional occurrence and investigate the source |
| Pseudonymous event | Subject token, rule version, flag variant | Short diagnostic retention with access controls | Locate by subject token and delete from mutable stores |
| Aggregate signal | Count of evaluations by rule version | Store without a subject dimension | Usually outside a subject lookup when re-identification is not reasonably possible |
| Required record | A record retained for a documented obligation | Isolate from general observability access | Follow the approved exception and schedule, not the diagnostic-log policy |

Pseudonymization reduces casual exposure; it does not automatically turn personal data into anonymous data. If the service can reconnect a token to a learner, operate it as protected data. Keep the re-identification secret or mapping outside the log system, restrict access, and rotate it under a documented plan. Don't place the mapping in the same archive as the events.

## The incident lesson is about retries, not a retention number

Imagine the bounded failure during a flagged pricing rollout. Version v2 writes learner IDs into a free-form message, the release is rolled back, and the old binary resumes cleanly. Operationally, the feature rollback worked. Privacy cleanup did not: v2 records can still sit in the active index, an export created for an incident review, and a cold archive. Restoring the old binary cannot retract bytes already emitted.

This is where queue discipline earns its keep. Give each erasure request a stable request ID, derive the subject token through the same versioned lookup used at write time, and fan out one task per destination. A worker records a completion marker for the tuple `(request ID, destination, policy version)`. Retrying a completed tuple returns success without repeating destructive work; retrying an incomplete tuple continues from the destination's own cursor. Do not mark the parent request complete until every in-scope destination has reported a durable result.

Make the state visible.

The runbook should distinguish queued, running, completed, excepted, and failed destination tasks; it should page on age and repeated failure rather than raw queue depth. A deletion service must also handle records that arrive late from buffers after the first sweep. One practical design runs a second lookup after the maximum documented ingestion delay, then places a subject tombstone at the ingestion boundary for the life of any remaining buffer. The tombstone is a control against re-entry, not evidence that deletion happened elsewhere.

I'm not sure a universal count of retention days would help here. Your mileage may vary because debugging windows, contractual duties, backup mechanics, and legal obligations differ. Pick periods through a recorded purpose-and-risk review, then test enforcement. For example, a team may choose a 24-hour diagnostic tier for a narrowly scoped rollout rehearsal, but that is an illustrative engineering choice, not a GDPR safe harbor.

## Preserve rollback evidence without preserving identity

A rollback decision needs enough evidence to compare the flagged path with the control path. It rarely needs the fields engineers first reach for under pressure. Record a stable event name, schema version, pricing-rule version, flag variant, coarse result, and trace identifier. Put business totals in a purpose-built ledger if they must be authoritative; logs are append-oriented diagnostic evidence and can be sampled, delayed, duplicated, or deleted.

Sampling is a separate control. OpenTelemetry describes head sampling as a decision made early and tail sampling as a decision made after more of a trace is available. Neither mechanism is a substitute for log retention or erasure. Sampling can lower collection volume and exposure, but a sampled event that contains personal data still needs the same classification and lifecycle controls. Zero collection is stronger.

Metrics offer a better rollback signal when the question is aggregate: evaluation error rate by rule version, assignment count by variant, or latency distribution. Keep subject-level troubleshooting in a short-lived tier. This separation lets an on-call engineer decide whether to disable `pricing_rule_v2` without reading a learner's identity or restoring a deleted log archive.

The preventative Go path below emits an allowlisted event. The HMAC creates a stable lookup value for an erasure sweep without exposing the source identifier in the log line. The secret must come from a managed secret store, stay outside telemetry, and have a version that the deletion registry retains for as long as matching events can exist.

```go
package pricinglog

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"
)

type Decision struct {
	UserID      string
	RuleVersion string
	FlagVariant string
	Result      string
	TraceID     string
}

type event struct {
	Name         string    `json:"event"`
	Schema       int       `json:"schema"`
	OccurredAt   time.Time `json:"occurred_at"`
	SubjectRef   string    `json:"subject_ref"`
	RuleVersion  string    `json:"rule_version"`
	FlagVariant  string    `json:"flag_variant"`
	Result       string    `json:"result"`
	TraceID      string    `json:"trace_id"`
}

func WriteDecision(w io.Writer, secret []byte, now time.Time, d Decision) error {
	if len(secret) < 32 {
		return errors.New("subject token secret is too short")
	}
	if d.UserID == "" || d.RuleVersion == "" || d.TraceID == "" {
		return errors.New("missing required decision field")
	}
	if d.Result != "applied" && d.Result != "control" && d.Result != "rejected" {
		return errors.New("invalid decision result")
	}

	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(d.UserID))

	record := event{
		Name:        "pricing_rule_evaluated",
		Schema:      1,
		OccurredAt:  now.UTC(),
		SubjectRef:  hex.EncodeToString(mac.Sum(nil)),
		RuleVersion: d.RuleVersion,
		FlagVariant: d.FlagVariant,
		Result:      d.Result,
		TraceID:     d.TraceID,
	}

	return json.NewEncoder(w).Encode(record)
}
```

This function deliberately doesn't accept an email, request body, displayed price, or error string. That narrow type is a guardrail — a generic `map[string]any` would let the next incident quietly undo the policy. Production code should also bound field lengths, control writer access, and test that rejected inputs never leak through error reporting.

## Make deletion a release-tested capability

A policy is credible only if the system can demonstrate it. In staging, create synthetic subjects, send their events through every configured tier, submit one erasure request twice, and verify both runs converge on the same empty subject lookup. Then replay a delayed event and confirm the ingestion tombstone rejects or strips the subject-linked record. Test a policy-version change as well; old token versions have to remain discoverable until their last possible copy expires or is erased.

Restore tests matter.

Backups need an explicit restoration rule. Editing an immutable backup in place may be impractical or conflict with its security purpose. Document its restricted access and expiry, prevent routine searches against it, and ensure a restore process reapplies outstanding erasure tombstones before the recovered system serves normal traffic. The catch is that a tombstone registry can itself be personal data, so minimize its fields, isolate it, and give it a lifecycle tied to the last recoverable copy. Legal review should confirm the treatment.

Measure the control without rebuilding a people index in metrics. Useful service-level indicators include oldest pending request age, destinations awaiting completion, deletion retries by reason, late events blocked, and restore drills that reapplied tombstones. Keep request IDs out of low-cardinality metrics; put detailed correlations in an access-controlled audit record with its own retention decision.

Rollout order matters. Ship the allowlist and destination inventory before enabling the pricing flag. Next, dry-run subject lookups and compare counts without deleting. Exercise idempotent deletion with synthetic data. Only then enable real erasure, and require the privacy workflow to pass before increasing the flag percentage. If the pricing release rolls back, leave the privacy worker and schema readers forward-compatible so they can still interpret and remove v2 events.

## When is this design not a good fit?

A stable pseudonymous subject reference is not a good fit when the service has no valid need for subject-level troubleshooting. Omit the reference and keep aggregate signals instead. It also falls short when a log backend cannot reliably locate and delete matching records: use a shorter-lived, deletion-capable diagnostic store, route sensitive workflows to a purpose-built record system, or redesign the event so it contains no subject link. Encrypting an immutable archive does not provide selective erasure by itself.

There is another boundary. If fraud review, tax records, litigation holds, or safety investigations impose a separate retention duty, don't make an SRE invent the exception during an incident. Separate those records from general application logging, restrict their use, assign an accountable owner, and apply the approved schedule. Stick with a simple time-based expiration policy only when no subject-linked data enters the stream and restore paths cannot reintroduce it.

The durable decision rule is plain: prevent identifiers first; use a deletable subject reference only for a documented operational purpose; preserve rollback evidence as aggregates and versioned outcomes; and treat retries, archives, and restores as part of erasure. Logs are a system, not exhaust.

## Sources

- https://eur-lex.europa.eu/eli/reg/2016/679/oj
- https://www.edpb.europa.eu/sites/default/files/files/file1/edpb_guidelines_201904_dataprotection_by_design_and_by_default_v2.0_en.pdf
- https://opentelemetry.io/docs/concepts/sampling/
- https://opentelemetry.io/docs/specs/otel/logs/data-model/
- https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
