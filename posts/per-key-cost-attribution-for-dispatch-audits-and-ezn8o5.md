# Per-Key Cost Attribution for Dispatch Audits (and the Hard Boundary)

Short answer: use application-level tags for the operational view, then retain a per-key dimension in the ledger when access auditability matters. One label rarely answers both “which service spent the money?” and “which credential made the call?”

At 02:17, a logistics dispatch service can be healthy while its bill quietly climbs. The page arrives later, after a carrier event replay or a queue consumer retry has multiplied calls. The first useful question is not “which team owns the bill?” It is “which credential authorized each request, and which application was that credential serving?”

That distinction changes the instrumentation design. A key is an access fact. An application tag is an operational grouping. Treating them as interchangeable makes the postmortem shorter and the audit trail weaker.

## The alert-to-audit path

The alert should fire on a signal that survives an outage: request count, retry rate, queue age, and spend grouped by a stable service identity. A raw account total is too late. A key-only alert is noisy when one credential is shared by several workers.

Work backward from the page. The request record needs a timestamp, provider or capability, application identity, environment, queue or job identifier, credential identifier, and outcome. Do not store the secret itself. OWASP recommends treating secrets as sensitive lifecycle objects, with controlled access and rotation; a non-secret key fingerprint or vault reference is enough to join usage to an audit record.

The useful join is deliberately boring:

```go
type UsageRecord struct {
	Time         time.Time
	Application  string
	Environment  string
	Capability   string
	KeyRef       string // non-secret fingerprint or vault reference
	JobID        string
	RequestCount int
	CostMicros   int64
	Outcome      string
}
```

If the dispatch API times out, the retry must carry the same job identifier and an idempotency key. Otherwise the cost stream can look like demand when it is actually duplicate delivery. I've been paged for missed jobs and duplicate deliveries, and the dashboard often pointed at the wrong layer: a carrier integration looked expensive when the real problem was a consumer that acknowledged a message after sending, then sent it again after a lease expired. In one investigation, the raw events showed the same job identifier crossing two workers within a lease window, while the application rollup hid that sequence. The evidence was there, but only at request level, so the fix had to be tested against delivery semantics rather than a pricing query.

That is the line.

The instrumentation change is to emit one immutable usage event per attempt and derive rollups from it. Keep the raw event long enough to reconstruct a billing dispute; aggregate views can expire sooner. This is where a per-key field earns its storage cost: it lets an auditor prove which access path was used without pretending that a credential is an application boundary.

Then check the threshold. A low spend threshold pages on a scheduled batch and trains people to ignore the next alert. A high threshold misses a runaway retry loop. Start with a baseline by application and capability, review false positives after two weeks, and record the reason for every threshold change.

## How should cost attribution and instrumentation granularity work together?

Use two axes, not one. Application tags answer ownership and deployment questions; per-key attribution answers access and separation questions. The smallest useful event contains both, while the reporting layer decides which axis to show.

| Decision need | Primary dimension | Why it fits | What it cannot prove |
| --- | --- | --- | --- |
| Team budget and service SLO | Application tag | Stable across worker restarts and key rotation | Which credential signed a request |
| Credential review and incident scope | Key reference | Directly joins access logs and rotation records | Whether several apps shared the key |
| Retry or duplicate-delivery analysis | Job or idempotency ID | Connects attempts to one business event | Long-term ownership without an app tag |
| Provider invoice reconciliation | Provider request or capability | Matches the billable unit | Why the application generated demand |

Do not put high-cardinality values such as raw customer IDs into every cost metric. Keep them in trace or event storage with access controls, and attach a bounded correlation ID to the metric. This keeps dashboards queryable without throwing away forensic detail.

## Where per-key attribution is the wrong boundary

The catch is that a key is often rotated, pooled, or shared. If a team changes credentials every seven days, a per-key chart fragments one service into eight apparent spenders. If a shared key serves dispatch, tracking, and notifications, the chart is precise about access but useless for ownership. In those cases, make the application identity mandatory at issuance and use the key reference as a secondary audit dimension.

Application tags are also not magic. A tag supplied by an untrusted caller can be forged, and a missing tag can silently fall into an “unknown” bucket. Derive the application from workload identity or the credential registry where possible, reject empty values at the instrumentation boundary, and test that the tag survives retries and asynchronous handoffs.

Stick with a coarser application model when the organization cannot protect the key-to-service mapping. A beautifully detailed key ledger built on mutable labels creates confidence without evidence. Your mileage may vary when providers expose only account-level billing; in that case, preserve your internal usage events and state the reconciliation gap explicitly.

## A runbook that survives the next outage

During an incident, freeze the dimensions before changing code. Capture the alert query, the top applications, the affected key references, retry counts, and the last successful rotation. Compare request attempts with queue deliveries. A mismatch usually points to an acknowledgement or deduplication boundary, not a pricing calculation.

After containment, replay a bounded sample through a staging ledger. Assert that one business event produces one billable outcome, that a retry keeps its idempotency key, and that a rotation changes the key reference without changing the application identity. Add a regression test for the exact failure mode; “the dashboard looked fine” is not a test.

The decision rule is simple: report by application, investigate by key, and reconcile by the provider’s billable unit. If your access policy requires proof of who could call a capability, retain the per-key reference. If your main need is team budgeting and the provider cannot expose key-level usage, do not invent precision—document the limit and invest in better internal events instead.

## References

- OWASP Secrets Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- OpenTelemetry semantic conventions: https://opentelemetry.io/docs/specs/semconv/
- OpenTelemetry metrics data model: https://opentelemetry.io/docs/reference/specification/metrics/data-model/
