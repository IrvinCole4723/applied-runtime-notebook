# React Frontend Feature Flags: Polling Evidence for Logistics Incident Reconstruction

**TL;DR:** Poll a feature-flag snapshot, but treat every browser decision as evidence: record the snapshot revision, its fetch time, the fallback source, and the stable subject used for evaluation. In a logistics UI, a boolean such as `new_dispatch_board=true` cannot explain why a dispatcher saw one workflow while support saw another ten minutes later. A small decision record can. Keep the last validated snapshot across transient failures, impose a maximum usable age, and fall back to a reviewed local configuration after that boundary.

This is an incident-reconstruction design, not a claim that polling is universally best. I have been paged by missed jobs and duplicate deliveries; the painful question is rarely “what is the flag now?” It is “what did this client know when it allowed the action?” The invariant is straightforward: **a decision must remain explainable after the remote configuration has changed.**

## How should a React frontend preserve feature flag API polling evidence?

Start with a bounded scenario. A dispatcher opens a React shipment console, the browser polls configuration, and the UI chooses either the established assignment flow or a revised one. Later, an operator investigates why shipment `shp_8f31` was assigned twice. The current flag value is irrelevant unless it happens to match the value the browser evaluated at the time.

Keep one compact record at the decision boundary:

| Field | Example | Why it matters |
| --- | --- | --- |
| `flag_key` | `new_dispatch_board` | Identifies the controlled branch |
| `value` | `false` | Captures the actual decision |
| `source` | `cached` | Separates remote, cached, and default behavior |
| `revision` | `cfg-1842` | Joins the decision to an immutable snapshot |
| `fetched_at` | `2026-10-19T02:14:00Z` | Establishes snapshot age |
| `decided_at` | `2026-10-19T02:17:21Z` | Places the choice on the incident timeline |
| `subject_hash` | `7bb4...` | Replays targeting without logging a raw user identifier |

Do not attach route names, shipment IDs, customer IDs, and every targeting attribute to a metric label. Prometheus explicitly warns that every unique label combination creates another time series and recommends keeping cardinality low. Put aggregate outcomes in metrics, then place the bounded decision record in logs or traces where the incident identifier can be searched.

One detail matters here: emit the record when the controlled action is committed, not on every render. React renders are implementation activity. The assignment mutation is the operational event.

## Model the fallback as a state machine

A fallback is not a single hard-coded `false`. There are three states with different meanings: a fresh validated snapshot, a previously validated snapshot that is still within its allowed age, and a reviewed local default. Treat malformed responses like failed responses. A response that cannot be validated must never replace known-good state.

The following Go example makes the transition explicit. The same reducer shape belongs behind the React hook: polling supplies candidates, while reads obtain a decision plus its provenance. Durations are policy inputs, not universal recommendations.

```go
package flags

import (
	"errors"
	"time"
)

type Snapshot struct {
	Revision  string
	FetchedAt time.Time
	Flags     map[string]bool
}

type Decision struct {
	Key       string
	Value     bool
	Source    string
	Revision  string
	FetchedAt time.Time
	DecidedAt time.Time
}

type Store struct {
	lastGood Snapshot
	defaults map[string]bool
	maxAge   time.Duration
	now      func() time.Time
}

func (s *Store) Accept(candidate Snapshot) error {
	if candidate.Revision == "" || candidate.FetchedAt.IsZero() || candidate.Flags == nil {
		return errors.New("invalid flag snapshot")
	}
	if !s.lastGood.FetchedAt.IsZero() && candidate.FetchedAt.Before(s.lastGood.FetchedAt) {
		return errors.New("snapshot is older than current state")
	}
	s.lastGood = candidate
	return nil
}

func (s *Store) Decide(key string) Decision {
	now := s.now()
	if value, ok := s.lastGood.Flags[key]; ok && now.Sub(s.lastGood.FetchedAt) <= s.maxAge {
		return Decision{
			Key: key, Value: value, Source: "cached",
			Revision: s.lastGood.Revision, FetchedAt: s.lastGood.FetchedAt, DecidedAt: now,
		}
	}
	return Decision{
		Key: key, Value: s.defaults[key], Source: "default", DecidedAt: now,
	}
}
```

The first successful poll may label a decision `remote`; subsequent reads from the same snapshot are `cached`. That distinction is useful, but the revision and timestamps carry the real proof. If the API supports conditional requests, preserve the existing snapshot when the server reports that it has not changed. Poll timers should also stop when the component unmounts, and overlapping requests should be canceled or serialized so a slow, older response cannot overwrite newer state.

Jitter matters operationally. Thousands of tabs opened near a shift change should not all poll on the same boundary. Add randomized delay around the configured interval and back off after transport failures, while enforcing the separate maximum-age rule. Backoff controls load; maximum age controls whether stale evidence is still acceptable. They solve different problems.

## The preventative path is smaller than the telemetry path

The UI guard should be boring: obtain one decision, render one branch, and carry the same decision metadata into the action request or its associated event. Re-evaluating halfway through a multi-step dispatch operation creates a split-brain interaction in which the screen began under one revision and committed under another.

**Pin a snapshot revision for the lifetime of the business operation.** A background poll may prepare the next revision, but it should not mutate an in-progress assignment wizard. Apply it at a deliberate boundary, such as the next operation or a full view refresh.

For metrics, count fetch attempts by coarse result (`success`, `transport_error`, `invalid`) and observe fetch latency. Track decisions by flag key and source only if the key set is controlled and small. Never use a configuration revision or subject hash as a metric label; those belong in the decision event. This follows the instrumentation principle that labels should not create unbounded cardinality.

Logs need delivery discipline too. A logging appender can buffer or forward events, but the application should define what happens under pressure. Blocking the dispatcher UI to preserve a low-value diagnostic event is usually the wrong trade. Losing the only record of a committed assignment is also unacceptable. Route the committed business event through the durable server-side write path, and use browser telemetry as corroborating evidence rather than the sole audit trail.

Short-lived polling failures should be quiet at page level. Alert on sustained loss of fresh snapshots, a rising share of default-sourced decisions, or rejection of invalid snapshots. A single failed request is a symptom for a counter, not a page.

## Test the timeline, not just the boolean

Most flag tests assert `true` or `false` and stop. Incident reconstruction needs tests over ordered events. Use a fake clock and cover at least these transitions:

1. No remote snapshot exists, so the reviewed default is returned with `source=default`.
2. A valid snapshot arrives and becomes the current revision.
3. A transport failure occurs before maximum age; the last validated snapshot remains usable.
4. The snapshot crosses maximum age; the result changes to the local default and records that source.
5. A delayed older response arrives after a newer response; validation rejects the regression.
6. The component unmounts with a request in flight; no later state update or new timer is created.

Also test a complete business operation. Begin under revision `cfg-1842`, allow `cfg-1843` to arrive, and verify that the commit still reports `cfg-1842`. Then start a new operation and verify that it adopts the newer revision. That test catches a class of timing failures that a snapshot of rendered markup will miss.

Deployment deserves the same care. Add the evidence fields before using them during a rollout, verify ingestion and retention, and only then rely on them in the incident runbook. The runbook query should begin with the shipment or operation identifier, find the durable action, and join to the browser decision by a correlation identifier. Practice that retrieval with synthetic operations. An event nobody can find under pressure is decorative telemetry.

## When this pattern does not apply

The main limitation of client polling is authority: it is a poor fit for permissions, billing, inventory allocation, or any decision whose enforcement must survive a modified browser. The server must evaluate those controls. The frontend may poll a display hint, but the request still needs authoritative validation.

Nor does every visual experiment warrant durable per-decision evidence. For a cosmetic choice with no customer-impacting branch, aggregate exposure counts may be enough. Retention, privacy, and ingestion load are real costs. The trade-off is deliberate: keep detailed events for flags that can change an operational workflow, and define a deletion window consistent with the incident-review obligation.

The decision rule is simple. If an operator may later need to explain a customer-visible action, preserve the evaluated value, provenance, revision, and time at the action boundary. Otherwise, prefer lower-cardinality aggregates. This keeps the React integration modest while giving the incident timeline enough evidence to answer the question that matters.

## Sources

References:

- Prometheus, “Instrumentation”: https://prometheus.io/docs/practices/instrumentation/
- Logback Manual, “Appenders”: https://logback.qos.ch/manual/appenders.html
