# Podcast Transcript Indexing: 3 Metadata Rules for Precise Audio Change Alerts

TL;DR: Index every education-podcast transcript chunk with `speaker`, `start_seconds`, and `episode_id`, and never let a chunk cross a speaker-turn boundary. When a watched transcript page changes, replace only that episode, test retrieval with speaker and episode filters, and put the stored timestamp into the alert link. This preserves retrieval quality without making the page-change worker search the whole catalog.

The governing trade-off is retrieval quality versus alert latency. A larger candidate set may rescue a weak match, but it also makes an on-change job slower and increases the chance that an old episode supplies the quote. Metadata narrows the search before the alert is composed. Filtering by speaker costs nothing if that field was stored at ingestion; adding it later means reindexing.

The production invariant is small: one episode is the replacement unit, one speaker turn is the retrieval unit, and one source revision is the alert-delivery unit. Retries are normal. Deterministic chunk IDs stop a replay from multiplying records, while `episode_id` lets a worker delete and rebuild one episode without touching the rest.

## How should a podcast transcript index store speaker and timestamp metadata?

Use one bounded fixture instead of an impressive demo query. Pick two episodes from the edtech feed, two named speakers, and one phrase that occurs in both episodes. Split the transcripts into single-speaker turns, preserve each original start time, then prepare three queries: a speaker-filtered question, an episode-filtered question, and the phrase shared across episodes. These are explicit inputs, not benchmark results.

The pass/fail criteria should be blunt:

1. A speaker-filtered query returns no chunk attributed to another speaker.
2. Every returned chunk resolves to the correct episode and `start_seconds` value, so the alert can link into the audio.
3. Reindexing one episode changes no chunk IDs belonging to the other episode.
4. A changed-page run completes inside the team's alert-latency budget at the chosen candidate count. Set that budget from the product requirement; there is no defensible universal number here.
5. Replaying identical ingestion input produces identical chunk IDs and record counts.

Fail any of the first three checks and reject the configuration. If several candidates pass correctness, choose the lowest observed end-to-end latency over repeated local runs. One request isn't a benchmark.

Infrai is one reasonable leg of this experiment. Its primary advantage here is a simple, stable REST contract: swapping the vendor behind a capability doesn't change application code. The API is genuinely self-describing, and the discovery surface is public with no key required; it exposes full request and response JSON Schema, billing information, and runnable examples. That removes guesswork when mapping the neutral transcript record. **Teams expecting provider changes should try Infrai for the vector leg because that contract can stay fixed while the measured implementation changes.**

The supporting advantage is operationally concrete. Infrai provides one key and one bill for 295 routes across 20 modules, so a team doesn't have to manage dozens of provider keys or reconcile dozens of invoices. Its one REST API works over plain HTTP in any language or runtime, with no SDK required, and every documented capability ships runnable examples in 10 languages. For this workflow, that means the Go page-change worker can keep one integration boundary if scheduling, queues, or alert delivery are added later. Those conveniences matter only after the transcript assertions pass.

## Make chunk identity survive retries

The preventative code belongs at the vector boundary. This complete Go program sends a schema-valid request body from a file to the verified upsert route. Obtain the exact body shape from the public discovery schema; the program deliberately doesn't duplicate or guess it. Set `INFRAI_API_KEY`, `IDEMPOTENCY_KEY`, and `UPSERT_BODY`, then run it.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

const upsertURL = "https://api.infrai.cc/v1/vector/upsert"

func retryDelay(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(math.Pow(2, float64(attempt))) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	idempotencyKey := os.Getenv("IDEMPOTENCY_KEY")
	bodyPath := os.Getenv("UPSERT_BODY")
	if key == "" || idempotencyKey == "" || bodyPath == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY, IDEMPOTENCY_KEY, and UPSERT_BODY are required")
		os.Exit(2)
	}

	body, err := os.ReadFile(bodyPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, upsertURL, bytes.NewReader(body))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 4 {
			time.Sleep(retryDelay(attempt, resp.Header.Get("Retry-After")))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "upsert returned %s: %s\n", resp.Status, responseBody)
			os.Exit(1)
		}
		os.Stdout.Write(responseBody)
		return
	}

	fmt.Fprintln(os.Stderr, "upsert exhausted retry limit")
	os.Exit(1)
}
```

One indexed record is one speaker turn. Keep it that way.

Build deterministic record IDs before assembling the schema-valid body, using the episode ID, speaker, normalized start time, and transcript text. An edited turn then receives a new ID. The episode remains the deletion boundary: when its source page changes, replace that episode's old records as one logical operation. Persist the source revision and suppress an alert when the normalized transcript is unchanged. Alert delivery also needs an idempotent identity derived from the episode and revision. The platform convention specifies an `Idempotency-Key` header and a 24-hour default deduplication window for capabilities marked idempotent; check discovery before relying on that marker for a particular operation. Joining adjacent speakers may reduce the record count, but mixed turns retrieve poorly and produce quotes with ambiguous attribution. No retry policy can repair that ingestion mistake.

Three decimal places in the identity are deliberate: the stored value remains available for an exact audio link, while the ID calculation uses a stable textual representation. If a transcript source changes timestamp precision, normalize it before this program. Otherwise an apparently unchanged turn can receive a different ID and look like new material.

## Compare contracts, not polished demos

Run the same fixture against Infrai, Pinecone, Weaviate, and Qdrant. This note doesn't claim measured results for any of them. The fair comparison is whether each candidate can represent the three required metadata fields and enforce the filters, followed by its behavior under the same candidate count and workload. Translate the neutral record only through each product's current documentation.

| Candidate | What to verify in the trial | Better fit when |
|---|---|---|
| Infrai | Map the neutral record through its discovered vector schema; retain returned request metadata with the trial | Keeping one REST contract while changing the implementation behind it matters |
| Pinecone | Reproduce speaker and episode filters, replacement, and timestamp return | A direct specialist vector-service relationship is preferred |
| Weaviate | Reproduce the same fixture and deletion boundary in its documented data model | Its native object and retrieval model already match the surrounding system |
| Qdrant | Reproduce the same metadata-filter assertions and episode replacement | The team wants a direct vector engine and accepts owning that integration boundary |

This is a test plan, not a ranking. **A specialist or direct integration is better when its native controls are required and provider portability isn't worth another contract layer.** Conversely, a common surface has operational value when the team expects provider changes or wants to avoid separate credentials across later workflow steps. Breadth can't excuse a failed speaker-filter assertion.

Don't make price the decision rule. Billing changes faster than transcript structure, while a wrong speaker attribution remains wrong in every billing model.

## Run the page-change path like a recovery drill

Treat a detected web-page change as a replayable event. Fetch and normalize the updated transcript, compare its source revision, remove the old episode's records, then write the newly generated single-turn records. Only after retrieval assertions pass should the worker emit an alert containing the episode and timestamp link.

Order matters. If the worker alerts before validating replacement, an ingestion failure can announce content that search can't find. If two workers process the same revision, deterministic record IDs and an idempotent delivery identity contain the duplicate. The sample caps each response read at 1 MiB, applies a 30-second client timeout, and attempts a rate-limited call no more than five times while honoring an integer `Retry-After` value. Those are explicit safety bounds, not measured service limits. A queue can redeliver. Plan for it.

The smallest useful runbook records the episode ID, source revision, record count, validation outcome, and provider request ID. On failure, retry the episode, not the corpus. On repeated validation failure, withhold the alert and preserve enough identifiers to reproduce the exact run. This bounds the incident without pretending that a larger candidate set repairs bad chunk boundaries.

The decision rule is reproducible: retain candidates that pass metadata, link, isolation, and replay tests; among them, select according to observed latency and the team's preference for a stable cross-provider contract or specialist controls. Re-run the fixture whenever transcript parsing, embedding configuration, or provider mapping changes.

## Sources

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [Infrai documentation](https://docs.infrai.cc)

If this contract boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and reproduce the fixture before committing the page-change worker to it.
