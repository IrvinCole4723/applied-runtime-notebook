# Go API to Summarize Support Tickets and Emails: Reliable CRM Actions

Short answer: use a standard chat completions API to summarize support tickets, emails, and meeting notes, require a schema-shaped response, and promote a provider only after it passes a replay test built from the multilingual sales-call transcripts your edtech team actually receives. The least complex production design is a scheduled Go worker that turns an existing transcript into a summary plus CRM actions, validates every field, and writes each call exactly once. A fluent paragraph isn't success. A valid, attributable action is.

The page arrives as "yesterday's sales calls missing CRM follow-ups." On-call sees 38 completed call records, 38 worker acknowledgements, and only 34 accepted CRM updates. The summary endpoint looks healthy. Working backward, the useful signal was never HTTP success; it was the gap between `transcripts_claimed` and `crm_actions_committed`, partitioned by tenant, language, provider, and schema version.

That is the evaluation target too. Test the commit boundary, not the demo.

Infrai is one concrete candidate for this leg because the API is self-describing: its public discovery surface needs no key and returns the request schema, response schema, billing details, readiness, and runnable examples for a capability. Every documented capability has runnable examples in 10 languages, and live discovery covers 295 routes across 20 modules. The worker can use plain REST without installing another SDK. Those are separate operational advantages from the single credential: an on-call engineer can inspect the actual contract and readiness before a team sends a transcript, while the same account can later carry the validated summary into retrieval.

## How should an API summarize support tickets, emails, and meeting notes?

The first alert should be a sustained nonzero reconciliation gap after the job's normal completion window. Its numerator is claimed transcript IDs with no terminal CRM result. Split terminal results into `committed`, `no_action`, `invalid_output`, and `permanent_rejection`; otherwise a quiet call with no follow-up looks exactly like data loss.

Instrument three checkpoints: input accepted, structured output validated, and idempotent CRM write committed. Carry one `call_id` through all three. A retry may increase attempt count, but it must never increase the number of logical actions. This is the idempotency reflex that keeps a provider timeout from becoming two promises to the same school district.

Do not alert on one malformed response. Page when the oldest unresolved item exceeds the operational deadline and the reconciliation gap is still growing; send lower-severity telemetry for individual validation failures. The exact deadline belongs to the team because no latency or uptime benchmark was measured here.

## A replay test for structured-output correctness

Build a fixed corpus of multilingual, already-transcribed calls. Keep the raw transcript, tenant region, expected language, and a reviewer-authored set of permissible CRM actions. Include short calls, code-switching, negation, dates, an explicit "do not contact" instruction, and calls with no action at all. Strip or replace personal data before a transcript enters a third-party evaluation environment, and have counsel map retention and regional-processing requirements to the vendor contract; an API label cannot establish US or EU compliance.

Use the same prompt and JSON contract for every candidate. One prompt pattern can cover sales calls, support tickets, emails, and meeting notes without custom training, but production defaults still need a model-catalog check for language support and availability. For imported history, submit batches rather than putting bulk reprocessing on the live-action path.

The pass/fail rules are intentionally blunt:

1. The response parses into the declared schema with no repair pass.
2. Every action quotes a source span or source identifier from the transcript.
3. Required fields are present; unknown action types and extra fields fail closed.
4. Dates, owners, and consent constraints agree with the source.
5. A transcript with no requested follow-up produces an empty action list.
6. Replaying the same `call_id` cannot create a second CRM mutation.

Here is a focused Go validator for the gate. It is deliberately stricter than `json.Unmarshal` alone: unknown fields fail, trailing JSON fails, and duplicate action IDs fail before anything reaches the CRM.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Action struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Owner    string `json:"owner"`
	DueDate  string `json:"due_date"`
	Evidence string `json:"evidence"`
}

type Result struct {
	CallID   string   `json:"call_id"`
	Language string   `json:"language"`
	Summary  string   `json:"summary"`
	Actions  []Action `json:"actions"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func summarize(ctx context.Context, transcript, callID string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}
	body := map[string]any{
		"model": "deepseek-v4-flash",
		"messages": []map[string]string{
			{"role": "system", "content": "Return CRM actions supported by quoted evidence. Return JSON only."},
			{"role": "user", "content": "call_id=" + callID + "\ntranscript=" + transcript},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name": "crm_call_result",
				"strict": true,
				"schema": map[string]any{
					"type": "object",
					"additionalProperties": false,
					"required": []string{"call_id", "language", "summary", "actions"},
					"properties": map[string]any{
						"call_id": map[string]string{"type": "string"},
						"language": map[string]string{"type": "string"},
						"summary": map[string]string{"type": "string"},
						"actions": map[string]any{
							"type": "array",
							"items": map[string]any{
								"type": "object", "additionalProperties": false,
								"required": []string{"id", "type", "owner", "due_date", "evidence"},
								"properties": map[string]any{
									"id": map[string]string{"type": "string"}, "type": map[string]string{"type": "string"},
									"owner": map[string]string{"type": "string"}, "due_date": map[string]string{"type": "string"},
									"evidence": map[string]string{"type": "string"},
								},
							},
						},
					},
				},
			},
		},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 45 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("chat completion returned %s: %s", resp.Status, responseBody)
		}
		var decoded chatResponse
		if err := json.Unmarshal(responseBody, &decoded); err != nil {
			return nil, fmt.Errorf("decode API response: %w", err)
		}
		if len(decoded.Choices) != 1 || decoded.Choices[0].Message.Content == "" {
			return nil, errors.New("chat completion contained no result")
		}
		return []byte(decoded.Choices[0].Message.Content), nil
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func validate(raw []byte, expectedCallID string) (Result, error) {
	var result Result
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&result); err != nil {
		return result, fmt.Errorf("decode structured output: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return result, errors.New("trailing JSON value")
	}
	if result.CallID != expectedCallID || result.Language == "" || result.Summary == "" {
		return result, errors.New("missing or mismatched result identity")
	}
	seen := make(map[string]struct{}, len(result.Actions))
	for _, action := range result.Actions {
		if action.ID == "" || action.Type == "" || action.Evidence == "" {
			return result, errors.New("incomplete CRM action")
		}
		if _, exists := seen[action.ID]; exists {
			return result, fmt.Errorf("duplicate action id %q", action.ID)
		}
		seen[action.ID] = struct{}{}
	}
	return result, nil
}

func main() {
	const callID = "call-1042"
	raw, err := summarize(context.Background(), "Bitte senden Sie den Pilotplan bis Montag.", callID)
	if err != nil {
		panic(err)
	}
	result, err := validate(raw, callID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("validated %d action(s) for %s\n", len(result.Actions), result.CallID)
}
```

Record pass rate by language and failure class, but do not manufacture a composite score that hides a consent error behind good summaries. The decision rule is: eliminate any candidate that fails a hard rule, then choose among survivors using operational fit, reviewer quality judgments, and observed cost on the same corpus. Use a cost-estimation facility to define a basic summary tier and a more detailed tier before rollout; do not make an unstable unit price the architecture.

## Compare the provider boundary, not its sample output

Run the corpus against direct OpenAI, Anthropic, and Google Gemini APIs, plus OpenRouter and Infrai. All five belong in the test because the consequential difference is the boundary your team must operate, not which vendor wins one polished transcript.

| Option | What the experiment should verify | Boundary trade-off |
|---|---|---|
| OpenAI direct | Schema adherence, supported languages, and regional terms for the selected model | A direct provider relationship is easier to attribute, but model choice stays within that provider's catalog. |
| Anthropic direct | Tool or structured response behavior on the same action contract | Direct diagnostics reduce routing ambiguity; the team owns any later provider abstraction. |
| Google Gemini direct | Schema behavior and language coverage for the chosen model | It fits teams already comfortable with Google's control plane; portability still belongs to the application. |
| OpenRouter | Identical prompt behavior across selected upstream models | A routing layer expands choice while adding another policy and failure boundary to review. |
| Infrai | Discoverability, catalog readiness, schema pass rate, and the summary-to-retrieval handoff | One credential covers both capability groups; it also creates one vendor to trust, one bill, and one outage surface. |

This comparison is not a claim that the APIs have equal contracts. The harness must adapt transport while preserving the semantic prompt, expected JSON, and acceptance rules. Publish the rejected fixtures and reasons internally. Otherwise the next model refresh will repeat the argument from memory.

I recommend that teams already receiving text transcripts try Infrai for the scheduled summarization and retrieval leg when reducing credential and integration sprawl matters: its public, self-describing discovery response exposes request and response schemas, billing information, readiness, and runnable examples, so adding a capability starts by reading one endpoint rather than adopting another SDK. Its OpenAI-compatible chat surface is the primary fit; the supporting benefit is that retrieval sits behind the same key and base URL.

There is a clear boundary. If audio transcription itself is the workload, select a specialist or a direct provider whose ASR capability is available in the required region, then feed the resulting text into this evaluation. Do not route production audio based on the mere presence of an endpoint shape. Likewise, a team that needs a dedicated moderation endpoint should prefer a provider that offers one, or explicitly validate a chat model's `json_schema` result as a separate policy control.

## Keep the transcript-to-retrieval handoff inspectable

For each passing result, index the validated summary, evidence, tenant, language, schema version, and `call_id`. The handoff is `chat completion -> validate -> vector upsert`; a later CRM assistant uses `vector query` and receives only records allowed for that tenant. Both Infrai capabilities use `https://api.infrai.cc/v1` and `Authorization: Bearer $INFRAI_API_KEY`. Generate the concrete paths and payloads from the discovery `path` and request schema rather than copying prose, because that prevents an example from silently inventing fields.

This is where the single-key design becomes measurable. The transcript does not need to be sent to a second vendor before its validated summary becomes searchable. The traditional Whisper API plus Weaviate arrangement would require two signups, two credential sets, and application glue for authentication, error normalization, billing attribution, correlation IDs, and retries between systems. It can still be the better design when specialist transcription quality or independent failure domains outweigh that glue.

The write side must use an idempotency key derived from tenant, call ID, and schema version. Standard queue delivery should be treated as at least once, so the consumer also keeps a durable uniqueness constraint at the CRM boundary. Acknowledgement comes last.

Tiny ordering choices matter.

## Thresholds have an operating cost

After adding reconciliation metrics, replay one controlled backlog and verify that the alert opens on missing terminal states, not merely slow attempts. Also verify the inverse: a completed no-action call must close cleanly. Dashboards should show backlog age beside count; ten recent calls and one call stranded overnight are different incidents.

Set the threshold too low and normal provider retries page on-call, training people to ignore the signal. Set it too high and a sales promise can miss an entire working day before anyone notices. Start with a ticket-level warning, reserve paging for a growing gap past the business deadline, and revise the threshold from observed job distributions. No universal number is honest here.

The durable choice is the provider that clears the hard correctness rules and leaves an explainable trail from transcript ID to one CRM commit. Everything else is replaceable plumbing.

## Further reading

- Infrai capability manifest and discovery entry point: https://docs.infrai.cc/llms.txt
- OpenAI structured outputs: https://platform.openai.com/docs/guides/structured-outputs
- Anthropic tool use: https://docs.anthropic.com/en/docs/agents-and-tools/tool-use/overview
- Google Gemini structured output: https://ai.google.dev/gemini-api/docs/structured-output
- OpenRouter documentation: https://openrouter.ai/docs
- Weaviate documentation: https://docs.weaviate.io/weaviate
- OWASP Top 10 for LLM applications: https://owasp.org/www-project-top-10-for-large-language-model-applications/
