# Logistics CRM Actions: Validating Unified LLM API Output in a Node.js Backend

**Short answer:** Choose a unified LLM API only after every candidate can produce the same validated CRM-action envelope, preserve the required US or EU processing boundary, and leave enough evidence to replay a failed sales-call job safely; one key is an operational convenience, while structured output correctness is the acceptance criterion.

This distinction matters in logistics. A summary that reads well but turns "call the consignee Friday" into an unowned task, drops the date, or invents a shipment identifier is not a slightly weak answer. It is bad state headed for a CRM. OpenAI, Claude, and Gemini may all sit behind one backend credential, but the application still owns the contract at the point where generated text becomes an action.

I've been paged by missed jobs and duplicate deliveries. The invariant those incidents leave behind is blunt: acknowledgment is not completion, and a fluent response is not a committed record. Don't let a gateway's successful status become either one.

## The incident boundary is the CRM write

Consider a bounded job: a completed carrier sales call arrives with a transcript and a stable `call_id`; the worker requests a summary plus CRM actions; the application validates the result; then it writes those actions. There are at least three distinct success boundaries. The model returned something. The returned object satisfied the business contract. The CRM accepted an idempotent write. Collapsing them into a single green check is how an ordinary retry becomes a duplicate follow-up.

The most useful postmortem question is not "Which model was up?" It is "What durable fact proves this call was converted exactly once under the schema version we intended?" Store the input identity, contract version, selected model alias, raw response reference or permitted audit representation, validation result, and commit identity. Keep sensitive transcript retention separate from the operational record; debugging value does not erase data-handling obligations.

Make the boundary concrete. Suppose job `call-1842` leaves the queue, the model produces valid JSON, and the worker writes two follow-ups before losing its acknowledgment. The queue delivers the job again. If the only recorded fact is that both model requests succeeded, the second worker has no proof that the first CRM write happened and may create the same two follow-ups again. If the worker acknowledges before the write instead, a process exit in that gap loses both actions. The preventative path is narrower: derive a stable operation identity from `call_id` and the contract version, validate the complete response, commit the actions under that identity, record the terminal outcome, and only then acknowledge the queue item. On redelivery, the worker reads the existing terminal outcome and performs no second mutation. This sequence does not promise magical exactly-once transport. It makes an at-least-once delivery harmless at the business boundary, which is the property the sales team actually needs.

Commit first. Acknowledge second.

This is also why the one-key decision comes second. Credential consolidation can reduce secret distribution and make provider selection a configuration concern — useful work — but it cannot decide whether an empty `owner` is legal for a `follow_up` action. The gateway should transport and account for requests. The application should enforce domain meaning.

## How should a Node.js backend test a unified LLM API with one key?

Test it as a fallible dependency behind a narrow internal interface, even if the production caller is Node.js. The example below is Go because a tiny contract checker is easy to run as an independent CI fixture; the same JSON envelope should be consumed by the Node.js worker. It deliberately contains no provider route. A route belongs in an adapter and must come from the chosen gateway's published interface, not from an assumed REST convention.

Start with fixtures taken from the shape of real logistics calls, after removing or replacing sensitive data. Include corrections ("not Tuesday, Thursday"), relative dates, multiple contacts, a refusal to commit, and a call with no legitimate next action. Then run every model candidate and every routing policy against the same fixtures. The scorer should reject malformed or semantically incomplete output before any CRM mutation.

```go
package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const ContractVersion = "crm-actions-v1"

type Action struct {
	Kind       string `json:"kind"`
	Owner      string `json:"owner"`
	DueDate    string `json:"due_date"`
	Evidence   string `json:"evidence"`
	ShipmentID string `json:"shipment_id,omitempty"`
}

type Result struct {
	CallID  string   `json:"call_id"`
	Summary string   `json:"summary"`
	Actions []Action `json:"actions"`
}

func DecodeAndValidate(body []byte, expectedCallID string) (Result, error) {
	var result Result
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return Result{}, fmt.Errorf("decode contract: %w", err)
	}
	if result.CallID != expectedCallID {
		return Result{}, errors.New("call_id does not match the job")
	}
	if strings.TrimSpace(result.Summary) == "" {
		return Result{}, errors.New("summary is required")
	}
	for i, action := range result.Actions {
		if action.Kind == "" || action.Owner == "" || action.Evidence == "" {
			return Result{}, fmt.Errorf("action %d is incomplete", i)
		}
	}
	return result, nil
}

func IdempotencyKey(callID string) string {
	sum := sha256.Sum256([]byte(callID + ":" + ContractVersion))
	return hex.EncodeToString(sum[:])
}
```

The short function encodes several production decisions. Unknown fields fail closed, the response must be tied to the queued call, and each action needs evidence from the source material. The idempotency key binds the call to the contract version, so retrying the same conversion does not create another logical operation. A contract change creates a deliberately different identity and therefore needs an explicit migration or replay decision.

It is not a complete semantic judge. For example, this checker can prove that `due_date` exists, but not that it reflects the caller's correction. Add fixture-specific assertions for facts that matter, and route uncertain extractions to review rather than manufacturing certainty. I'm not sure any offline suite can predict the full distribution of live call language; sampled production evaluation, under the applicable privacy controls, is what would narrow that uncertainty.

## Compare failure ownership, not feature counts

A useful evaluation table names the party and evidence needed at each boundary. Fill it with observed results from your own fixtures and deployment documents. Marketing labels are not evidence for residency or retry behavior.

| Decision boundary | Evidence to collect | Reject when |
|---|---|---|
| Output contract | Raw fixture result and validator report | Required fields are missing, unknown fields appear, or identifiers drift |
| Semantic fidelity | Expected facts and source spans | An action contradicts a correction or lacks transcript evidence |
| Retry safety | Replayed job and CRM commit identity | The same logical job creates a second action |
| US/EU handling | Contractual terms and documented processing path | The required regional boundary cannot be demonstrated |
| Provider switch | Results from the identical fixture suite | A route change silently changes the envelope or business meaning |
| Operations | Logs that join job, request, validation, and commit IDs | An on-call engineer cannot locate the last durable boundary |

OpenAI, Claude, and Gemini are choices inside the model-adapter row, not separate application architectures. Give the worker a stable internal request such as `SummarizeCall(callID, transcript, contractVersion)` and return an untrusted byte sequence plus request metadata. Provider-specific model identifiers and request options stay in configuration or adapters. The validator and CRM writer should not branch on the provider name.

A self-hosted open-source gateway such as LiteLLM is one possible way to centralize that adapter boundary. The catch is ownership: self-hosting is not suitable when the team cannot operate the gateway, patch it, observe it, and test upgrades. A managed unified endpoint moves some of that operational work outside the team, but it is unsuitable when its documented processing path cannot meet the required US/EU boundary. Direct provider integrations remain reasonable when the model set is small, provider-specific controls are central to the workload, or adding another intermediary would make incident ownership less clear.

No option removes evaluation work.

## Deploy the contract before changing the route

Treat a model or routing change like a data-producing dependency release. First, record the current contract pass rate on the fixed fixture set without turning a small sample into a universal benchmark. Next, shadow the candidate where policy permits, compare normalized outputs without writing to the CRM, and inspect disagreements in business terms: wrong owner, wrong date, unsupported shipment ID, or lost correction. Only then move a limited slice of jobs, with rollback tied to validation and commit signals rather than to response latency alone.

Keep the queue acknowledgment after the durable CRM outcome, or after a durable terminal record that a separate review process owns. Retries need a budget and a reason code. Validation failures are not transport retries; repeating the same prompt against the same model may only reproduce the same invalid object. A retry policy can change an explicitly recorded dimension, such as the model route, but the contract and idempotency identity must remain visible.

Fail closed.

Watch counts for jobs accepted, model responses received, contracts accepted, review decisions, CRM commits, retries, and duplicate commits prevented. Correlate them by identifiers, not transcript text. Alerts should describe a broken invariant — for example, accepted jobs without a terminal record — because a provider-level success metric can stay healthy while the CRM pipeline is losing work.

Be careful with failover. Automatic routing across model families sounds attractive, yet it changes the data producer during an incident. If the fallback has not passed the same structured-output fixtures and regional review, fail closed into a delayed queue or human review. Fast wrong actions are harder to unwind than late visible ones.

## What should decide the final selection?

Select the simplest option that passes the contract suite, provides auditable US/EU handling for the actual deployment, and leaves failure ownership clear to the on-call team. "One key" earns weight when it materially simplifies secret rotation and adapter maintenance. It loses weight when centralization hides provider identity, weakens audit evidence, or introduces an operational layer the team cannot support.

The final decision record should contain the fixture corpus version, schema version, observed failures, retry and idempotency rules, regional evidence, rollback trigger, and named owner for each boundary. Re-run it for model changes and gateway upgrades. Don't promote a candidate because its sample summaries sound better in a review meeting; promote it because the actions survive mechanical validation and targeted semantic checks.

There are workloads where this advice does not apply. If the output is disposable prose with no downstream mutation, a strict action schema and exactly-once commit boundary may cost more than they protect. Stick with a lighter direct call for low-risk experimentation. For logistics CRM automation, however, the durable record matters more than the elegance of the access layer. The backend can have one key or three. It still needs one enforceable contract.

## References

- https://platform.openai.com/docs/guides/embeddings
- https://github.com/BerriAI/litellm
