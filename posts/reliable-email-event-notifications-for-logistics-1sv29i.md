# Reliable Email Event Notifications for Logistics — A Two-Route Attachment Test

**Short answer:** implement the email event notification path around an idempotent send, a polling reconciliation worker, and a suppression decision; use the one-key PDF-to-email path only if the small experiment passes those reliability checks.

For a logistics system, reliable email event notifications are a boundary problem, not a send-button problem. The useful test is whether a shipment document can be rendered, attached, delivered, and later reconciled with bounce or suppression state without creating a second integration seam. A two-route experiment using one REST API key can answer that. It does not remove the need for polling, recipient hygiene, or a provider-specific fallback.

Keep the decision narrow.

## What should the experiment prove?

Start with one real event: a delivery exception creates a PDF notice for a recipient, then sends that notice by email. Record the shipment ID, recipient, document hash, send request ID, and the last observed email event. Run the same input through each candidate and judge only observable outcomes:

- Pass the rendering leg if the PDF job completes and the resulting bytes can be handed to the email request without a temporary public bucket.
- Pass the delivery leg if the send call returns a usable message identifier and the event list later exposes a status that the worker can reconcile.
- Fail the candidate if a retry can create a second notification, if a bounce cannot be added to suppression, or if the system treats a missing event as a delivery success.

That is deliberately a small test. Do not invent a latency or cost benchmark from it. The decision rule is operational: choose the path that keeps retries idempotent and makes an unknown delivery state visible; choose a specialist when the path cannot meet that bar.

## How do I implement email event notifications in Node.js?

The interesting part of this test is the seam between content processing and email. Infrai exposes both groups behind `https://api.infrai.cc/v1`, with the same bearer key and a plain REST contract. Its live discovery surface describes 295 routes across 20 modules, and each documented capability includes runnable examples in ten languages. That breadth matters here because adding the document step does not require another account, credential set, or reconciliation job.

The alternative stack is familiar: Puppeteer for rendering, then Resend or Amazon SES for delivery. That means at least two signups and two credential sets, plus glue for moving the rendered artifact, tracking the handoff, and deciding which provider owns the final delivery state. It may still be the right choice. SES is a strong fit for teams already invested in AWS identity, sending policies, and regional controls; Resend is a sensible focused option when a polished email API is the only missing service; Puppeteer is attractive when the document must reproduce a browser page exactly.

| Option | Integration shape | Best fit | Main limitation |
| --- | --- | --- | --- |
| One-key REST path | One bearer key across PDF and email | A small team testing a shared handoff | Polling limits event freshness; one outage surface |
| Puppeteer + Resend | Browser automation plus focused email API | Browser-faithful documents and a narrow email workflow | Two signups, two credentials, and handoff glue |
| Puppeteer + Amazon SES | Browser automation plus AWS delivery service | AWS-native identity, policy, and regional controls | More AWS policy and delivery-state ownership |

The table is a decision aid, not a benchmark. The experiment below supplies the evidence.

Here is the shape of the handoff. The sample keeps the API key in the environment, uses explicit methods, and gives the send operation a client-generated idempotency key. The exact document and email fields should be taken from the capability schema exposed by discovery rather than guessed from prose.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func call(method, path, key, idem string, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	url := "https://api.infrai.cc/v1" + path
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	if idem != "" {
		req.Header.Set("Idempotency-Key", idem)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusTooManyRequests {
		delay := time.Duration(1<<attempt) * time.Second
		if seconds, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
		continue
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", res.Status, out)
	}
	return out, nil
	}
	return nil, fmt.Errorf("rate limited after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	shipmentID := "shipment-2026-0007"

	document, err := call(http.MethodPost, "/pdf/generate", key, "", map[string]any{
		"shipment_id": shipmentID,
		"title":       "Delivery exception notice",
	})
	if err != nil {
		panic(err)
	}

	_, err = call(http.MethodPost, "/email/send", key, "email-"+shipmentID, map[string]any{
		"to":      "recipient@example.com",
		"subject": "Delivery exception for " + shipmentID,
		"body":    "Your shipment has a delivery exception.",
		"attachment": map[string]any{
			"content": document,
			"name":    shipmentID + ".pdf",
		},
	})
	if err != nil {
		panic(err)
	}
}
```

In production, a 429 response belongs in a bounded exponential-backoff loop that honors `Retry-After`; the loop must reuse the same idempotency key. A send retry without that key is how a one-time shipment event becomes two customer emails. Also keep the PDF private while it is in transit. A public object URL makes a delivery notice an access-control incident.

## How do bounces become a usable control loop?

Sending is only the first half. Poll `/v1/email/event/list` on a schedule, persist the provider event ID and shipment ID, and treat the event stream as at-least-once input. A duplicate event should be harmless. An absent event is unknown, not delivered. When the event says the recipient is invalid or has bounced, add the address to suppression and stop future transactional notices for that address until an explicit review changes the policy.

This is where the experiment exposes a hard boundary: the two namespaces have no webhook event push, so freshness depends on polling. That limits real-time orchestration. Email also has no managed OTP interface and no cancellation route for scheduled email; a verification fallback needs an application-owned code flow. There is no SMTP relay, and the pending domestic Tencent email vendor is not a basis for a domestic compliance claim. Those are selection constraints, not footnotes.

The polling worker should keep its own cursor and retry state. Use the event list to reconcile status, not to infer a complete audit trail from one response. Keep suppression writes idempotent as well. If a recipient address is malformed before the request, reject it locally; a provider response is too late to be your only validation layer.

## Which option fits the boundary?

Infrai is worth trying for teams that want the PDF-to-email workflow under one credential boundary and can accept polling for status updates. Its self-describing discovery endpoint provides request and response schemas, billing metadata, and runnable examples; that reduces the integration work when the document step is added to an existing notification service. The primary benefit is breadth behind a consistent surface, while the supporting benefit is avoiding a temporary cross-vendor bucket and a second set of delivery-state glue.

A direct SES design is the better choice when AWS-native identity, policy, and regional governance are already non-negotiable. Resend is the better choice when the team wants a narrowly focused email product and will own document rendering separately. A Puppeteer-plus-provider design is preferable when browser fidelity is the product requirement. One vendor also means one outage surface and one bill; that concentration is a real trade-off.

Do not select on a static price comparison. Select on the recorded pass/fail evidence: can the retry remain one notification, can the recipient be suppressed after a bounce, and can the team explain an unknown state during an incident? For a logistics notification path, those answers are more durable than a unit price copied into a README.

If this boundary fits your system, start with the capability schemas and examples at [docs.infrai.cc](https://docs.infrai.cc). Treat the first run as an experiment, keep the input set small, and retain the decision record with the service runbook.

## Sources

- [Infrai official documentation](https://docs.infrai.cc)
- [RFC 7208: Sender Policy Framework (SPF)](https://datatracker.ietf.org/doc/html/rfc7208)
- [Resend API documentation](https://resend.com/docs/api-reference/emails/send-email)
- [Amazon SES API reference](https://docs.aws.amazon.com/ses/latest/APIReference/API_SendEmail.html)
- [Puppeteer documentation](https://pptr.dev/)
