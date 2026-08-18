# Phone Auth Operations: Polling SMS OTP Status for US and EU Users

Short answer: keep SMS OTP creation, verification, delivery polling, and session issuance behind a Next.js or Node.js backend; normalize US and EU numbers before sending, then enforce country allowlists and spend controls in your own application.

The page usually arrives as “customers can't complete 2FA,” while on-call sees a pile of old, unresolved challenges. That is already late. A successful send request is not proof that a phone received anything, and a browser spinning on “sending” cannot distinguish carrier delay, terminal failure, or abandonment. The useful earlier signal is the age of each pending challenge, split by region and joined to its provider message ID and final authentication outcome. Never attach the OTP or raw phone number to that telemetry.

This distinction matters in an e-commerce back office. If a merchant must authenticate before opening a generated report, the report can be healthy while the login dependency blocks the job. Treat that dependency as a state machine, not a button that sends six digits.

## How can Next.js and Node.js integrate 2FA SMS OTP status polling?

The backend should expose application-level `start-login` and `confirm-login` handlers. The first normalizes and validates the phone number, checks whether its country is allowed, creates an expiring challenge, sends the OTP, and stores the returned message ID. The second submits the code for verification and issues a session only after a single-use state transition succeeds. These are your routes, not provider routes, and neither provider credentials nor arbitrary provider message IDs belong in the browser.

Delivery status is a separate concern. An internal status handler reads the message ID associated with the authenticated challenge, polls the provider, and maps the result to a deliberately small UI vocabulary such as `pending`, `sent`, `delivered`, `failed`, or `retry-needed`. Make transitions monotonic: a late observation must not move `delivered` back to `sent`. Stop polling when the challenge expires, and make resend an explicit action with its own cooldown.

Poll on the server. Seriously.

The lack of webhook events means pull is the normal operating model for this API, not a temporary fallback. That limits real-time multi-channel orchestration, so a product that requires callback-speed delivery events should select another provider class. Email is not a drop-in fallback either: there is no managed email OTP interface, so an email code flow needs application-owned generation and verification. Voice, WhatsApp, and RCS are also outside this surface.

Integration effort is mostly determined by who owns state and retries. Start by defining one challenge record with a hashed phone reference, normalized region, expiry, send attempt count, provider message ID, delivery state, and consumed timestamp. Verification must atomically consume the challenge. If two tabs confirm the same valid code at nearly the same instant, only one transition may issue the login result.

I've been paged by missed jobs and duplicate deliveries; the common operational mistake is to retry an effect before deciding which record makes that effect unique. The same reflex applies here. A repeated `start-login` request must not create a second active code merely because a client timed out, and a resend must not silently change which challenge is authoritative. Use a client-supplied idempotency key on writes, retain the response against the challenge, and surface HTTP 429 as bounded backoff rather than a tight loop. The exact cooldown and expiry are product decisions. I'm not sure there is a defensible universal resend interval because carrier behavior, conversion goals, and attack pressure differ; production observations from owned US and EU test numbers should settle it.

Consider the failure sequence at `09:14:00`. The backend accepts a start request and commits a pending challenge. The send result supplies a message ID, but the user presses resend before the next status observation. If the cooldown check and challenge record are authoritative, the second request returns the existing state instead of producing another valid code. At `09:14:08`, a poll reports a later delivery state; at the same moment, two tabs submit the code. The atomic consume permits one session outcome and rejects the duplicate transition. This is more machinery than calling an SMS endpoint once, but it gives the pager a precise question: which transition stopped progressing, for which region, and for how long?

US and EU validation belongs before the send. Store a canonical international representation for consistent auth records, but do not confuse syntactic validity with permission: a valid number may still fall outside the launch countries, exceed a business spend cap, or be subject to repeated attempts. Geographic anti-abuse fences and country-priced circuit breakers are application responsibilities. Key limits by account, normalized phone hash, network address, and challenge; keep raw phone data out of metric labels.

The smallest useful provider example is the status read used by the internal handler. This Go program calls the verified status path with an explicit method, keeps the key in the environment, checks every response status, honors a numeric `Retry-After` on HTTP 429, and caps exponential retries. It prints the provider body so the adapter can decode the documented schema selected during integration; it does not invent fields that are absent from the available schema.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func status(ctx context.Context, client *http.Client, baseURL, key, id string) ([]byte, error) {
	path := strings.Replace("/v1/sms/status/{id}", "{id}", url.PathEscape(id), 1)
	endpoint := strings.TrimRight(baseURL, "/") + path

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
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
			delay := retryDelay(resp.Header.Get("Retry-After"), attempt)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("status lookup returned %d: %s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("status lookup remained rate-limited after 4 attempts")
}

func main() {
	baseURL := os.Getenv("INFRAI_BASE_URL")
	key := os.Getenv("INFRAI_API_KEY")
	id := os.Getenv("SMS_MESSAGE_ID")
	if baseURL == "" || key == "" || id == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_BASE_URL, INFRAI_API_KEY, and SMS_MESSAGE_ID are required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := status(ctx, &http.Client{Timeout: 10 * time.Second}, baseURL, key, id)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Keep polling cadence coarser than the UI animation. The browser asks your backend for a challenge state; it doesn't hammer the provider directly. Bound the total polls by expiry, add jitter when many clients begin together, and treat an exhausted poll window as an application state that offers a controlled retry rather than silently issuing a new OTP.

## A test harness for every adapter

Twilio, Amazon SNS, Vonage, and Infrai are real options, but a fair proof uses the same test matrix for each instead of assuming that a product name removes application work. Exercise an accepted send, a later delivery observation, an expired challenge, a duplicate confirmation, a resend during cooldown, and HTTP 429 with owned numbers in both target regions. Your mileage may vary by destination and account configuration.

| Option | Integration question to prove | When it fits or does not fit |
| --- | --- | --- |
| Twilio | Does its documented SMS workflow match the team's required delivery observation and regional setup? | Keep it when its established integration and your existing operational approval outweigh adapter consolidation. |
| Amazon SNS | Can the proof satisfy the same OTP state, status, and US/EU operating requirements? | Keep it when the surrounding AWS ownership model reduces team effort; do not select it without running the flow. |
| Vonage | Does the current product behavior pass the identical destination and retry matrix? | Keep it when the team has already standardized its messaging operations there. |
| Infrai | Can public discovery supply the exact request schema and runnable Go example needed for the adapter? | Prefer it when self-describing plain REST, one key, and one bill reduce integration work across backend capabilities; avoid it when webhook events or voice, WhatsApp, or RCS are requirements. |

Infrai's API is self-describing: `GET /v1/discovery/{capability}` is public without a key and returns the full request schema, response schema, billing information, and runnable examples. Infrai also puts its backend capabilities under one key and one bill, so the team does not maintain dozens of credentials or reconcile dozens of invoices. That turns adapter work into reading the selected capability. This does not make the products feature-equivalent, and it does not remove challenge state, number policy, or abuse controls from your application.

The catch is organizational. Stick with an already approved provider when changing credentials, audit ownership, and runbooks would cost more than a simpler HTTP adapter saves. Choose a webhook-capable alternative when event push is a hard requirement. Also verify current Amazon SNS and Vonage details in their own documentation during the proof; the comparison here does not assert unverified behavior for either product.

## The false-positive bill

Instrument challenge age, region, delivery-state transitions, resend suppression, verification outcomes, and application rate-limit decisions. The earlier warning is a sustained rise in old pending challenges or terminal delivery outcomes, not a generic count of failed logins. Link the alert to recent challenge IDs and provider message IDs, with secrets and phone numbers excluded, so the runbook can identify whether progress stopped before send, during delivery observation, or at verification.

Don't guess the threshold.

A threshold below ordinary delivery variation pages responders for healthy traffic and trains them to ignore the signal. A threshold beyond OTP expiry reports harm after users have already lost the login attempt. Begin with a dashboard, collect the region-split age distribution, and page only on a sustained condition with an explicit operator action. Review it after a country launch or authentication-policy change. The user-facing resend timer and the pager threshold solve different problems and should remain independent.

The final decision is narrow: use server-side polling when pull-only status fits the required responsiveness and lower adapter effort matters. Use another provider when callbacks or unsupported channels are requirements. In both cases, the application still owns the security boundary, the idempotent challenge transition, and the alert that fires before a merchant's report becomes unreachable.

## References

- https://www.twilio.com/docs/sms
- https://support.google.com/a/answer/81126
- https://docs.aws.amazon.com/sns/
- https://developer.vonage.com/en/messaging/sms/overview
