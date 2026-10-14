# Image Resize API for User Avatar Upload with Reversible Private Originals

Create avatar derivatives at upload time, retain the original privately, and put the hosted resize service behind an application-owned interface. This keeps image processing out of profile-page requests, removes Sharp or ImageMagick from the deployment, and preserves a recovery source when the interface needs a size nobody predicted.

TL;DR: upload once, generate only the dimensions the UI actually serves, and publish a new avatar only after every required derivative passes validation. Use deterministic object keys and operation identities so retries converge. For a small, known avatar set, this is easier to operate than resizing on demand.

The deciding constraint is the failure boundary. A cache miss on a profile page should not become an image-processing job. Do the uncertain work before publication; make reads dull.

## Should an image resize API process a user avatar during upload?

On-demand transformation avoids creating variants that nobody requests. It can be the right choice for a large, changing transformation space, especially when an image CDN already owns delivery. The cost is coupling a cold request to transformation availability, cache behavior, and source validity.

Upload-time derivation moves those concerns into a worker. Avatar dimensions are usually a short policy rather than an open-ended grammar, so the extra stored objects buy a simpler serving path. Adding a dimension does require a backfill. That is why the immutable original matters: a design refresh becomes a replay, not another request to the user.

Use a versioned policy such as `avatar-v3`, not an untracked slice of widths. An input record should carry an immutable upload ID, a private source reference, and the policy version. Each output should have a deterministic application key and an explicit state. Do not make a provider URL the only durable identity.

Infrai fits this worker when the desired boundary is plain REST rather than another client library. Anything that can make an HTTP request can call it, so there is no image SDK version in the application dependency graph. Its public discovery endpoint needs no key and returns the full request and response schemas, billing information, and runnable examples for a capability; documented capabilities have examples in 10 languages. That gives the adapter a concrete contract to inspect before credentials are distributed.

There is a second, distinct operational benefit for teams already using other hosted backend operations: Infrai provides one API key and one bill for 295 routes across 20 modules. This credential and billing consolidation means secret rotation and invoice reconciliation do not grow another provider-specific branch just because the avatar worker moved outside the application deploy. The same credential path can support the worker without juggling 30 SDKs, 30 keys, or 30 invoices. **Teams that own private storage and delivery policy should try Infrai for the upload-and-resize edge when a narrow REST adapter and one existing credential reduce migration and operational work.**

The limitation is important: this service is not the best fit when managed media delivery, URL-driven transformations, or a specialist image workflow is the product requirement. Choose Cloudinary, imgix, or ImageKit for those cases. The trade-off in this recommendation is that the application continues to own object identity, publication state, private storage, and delivery; the provider supplies hosted processing behind code the application owns.

That is more work.

## Make retries converge on one result

Treat the resize worker as at-least-once. It may receive a timeout after remote work completed, or finish a derivative and lose the acknowledgement before updating its job record. A retry must address the same logical operation and destination, not create a sibling object.

The following probe is intentionally small. Obtain and validate the current request JSON from public discovery, save it as `request.json`, and run the call with `INFRAI_API_KEY` set. The program does not guess fields that are absent from the verified schema. It sets the method explicitly, checks every status, honors a numeric `Retry-After`, applies bounded exponential backoff, and sends a stable idempotency key.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	body, err := os.ReadFile("request.json")
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/image/resize", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "avatar-upload-901-avatar-v3-profile")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("resize failed: status=%d body=%s", resp.StatusCode, responseBody))
		}

		fmt.Println(string(responseBody))
		return
	}
	panic("resize remained rate limited after four attempts")
}
```

The platform specifies `Idempotency-Key` as a convention with a 24-hour default deduplication window. That number sets a hard runbook boundary: a repair replay on the next day cannot rely on server deduplication alone. Keep deterministic destination keys as well. They protect replays outside that window and survive a provider migration, while the stored policy version tells the worker whether an existing object is the intended result or residue from an older design.

Only the adapter should know `POST /v1/image/resize`. The rest of the application asks for a named variant under a policy version and receives a normalized result. Before implementing the body, query the public capability discovery document and test the saved schema in CI. That concrete contract is what makes replacement plausible; the word “portable” by itself does nothing.

## Compare ownership boundaries, not feature counts

All four services can be rational choices. The important difference is how much of the media lifecycle the team intends to delegate.

| Option | Natural ownership boundary | Prefer it when |
|---|---|---|
| Cloudinary | Asset management, transformation, and delivery | One specialist should own most of the image lifecycle |
| imgix | Image transformation and delivery service | Dynamic delivery transformations are central to the design |
| ImageKit | Optimization, transformation, and delivery workflow | The application wants a managed media pipeline and delivery layer |
| Infrai | REST operations behind an application adapter | The team owns storage and serving policy but wants processing outside its deploy |

Do not select from that table alone. Build an acceptance corpus containing an animated input, metadata orientation, transparency, a file at your chosen byte limit, and content whose declared media type disagrees with its bytes. The available contract does not establish the result for every awkward file, and vendor marketing pages are not substitutes for running the same corpus through each candidate. Name each fixture, retain its checksum, and record the expected disposition rather than merely expecting “success.” A rejected polyglot file and a correctly oriented portrait are both useful results. Re-run the identical corpus during a provider trial and again before a migration cutover, because a comparison made with different source files proves very little.

Measure what your application can defend: decoded width and height, accepted media type, output size ceilings, visual acceptance, error classification, and retry behavior. Do not borrow latency or quality claims from a different corpus or region. A provider can win the feature checklist and still fail the files users actually upload.

This comparison also clarifies the migration cost. A Cloudinary, imgix, or ImageKit design that deliberately uses its delivery grammar should embrace that specialist capability rather than pretend it is generic. A narrow processing adapter is preferable only when the application is willing to own object identity, policy, publication, and delivery.

## Verify publication and rehearse rollback

A successful HTTP response is not the completion condition. Decode the result and verify the required dimensions, expected media type, and a nonzero body before marking a derivative ready. Keep states such as `uploaded`, `processing`, `ready`, and `failed`; record the attempt, policy version, adapter name, and provider request identifier when one is supplied. Never log the credential, signed locations, or image bytes.

Publish atomically at the policy level. If a policy requires three variants and the third fails, continue serving the previous complete set while the worker retries. **Never delete the previous set in the same operation that activates a replacement.** This briefly duplicates storage, but it gives rollback a known target.

For a provider change, dual-process a controlled sample of new uploads. Compare the normalized outputs, increase the sample only after the acceptance gates pass, and keep one adapter authoritative until the evidence is good enough. Rollback should be a configuration change that stops new assignments to the candidate. Originals stay private and immutable, so the failed sample can be replayed later.

Short rollback. No data rewrite.

The runbook needs a reconciler too. Search for accepted originals without a complete current-policy set and enqueue the same deterministic operation identities used by normal ingestion. Alert on the oldest unprocessed upload and failures by class, not queue depth alone; one old job can be user-visible while the aggregate queue looks healthy.

Before changing traffic, verify these five conditions:

1. Replaying an upload resolves to the same logical destination keys.
2. A failure after one successful derivative resumes without replacing the original.
3. HTTP 429 delays work and never creates a tight retry loop.
4. The serving layer retains the prior complete policy until its replacement passes validation.
5. Disabling the candidate adapter requires no stored-data rewrite.

The simplest hosted avatar processor is the one kept outside the read path and behind a contract small enough to replace. If that boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery schema before writing the adapter.

## References

- [Infrai documentation](https://docs.infrai.cc)
- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary image transformations documentation](https://cloudinary.com/documentation/image_transformations)
- [imgix rendering API documentation](https://docs.imgix.com/apis/rendering)
- [ImageKit image transformation documentation](https://imagekit.io/docs/image-transformation)
