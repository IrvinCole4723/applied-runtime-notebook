# SaaS Avatar Upload Isolation: Private Presigned Reads Without Byte Proxying

Short answer: for a B2B SaaS, the simplest safe design is private object storage with server-issued, tenant-bound capabilities: the browser sends large media directly, while the application owns the object key, authorization decision, and completion record. S3 compatibility can reduce integration friction, but it is not the selection criterion. Tenant isolation is.

I've been paged by missed jobs and duplicate deliveries. The relevant lesson isn't that object storage behaves like a queue; it is that “the request was accepted” and “the business operation happened exactly once” are different statements. An upload grant, a finished byte transfer, and a media row visible to a tenant are three separate state changes. Collapsing them into one happy-path handler makes retries dangerous and incident reconstruction vague.

Keep one invariant in the runbook: a principal authenticated for tenant A can never receive a capability for tenant B's key.

## How should a SaaS isolate private avatar uploads and presigned download URLs?

Model the flow as capabilities with short, explicit lifetimes. The application authenticates the caller, resolves the internal tenant ID from trusted session state, allocates an upload ID, derives an opaque object key, and asks its storage adapter for an upload capability. The browser receives permission to transfer bytes to that one key; it does not receive permission to choose a tenant prefix, bucket, or storage policy.

That distinction matters more than the client language. A Node.js API can issue the application-level grant, while the browser talks to object storage and the storage adapter stays behind a small internal contract. The app doesn't proxy the large payload, yet it still controls who may write, what logical record the object belongs to, and when that record becomes readable.

Treat the private download URL the same way. First load the media row through a query constrained by the authenticated tenant. Then sign the stored key. Persist the key and authorization state, not the signed URL, because the URL is a temporary bearer capability rather than the identity of the avatar or file.

The catch is capability lifetime. Once issued, a presigned URL remains usable according to its own validity conditions; an application permission change does not retroactively alter that already-issued capability. Workloads requiring immediate revocation should put an authorization-aware delivery service in the read path. That is more infrastructure, but it matches the requirement.

## Make completion the tenant boundary, not a callback detail

The awkward failure is not usually “could the browser upload bytes?” It is “which tenant can observe the object after retries?” Create a pending row before signing. Bind that row to the internal tenant ID, upload ID, expected media policy, and server-derived key. After transfer, a completion request must reload the pending row under the current tenant and advance it to ready once. A replay returns the existing result.

Small rule. Large blast-radius reduction.

Consider two completion calls arriving together after a mobile client retries. If both handlers insert a ready media row, downstream thumbnail work can be duplicated and two logical avatars can point at one object. If a handler trusts an object key sent by the browser, the problem is worse: completion becomes a way to attach a key outside the caller's tenant namespace. The state transition needs a database uniqueness constraint or equivalent compare-and-set behavior around the upload ID, while the object key comes only from the pending record. I don't count “the UI sends completion once” as a control. It is an optimistic observation.

Multipart transfer adds another state machine for large media. The AWS S3 multipart overview describes initiation, upload of parts, and completion, and notes that uploaded parts continue to incur storage charges until the multipart upload is completed or stopped. Record the multipart identifier with the pending upload, make repeated part submissions harmless at the application boundary, and give stale pending work a bounded cleanup path. A cleanup schedule without oldest-pending age and outcome metrics is just hope — this is exactly where a missed job turns into retained debris rather than a visible request failure.

The preventative path can remain provider-neutral. This Go sketch makes the trusted tenant context and idempotent repository operation explicit; signing details stay inside the selected adapter.

```go
package media

import (
	"context"
	"errors"
	"fmt"
)

type Principal struct {
	TenantID string
	UserID   string
}

type PendingUpload struct {
	ID        string
	TenantID  string
	ObjectKey string
	State     string
}

type UploadGrant struct {
	UploadID string
	URL      string
}

type Repository interface {
	CreatePending(ctx context.Context, upload PendingUpload) error
	FindPending(ctx context.Context, tenantID, uploadID string) (PendingUpload, error)
	MarkReadyOnce(ctx context.Context, tenantID, uploadID string) error
	FindReadyKey(ctx context.Context, tenantID, mediaID string) (string, error)
}

type ObjectSigner interface {
	SignUpload(ctx context.Context, key string) (string, error)
	SignDownload(ctx context.Context, key string) (string, error)
}

type Service struct {
	repo   Repository
	signer ObjectSigner
	newID  func() string
}

func (s *Service) BeginUpload(ctx context.Context, p Principal) (UploadGrant, error) {
	if p.TenantID == "" || p.UserID == "" {
		return UploadGrant{}, errors.New("authentication required")
	}

	uploadID := s.newID()
	key := fmt.Sprintf("tenants/%s/media/%s", p.TenantID, uploadID)
	pending := PendingUpload{
		ID: uploadID, TenantID: p.TenantID, ObjectKey: key, State: "pending",
	}
	if err := s.repo.CreatePending(ctx, pending); err != nil {
		return UploadGrant{}, fmt.Errorf("create pending upload: %w", err)
	}

	url, err := s.signer.SignUpload(ctx, pending.ObjectKey)
	if err != nil {
		return UploadGrant{}, fmt.Errorf("sign upload: %w", err)
	}
	return UploadGrant{UploadID: uploadID, URL: url}, nil
}

func (s *Service) CompleteUpload(ctx context.Context, p Principal, uploadID string) error {
	pending, err := s.repo.FindPending(ctx, p.TenantID, uploadID)
	if err != nil {
		return fmt.Errorf("load tenant upload: %w", err)
	}
	return s.repo.MarkReadyOnce(ctx, pending.TenantID, pending.ID)
}

func (s *Service) DownloadURL(ctx context.Context, p Principal, mediaID string) (string, error) {
	key, err := s.repo.FindReadyKey(ctx, p.TenantID, mediaID)
	if err != nil {
		return "", fmt.Errorf("load tenant media: %w", err)
	}
	return s.signer.SignDownload(ctx, key)
}
```

The interface deliberately omits raw tenant and key choices from browser input. Production policy still has to validate file type and size, verify the transferred object before publication, and define capability duration. Those values belong to the SaaS policy and threat model; I'm not sure there is a responsible universal default. Integration tests against the exact candidate configuration settle the signing behavior better than an “S3-compatible” label does.

## Exercise isolation before comparing storage services

Run one acceptance suite against every candidate. Start with cross-tenant denial: authenticate as tenant A, present tenant B's media ID, and confirm that no download capability is produced. Repeat the test at upload completion, multipart continuation, replacement, and deletion boundaries. Then race two completion requests and verify that one logical media record becomes ready. Interrupt a multipart transfer and verify that cleanup can identify and stop the abandoned work without touching another tenant's upload.

Browser behavior deserves a separate test lane. CORS is an HTTP-header mechanism that lets servers indicate which origins a browser may permit to load resources; some cross-origin requests trigger a preflight request. Configure the required application origins, methods, and headers, then test from a browser rather than assuming a successful server-side signing test proves the upload flow. Keep CORS diagnosis separate from application authorization. They protect different boundaries.

Selection should follow evidence from that suite. Compare the candidates on the exact private signing operations, multipart lifecycle, CORS behavior, access evidence, regional constraints, and failure semantics your application uses. SDK convenience is secondary for a Node.js service if it hides behavior the on-call engineer must later explain. Likewise, “compatible” is useful shorthand, not proof that every operation, signature rule, or lifecycle behavior matches another implementation.

No vendor wins this decision on a checklist alone.

Observe the business transition as well as storage calls. Logs should join an internal tenant ID, upload ID, media ID, operation, and correlation ID without recording signed query strings. Metrics should distinguish grants created, completions accepted, duplicate completion attempts, oldest pending age, multipart cleanup outcomes, and browser-reported CORS failures. During an incident, those signals answer whether bytes failed to move, publication failed to converge, or authorization rejected the caller. One generic upload-failure counter cannot.

## Know when direct transfer is the wrong boundary

Direct browser transfer is not suitable when every byte must pass through synchronous application-controlled inspection before durable storage, when an issued read capability must be revocable immediately, or when supported clients cannot implement the required retry and multipart protocol. Use a controlled upload or delivery gateway in those cases and accept the bandwidth, scaling, and operational burden. Preserve tenant-derived naming and idempotent completion behind it.

For tightly bounded avatars, application-proxied upload may also be the simpler operational choice. If production file sizes are small and predictable, removing a separate browser-to-storage protocol can matter more than avoiding proxy traffic. Large B2B media changes that balance because slow transfers occupy the application data path; your mileage may vary, and interruption tests with representative files are the evidence needed to decide.

The decision rule is narrow: choose the object storage configuration that passes the tenant-isolation suite and whose capability, multipart, CORS, and audit behavior the team can operate. Private-by-default objects and presigned transfer are mechanisms. The real control is keeping tenant identity in every state transition, including the boring retry paths.

## References

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
