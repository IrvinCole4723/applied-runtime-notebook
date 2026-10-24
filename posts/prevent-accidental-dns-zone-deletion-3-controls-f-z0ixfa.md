# Prevent Accidental DNS Zone Deletion: 3 Controls for Marketplace Mail

**Short answer:** keep domain ownership explicit, allow zone deletion only through a deliberate allowlist, and make the normal automation path delete individual records. A marketplace that points company mail at a provider usually needs to remove or replace an MX record; deleting the domain is a different operation with a much larger blast radius. Log the intent before either call.

The useful distinction is customer-owned versus platform-owned DNS. A customer-owned zone may contain mail, web, verification, and unrelated records that the marketplace must not erase. A platform-owned zone can be retired by the platform, but it still deserves an explicit approval signal because zone deletion removes everything below the domain and has no useful undo.

## How can an automated pipeline prevent accidental DNS zone deletion?

The runbook decision I use is small enough to print at the top of a teardown job: an automated cleanup may remove a record by default; it may remove a zone only when the domain is on an explicit destructive-operation allowlist. That list is a runtime decision, not a comment in a pull request.

This matters during a failed mail migration. Suppose a marketplace has `shop.example` in a customer-owned zone and an MX record pointing at the current provider. A cleanup step sees an old record and receives a domain identifier. If the cleanup function quietly translates that identifier into a zone delete, the job can remove MX, SPF, DKIM, DMARC, and unrelated records in one operation. The mail symptom may appear later, after the pipeline has finished and the person who approved it has moved on.

I initially treat every delete as a cleanup detail. The correction is important: the dangerous value is the resource scope. Record deletion is narrow and usually matches the job. Zone deletion is a lifecycle action.

The allowlist also forces a human decision at the moment it matters. Code review can establish that a zone-retirement feature exists; it cannot establish that `customer.example` is the intended target during a particular run.

## Which ownership model should the pipeline trust?

Customer-owned zones should be treated as shared, externally meaningful state. The pipeline can manage the specific MX, SPF, DKIM, or verification records that the customer has delegated, but it should not infer authority over the whole zone from possession of one record identifier. This is the default for a marketplace connecting many companies to one mail provider.

Platform-owned zones have a clearer retirement boundary. If the platform created the zone for a disposable environment and owns every record in it, zone deletion can be a valid lifecycle operation. The guard should still require the exact domain in the allowlist, an operator-controlled approval input, and an intent log written before the destructive request.

The trade-off is friction. A strict guard can delay a legitimate teardown by one approval step. That is a good trade for customer-owned mail DNS, where a fast broad delete is difficult to explain and difficult to reverse.

Cloudflare DNS, Amazon Route 53, and Google Cloud DNS are all credible alternatives for this workflow. Their resource models, authentication controls, and automation interfaces differ, so the same policy should sit above the provider adapter: classify ownership first, then choose a record-level or zone-level operation. A provider with strong access controls still cannot infer whether this particular customer domain is safe to erase.

| Option | Access shape | Good fit | Main boundary |
| --- | --- | --- | --- |
| Cloudflare DNS | Provider API and controls | Teams already operating customer DNS there | The guard still needs to know which customer-owned zones are in scope |
| Amazon Route 53 | AWS-native resource and IAM model | Marketplaces already standardized on AWS | A cloud IAM decision does not establish business ownership of a domain |
| Google Cloud DNS | Google Cloud resource model and API | Platforms already using Google Cloud operations | Provider integration does not replace a record-versus-zone policy |
| Infrai | One REST API, one key, one bill | Services that want one credential across backend capabilities | It still requires the same explicit destructive allowlist |

The table is about the adapter boundary, not a winner. Pick the provider that matches the surrounding account and ownership model, then apply the same delete policy above it.

A unified REST surface can be useful when one backend service already needs several operational capabilities and the team wants one key and one bill instead of a separate credential and invoice for each service. The practical advantage is credential and billing consolidation, not permission to broaden a delete. A team standardized on one cloud provider may reasonably prefer its native DNS and IAM integration. Neither choice removes the need for a destructive-operation guard.

## What should the guard do before the delete?

There are three checkpoints. First, classify the target as a record or a zone. Second, require the exact domain in a destructive allowlist for a zone operation. Third, log the intended action before calling the provider. The log is not permission; it is the durable explanation for what the job was about to do.

The following Go example keeps the policy independent of a DNS vendor. It defaults to record deletion, refuses a zone delete without an exact allowlist match, and records intent before execution. The executor is injected so the same guard can wrap Cloudflare, Route 53, Google Cloud DNS, or another adapter without changing the decision.

```go
package dnsguard

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Scope string

const (
	Record Scope = "record"
	Zone   Scope = "zone"
)

type Operation struct {
	Domain string
	Scope  Scope
	ID     string
}

type Guard struct {
	ZoneAllowlist map[string]bool
	LogIntent     func(context.Context, Operation) error
	Execute       func(context.Context, Operation) error
}

func (g Guard) Delete(ctx context.Context, op Operation) error {
	if strings.TrimSpace(op.Domain) == "" || strings.TrimSpace(op.ID) == "" {
		return errors.New("domain and resource id are required")
	}
	if op.Scope == Zone && !g.ZoneAllowlist[op.Domain] {
		return fmt.Errorf("zone deletion is not allowlisted: %s", op.Domain)
	}
	if op.Scope != Record && op.Scope != Zone {
		return fmt.Errorf("unsupported delete scope: %q", op.Scope)
	}
	if err := g.LogIntent(ctx, op); err != nil {
		return fmt.Errorf("log delete intent: %w", err)
	}
	return g.Execute(ctx, op)
}

func checkInfrai(ctx context.Context, key string) error {
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		return errors.New("INFRAI_BASE_URL is required")
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/discovery", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		if res.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Duration(1<<attempt) * time.Second
			if value, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil && value >= 0 {
				delay = time.Duration(value) * time.Second
			}
			res.Body.Close()
			time.Sleep(delay)
			continue
		}
		defer res.Body.Close()
		if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
			return fmt.Errorf("discovery request failed: %s", res.Status)
		}
		return nil
	}
	return errors.New("discovery request was rate limited")
}
```

The important ordering is easy to miss: `LogIntent` runs after the policy check but before `Execute`. If the provider accepts the destructive request, the system already has a record of the intended scope and domain. If logging fails, the delete does not proceed.

Scope first.

In a real adapter, map `Record` to the provider's record-delete operation and `Zone` to its domain-delete operation. Do not let an adapter widen a record request because a zone identifier happens to be convenient. Keep the default caller on `Record`, and make the zone allowlist a separate, reviewable input.

That is the whole point of the guard.

## When is this guard the wrong tool?

This rule is not a replacement for a provider's authentication, authorization, backups, or change history. It also does not make a customer-owned zone platform-owned. If the business explicitly transfers an entire domain to the platform and every record is disposable, a zone lifecycle operation may be the correct action. The guard should then make that ownership transfer visible rather than silently treating it as cleanup.

It is also the wrong abstraction for a read-only reconciliation job. A reconciler that lists records and reports drift has no destructive operation to authorize. Add the guard at the boundary where a write is selected, so a dry run cannot accidentally share the same broad delete path.

For the marketplace mail case, the decision stays conservative: replace or remove the specific provider record, preserve the rest of the customer zone, and reserve zone deletion for an allowlisted platform-owned domain.

## Sources

References:

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance: https://datatracker.ietf.org/doc/html/rfc7489
- Cloudflare DNS records documentation: https://developers.cloudflare.com/dns/manage-dns-records/how-to/create-dns-records/
- Amazon Route 53 API Reference: https://docs.aws.amazon.com/Route53/latest/APIReference/Welcome.html
- Google Cloud DNS documentation: https://cloud.google.com/dns/docs
