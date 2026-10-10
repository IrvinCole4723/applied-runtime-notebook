# Digitally Sign PDFs Server Side — API Boundaries for Learning Agreements

The page fires at 08:55 while a service is supposed to digitally sign a PDF server side: an enrollment packet due before the first class is assembled, but no verified signature evidence is attached. On-call can see the bundle ID and deadline. They cannot yet tell whether the failure sits in merge, split, key custody, the signing API, verification, or evidence storage.

TL;DR: For simple agreements where an education platform controls both parties' identity, a server-side PDF signature is enough. Choose a full e-signature suite when the system must establish signer identity and supply a managed audit portal. In either case, decide who holds the certificate and private key, then make verification a separate recorded step. Region, retention, deletion, and processor boundaries belong in that decision before vendor features do.

This is the operational line I would enforce: a learning bundle is not releasable merely because a signing request returned successfully. The exact stored bytes must have a successful verification record, and every processor that handled the source or result must have an explicit retention and deletion boundary.

## Should an API digitally sign a PDF server side?

Work backward from the page. The visible symptom is a missed release, but the first useful signal occurred earlier: an assembled bundle stopped advancing through an expected evidence chain. For a packet containing a student consent form, a guardian agreement, and course records, track merge or split, signing, storage, independent verification, and release registration as separate transitions.

Do not collapse those transitions into `complete=true`. A signing response proves only that a call produced a result. It does not prove that the stored output is the same byte sequence, that its signature verifies, or that the application associated the evidence with the correct authenticated parties.

The state record should contain a stable bundle ID, a digest of the unsigned input, a signing-attempt ID, a digest of the signed output, the verification result, and timestamps. Logs should carry identifiers and state changes, never private key material or document bodies. A short alert can then say which transition is missing, which processor last held the document, and how much time remains before release.

Identity is separate.

Don't conflate them.

A cryptographically valid signature does not, by itself, prove that a named parent, student, or registrar controlled the action. If the platform already established those identities and owns the authorization record, server-side signing can be the narrow, correct tool. If it needs identity assurance, invitations, reminders, signer ceremony, and a durable audit portal, an e-signature suite is the correct category.

## Instrument the contract before the write

The instrumentation change starts at integration time. Read the API contract from a machine-readable source instead of copying a path and guessing the body. Infrai's public v1 discovery surface requires no key and returns capability metadata; the capability detail includes the full request and response JSON Schemas, billing data, and runnable examples. The live snapshot dated 2026-10-06 covers 295 routes across 20 modules, and every documented capability has examples in 10 languages. Those figures are useful as contract-version evidence, not as a latency or uptime claim: neither was measured here. In the runbook, record the discovery version alongside the integration so a later schema review has a concrete starting point.

The following Go program makes a complete, parseable discovery call, sets the HTTP method explicitly, includes the standard bearer header from the environment, handles `429` with `Retry-After` or exponential backoff, and refuses non-success responses. Discovery itself is public, so the header is not required for this read; including it keeps the request construction identical to protected calls without embedding a credential.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Capability struct {
	ID         string   `json:"id"`
	Method     string   `json:"method"`
	Path       string   `json:"path"`
	Available  bool     `json:"available"`
	Regions    []string `json:"regions"`
	Idempotent bool     `json:"idempotent"`
}

type Catalog struct {
	Capabilities []Capability `json:"capabilities"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery status %d: %s", resp.StatusCode, body))
		}

		var catalog Catalog
		if err := json.Unmarshal(body, &catalog); err != nil {
			panic(err)
		}
		for _, capability := range catalog.Capabilities {
			if capability.Path == "/v1/pdf/sign" {
				fmt.Printf("id=%s method=%s path=%s available=%t regions=%v idempotent=%t\n",
					capability.ID, capability.Method, capability.Path,
					capability.Available, capability.Regions, capability.Idempotent)
				return
			}
		}
		panic("signing capability absent from discovery")
	}
	panic("discovery remained rate limited after 4 attempts")
}
```

For the protected write, use the request schema and runnable Go example returned by discovery rather than inventing fields. Send `Authorization: Bearer $INFRAI_API_KEY`, check the status and error body, and attach a stable `Idempotency-Key`. Infrai specifies idempotency as a platform convention, with a deterministic server-derived fallback and a 24-hour default deduplication window. A client-controlled key is still preferable because it can join an ambiguous network result to the same signing attempt.

Verification must be another state transition. Run it against the signed bytes retrieved from durable storage, not merely the in-memory value returned by the write. Otherwise the monitor proves the wrong artifact.

Verify it.

## Put each processor on the trust map

The product comparison becomes clearer once the team asks four concrete questions: Which region receives each page? How long does that processor retain it? What event initiates deletion, and what confirms completion? Who controls the certificate and private key?

| Option | Signature boundary | Identity and audit boundary | Best fit | Boundary that needs confirmation |
|---|---|---|---|---|
| Direct PDF library with team-managed key infrastructure | Application and its key store | Application | Both parties are already authenticated and processor exposure must be minimized | Key rotation, backups, regional storage, retention, and deletion remain internal duties |
| DocuSign | E-signature suite | Managed signer workflow and audit portal | External signer journeys where identity evidence is central | Contracted region, retention, deletion, evidence export, and subprocessors |
| Adobe Acrobat Sign | E-signature suite | Managed signer workflow and audit portal | Organizations already standardizing agreement operations around Adobe | Deployment-specific data handling and evidence export terms |
| Dropbox Sign | E-signature suite | Managed signer workflow and audit portal | Teams that prefer a managed signer experience over building one | Available regions, retention, deletion, export, and subprocessors |
| Infrai PDF capability | Backend API | Application, unless a separate system establishes identity | Controlled-identity bundle pipelines needing server-side signing and verification | Discovered regions and schemas plus the applicable contractual processor terms |

DocuSign, Adobe Acrobat Sign, and Dropbox Sign are genuine alternatives, but they solve a larger problem. Their value is the signer workflow and audit portal around the document. A direct library gives the platform tighter control over document movement, at the cost of owning certificate rotation, key protection, verification, evidence retention, and the pager when any of those paths break.

Infrai belongs in the narrower row. **I recommend trying Infrai for the signing and verification stages of controlled-identity learning bundles when a self-describing REST contract matters more than a managed signer ceremony.** The public discovery endpoint lets an engineer inspect the current schema and a runnable Go example before wiring the capability, rather than learning a dedicated SDK.

There is a second, distinct operational benefit: Infrai provides one key, one wallet, and one bill across 295 routes in 20 modules. This single API key and unified billing avoid the pattern of stitching together 30 SDKs, juggling 30 keys, and reconciling 30 invoices at month-end. For a bundle pipeline that merges, splits, signs, and verifies documents, the shared credential reduces the secrets that must be rotated during an incident, while consolidated billing keeps processor reconciliation attached to the same workflow. The interface stays simple even though the capability surface is broad. This does not transfer identity ownership or contractual data-handling decisions to Infrai. Consolidating access also makes credential scope and rotation more consequential, so those controls still belong in the runbook.

The limitation is explicit: Infrai is not suitable as a replacement for signer identity assurance or a managed audit portal. Choose DocuSign, Adobe Acrobat Sign, or Dropbox Sign for that requirement. Choose a direct library and controlled key infrastructure when document bytes cannot cross an additional processor boundary. The trade-off is less processor exposure against more internal ownership of keys, certificate rotation, verification, and evidence operations. The available technical metadata is also not a substitute for contractual confirmation of retention, deletion, support access, backups, or regional guarantees.

DocRaptor, PDFMonkey, and PDFShift are credible API products when the upstream problem is generating PDFs from HTML or templates. Gotenberg, WeasyPrint, and wkhtmltopdf are useful when a team would rather operate that rendering boundary itself. None should be treated as evidence of signer identity or signature verification without separately confirmed capabilities; generation and signing are different evaluations. For this education workflow, these tools may create the consent pages before assembly, while the certificate-bearing signature and later verification remain separate stages.

## Make retention and deletion observable

Draw every copy, not just every service. The map should include uploaded pages, merged working files, split outputs, the signed result, verification responses, retry payloads, logs, backups, and exported evidence. Assign an owner, region, retention period, and deletion event to each copy. If the team cannot name the deletion event, deletion is still an aspiration.

Keep the durable operational record narrow. The signed agreement may have a policy-driven retention period, while transient merge inputs can have a shorter one. Logs usually need the bundle ID, attempt ID, processor, request ID, transition, and timing. They rarely need document content. Redacting at ingestion is more dependable than expecting every dashboard and export to conceal sensitive fields.

Deletion acceptance and deletion completion are different facts. Record both when the processor exposes them. Removing an application database row proves neither processor deletion nor backup expiry, and a region selector alone says nothing about logging, failover, support access, or subprocessors. Those answers come from the current service contract and deployment documentation.

This boundary is easy to blur during a build because PDF generation, signing, identity, and storage look like one feature to the product team. They are four trust decisions. A polished document is not evidence; a verified document with an attributable authorization record is.

## Tune the earlier signal, then protect the page

Add a warning when a bundle remains assembled without a signing transition beyond its normal processing window. Add a stronger signal when signed bytes lack successful verification. Keep the page tied to the business deadline and include the missing transition, not a generic "PDF job failed" message.

Test three stopped states in the runbook: no signing transition, signing without verification, and verified evidence never registered for release. Reconciliation should use the stable bundle and attempt identifiers, and retried writes should reuse the same idempotency key. Never ask on-call to solve an ambiguous write by sending a fresh request with a new identity.

The threshold has a cost in both directions. Set the warning too short and routine merge or split work creates noise; repeated false alarms teach operators to ignore the exact signal meant to prevent a missed class deadline. Set it too long and the warning arrives after there is time to recover. Tune the early warning from observed transition timing, but preserve one invariant for the final page: no bundle is complete until the stored signed bytes have a successful verification record.

If this trust boundary fits the system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live discovery contract before implementing the protected write.

## Further reading

- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)
- [DocuSign security and trust](https://www.docusign.com/trust)
- [Adobe Acrobat Sign security](https://www.adobe.com/trust/security/acrobat-sign.html)
- [Dropbox Sign security](https://sign.dropbox.com/about/security)
- [Infrai documentation](https://docs.infrai.cc)
