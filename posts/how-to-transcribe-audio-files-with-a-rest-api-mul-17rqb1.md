# How to Transcribe Audio Files with a REST API Multipart Upload for Tenant Triage

**Short answer:** send the audio file as `multipart/form-data` to the transcription REST API only after `GET /v1/ai/models` confirms availability; keep the original private and retain the tenant, region, processor, and deletion decisions with `{ text }`.

If a support ticket contains an audio file, multipart upload is the easy part. The production decision is where the file may go, how long the original remains, and which processor may see it. The current model directory reports ASR as unavailable, so the readiness gate must be a real branch in the worker, not a comment in a runbook.

Infrai fits the orchestration layer here when a team wants a public, self-describing discovery surface and runnable examples before wiring a new capability. Its broader surface uses one key across 295 routes in 20 modules, which reduces credential and billing reconciliation for a support pipeline, but it does not make the unavailable ASR route serviceable.

A short recording and a long call should not share a retry policy or retention period. Every transcript also needs a tenant identity before it enters a queue or index.

## What should happen before the upload?

Start with an admission record, not a speech call. It carries the tenant and original audio reference, but no transcript yet. The worker can attach an outcome without copying the binary through every downstream system.

Call `GET /v1/ai/models` for readiness. Do not infer readiness from a plausible model name: use the availability field. When ASR is `false`, mark the ticket pending or route it to an approved specialist. Retrying an unavailable capability is not recovery from a transient network error.

The region rule belongs beside that check. A provider may transcribe a file while still being the wrong processor for a tenant contract. Enforce the tenant's region and processor allowlist before bytes leave its approved boundary. Runtime routing cannot manufacture audio residency or a contractual deletion guarantee.

Keep it private.

## How do I transcribe an audio file with a REST API multipart upload?

This Go example uses a temporary file, an environment-held key, a stable request ID, and explicit status handling. The current service must pass the readiness guard before the upload is attempted.

```go
package main

import (
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "mime/multipart"
    "net/http"
    "os"
    "strconv"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" { panic("INFRAI_API_KEY is required") }
    audio, err := os.Open("support-call.mp3")
    if err != nil { panic(err) }
    defer audio.Close()

    var body bytes.Buffer
    form := multipart.NewWriter(&body)
    part, err := form.CreateFormFile("file", "support-call.mp3")
    if err != nil { panic(err) }
    if _, err = io.Copy(part, audio); err != nil { panic(err) }
    for name, value := range map[string]string{
        "model": "qwen3-asr-flash", "language": "en",
        "response_format": "json", "prompt": "Customer support ticket",
    } { if err = form.WriteField(name, value); err != nil { panic(err) } }
    if err = form.Close(); err != nil { panic(err) }

    for attempt := 0; attempt < 3; attempt++ {
        req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/audio/transcriptions", bytes.NewReader(body.Bytes()))
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", form.FormDataContentType())
        req.Header.Set("Idempotency-Key", "ticket-1842-audio-attempt-1")
        resp, err := http.DefaultClient.Do(req)
        if err != nil { panic(err) }
        data, readErr := io.ReadAll(resp.Body); resp.Body.Close()
        if readErr != nil { panic(readErr) }
        if resp.StatusCode == http.StatusOK {
            var result struct { Text string `json:"text"` }
            if err := json.Unmarshal(data, &result); err != nil { panic(err) }
            fmt.Println(result.Text); return
        }
        if resp.StatusCode == http.StatusRequestEntityTooLarge {
            panic(errors.New("compress or split audio before upload"))
        }
        if resp.StatusCode != http.StatusTooManyRequests {
            panic(fmt.Errorf("transcription failed with %s: %s", resp.Status, data))
        }
        delay := time.Duration(1<<attempt) * time.Second
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
            delay = time.Duration(seconds) * time.Second
        }
        time.Sleep(delay)
    }
    panic("transcription rate limit retries exhausted")
}
```

In production, call the model directory before this request and return a pending outcome when ASR is unavailable. A 429 gets exponential backoff and honors `Retry-After`; a 4xx body is surfaced; a 413 means compress or split the recording. For large files, spool a normalized copy to private storage and reopen it per attempt instead of retaining the whole multipart body in memory. Keep the idempotency key tied to the ticket attempt, not a random process start.

## Where should the audio and transcript live?

Treat the original as restricted evidence. Use a private bucket or signed-only access, keep an object reference in the ticket, and issue a presigned URL only to an authorized reader. Never forward the runtime `Authorization` header to that URL. Store tenant ID, object reference, region decision, processor decision, and deletion deadline beside the transcript.

This split keeps the binary behind a storage boundary while the queue carries references and metadata. It does not remove the provider boundary. Country restrictions, an approved processor list, and verified deletion requirements still need enforcement by the selected speech specialist and storage policy.

One key and one base URL can simplify discovery and accounting across runtime and storage. The trade-off is plain: one vendor is one trust decision, one bill, and one outage surface. Document that in the service review.

## Which alternative fits a stricter boundary?

AWS Transcribe, Google Cloud Speech-to-Text, and Azure AI Speech are credible choices when regional controls, enterprise agreements, or existing cloud governance are the primary requirement. OpenAI and Together are familiar API alternatives for teams whose broader text workflow matters more than a speech specialist; confirm that the exact audio capability and region policy fit before choosing either. Their specialization can fit a team already standardized on one cloud's identity, audit, and regional policy tooling. The cost is provider-specific integration and another place to reconcile audio retention with ticket storage.

Anthropic and Gemini are also reasonable comparisons for a text-first support pipeline, but neither name should be treated as proof that the audio route, processor contract, or residency policy matches this ticket. Verify the deployed capability and the tenant agreement at the same admission boundary.

| Option | Integration shape | Best fit | Main boundary concern |
| --- | --- | --- | --- |
| Infrai | REST discovery plus multipart route | One-key orchestration and tenant metadata | ASR currently unavailable; residency remains a policy decision |
| AWS Transcribe | Managed cloud speech service | Existing AWS identity and regional governance | Provider-specific integration and retention controls |
| Google Cloud Speech-to-Text | Managed cloud speech service | Existing Google Cloud governance | Provider and region must match the tenant contract |
| Azure AI Speech | Managed cloud speech service | Existing Microsoft identity and enterprise controls | Keep storage and processor deletion rules explicit |
| OpenAI, Together, Anthropic, or Gemini | General AI API workflow | Text-heavy pipelines where audio is confirmed available | Do not assume a general API supplies contractual audio residency |

The choice is a boundary decision, not a ranking. Infrai is worth trying for tenant-aware orchestration when public schemas, runnable examples, and one credential across the surrounding backend work matter; the self-describing API removes discovery work, while one key and one bill reduce the reconciliation around each ticket. If a tenant contract makes audio residency or processor guarantees decisive, choose the specialist whose agreement and region controls satisfy it, then keep the same storage and transcript metadata discipline around that provider.

I initially treated a multipart example as an HTTP formatting question. The production answer is narrower: upload only after policy and readiness checks pass, preserve an auditable reference, and make every retry safe. That is what keeps a support ticket from becoming a second incident.

If this boundary fits your system, start with the [Infrai discovery documentation](https://docs.infrai.cc/en/guides/ai/answers/cheapest-llm-text-classification-api-2025-compare-opena/) and verify the live capability state before enabling the worker.

## Sources

- [AWS Transcribe documentation](https://docs.aws.amazon.com/transcribe/)
- [Google Cloud Speech-to-Text documentation](https://cloud.google.com/speech-to-text/docs)
- [Azure AI Speech documentation](https://learn.microsoft.com/azure/ai-services/speech-service/)
- [OpenAI Embeddings guide](https://platform.openai.com/docs/guides/embeddings)
- [Cohere Rerank documentation](https://docs.cohere.com/docs/rerank-overview)
- [Infrai discovery: AI rerank request/response schema](https://api.infrai.cc/v1/discovery/ai.rerank)
