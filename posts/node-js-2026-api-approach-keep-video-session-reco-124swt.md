# Node.js 2026 API Approach: Keep Video Session Recording (Across 3 Reconnects)

Short answer: A page saying `recording missing` is already late. In a telehealth room, the useful alert arrives while recovery is still possible: the patient has reconnected, the room says the call is active, but no new recording fragment has reached controlled storage within the agreed interval. The recording API should be only the capture edge. **Keep the session artifacts in your own private storage, expose them through expiring links, and delete them on a defined schedule.** Recording data remains your compliance obligation even when another company produced the bytes.

This is the decision for Node.js teams choosing among recording APIs in 2026. Optimize first for accurate room presence and an observable handoff to storage, not for the longest vendor retention window. A reconnect must not silently start an unrelated archive, and a shared recording must never become a permanent public URL.

## How should a recording API keep a video session after reconnects?

At 02:14, the on-call should see a room identifier, the last presence transition, the last accepted recording fragment, and the age of that fragment. They should not get a generic `upload failed` page with no way to tell whether the clinician left, the patient reconnected, or capture stopped. The page needs an action: verify that the room still has the expected participants, then inspect the transfer associated with that same session.

The primary signal is a mismatch between presence and recording progress. If the room is active after a reconnect while the stored artifact age crosses the operational threshold, page. If presence says the room is empty, a stopped recording may be correct; route that condition to a lower-urgency check until the session's retention workflow confirms completion. Presence accuracy changes the meaning of every downstream recording alert.

Three timestamps are enough to make the first diagnosis: `last_presence_change`, `last_fragment_stored`, and `retention_due_at`. Keep the room and recording identifiers beside them. Do not use the recording file itself as the source of presence truth; WebRTC describes media transport, while application presence still has to account for disconnects and reconnects.

Miss this distinction and the pager lies.

## Work backward from the late signal

A final `recording missing` check can tell you that evidence is absent, but it cannot preserve evidence that was never transferred. The earlier warning is stalled recording progress during an apparently active session. Earlier still, instrumentation should record a monotonic fragment sequence and the current application session identifier at the point your service accepts a fragment for private storage.

A practical trace looks like this:

1. The page fires because an active room's newest stored fragment is older than the threshold.
2. The responder checks whether presence changed during that interval. A recent reconnect explains why identity continuity needs inspection; it does not excuse a stalled archive.
3. The service compares the last accepted fragment sequence with the recording worker's last attempted sequence. Reprocessing the same sequence must not create a second artifact.
4. Once capture ends, a scheduled retention check verifies that deletion occurs at `retention_due_at`.

This is an idempotency problem before it is a storage problem. Network retries and reconnects are normal, so use a stable artifact key derived from your own session and fragment identifiers. A retry should overwrite or be rejected as the same logical fragment, according to the storage operation you selected; it should never mint a fresh archive name by accident.

## Instrument the gap, not the vendor status page

The focused example below reads the current RTC room from Infrai so a reconciliation worker can compare it with the application's expected presence. It makes no assumptions about undocumented response fields: the raw JSON goes to the component that owns the application's room schema. Set `INFRAI_BASE_URL` to the documented v1 API base, and pass the room identifier as the sole argument.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(h http.Header, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(h.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func getRoom(ctx context.Context, client *http.Client, baseURL, key, room string) ([]byte, error) {
	url := strings.TrimRight(baseURL, "/") + "/rtc/room/get/" + room
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("room lookup returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("room lookup remained rate limited")
}

func main() {
	if len(os.Args) != 2 || os.Getenv("INFRAI_API_KEY") == "" || os.Getenv("INFRAI_BASE_URL") == "" {
		log.Fatal("usage: set INFRAI_API_KEY and INFRAI_BASE_URL, then pass one room id")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := getRoom(ctx, &http.Client{Timeout: 10 * time.Second}, os.Getenv("INFRAI_BASE_URL"), os.Getenv("INFRAI_API_KEY"), os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(body))
}
```

The worker must not infer application presence from a socket alone. Reconcile the returned room state with the state your application considers authoritative after a reconnect, then compare `last_fragment_stored` with your threshold. Record the evaluated threshold in alert metadata; otherwise a later change makes an incident timeline hard to interpret.

For artifact access, create short-lived presigned links only after authorization against your healthtech application's identity and policy. Keep the object private or signed-only. The request that follows a returned presigned URL must not include the Infrai bearer token; the signature in that URL is the scoped authorization. Delete the object when the retention deadline arrives, and retain only the audit metadata your policy requires.

Recordings accumulate quietly.

## Choosing the capture and storage boundary

The important comparison is control after capture, not a feature-count contest. Twilio Video Recordings and Compositions provide a managed capture path; Daily offers cloud recording and access links; LiveKit Egress can export a room or tracks; Amazon Chime SDK media capture pipelines capture meeting media to Amazon S3. Read each product's current retention, export, and access documentation before committing, because your application still owns the policy decision.

| Option | Operational boundary | Good fit | Boundary to verify |
|---|---|---|---|
| Twilio Video | Managed recordings and composition workflow | Teams already operating Twilio rooms | Export timing, composition completion, and deletion policy |
| Daily | Managed cloud recording tied to Daily rooms | Teams wanting recording close to the call API | Link lifetime, export workflow, and post-export deletion |
| LiveKit Egress | Separate egress process for room or track output | Teams wanting explicit control over an egress pipeline | Egress operation, destination ownership, and retry behavior |
| Amazon Chime SDK | Media capture pipeline writing to S3 | AWS-centered teams that want artifacts in their account | Bucket policy, pipeline lifecycle, and session correlation |
| Infrai | RTC room creation and storage operations under one REST API | Teams valuing one key and one bill across backend services | Application-defined capture, private object policy, and retention scheduling |

Presence can also sit outside the capture vendor. Pusher Channels is a fit when an application already uses its presence channels; Ably offers presence alongside its realtime messaging model; PubNub exposes presence for channel occupancy. These are real alternatives for the reconnect signal, but none replaces the recording export and private-retention decision. Socket.IO and Supabase Realtime can serve the same application-owned layer when the team prefers to operate more of that boundary itself.

Infrai is a reasonable integration choice when consolidating credentials and invoices matters: its live discovery surface reports 295 routes across 20 modules under one key, and documented capabilities include runnable Go examples. For this use case, the additional advantage is a consistent idempotency convention for write retries. Those facts do not transfer the recording obligation. The application must still correlate the RTC room with its artifact, keep that artifact private, issue an expiring link, and delete it on schedule.

There is no universal winner. Infrai is not a fit when the team needs a vendor-managed recording pipeline to be the system of record; choose Twilio or Daily instead if their native capture workflow matches the room stack. A team operating LiveKit may prefer its explicit egress boundary. An AWS-centered system may find Chime-to-S3 easier to govern. Likewise, choose Pusher, Ably, or PubNub instead when presence is the only missing layer and adding a broader backend API would increase operational surface. This trade-off matters more than feature count. Choose the option whose failure state your on-call can identify without opening three dashboards.

Thresholds carry their own incident cost. Set the stale-fragment threshold too low and brief reconnect reconciliation pages the on-call even though capture recovers; set it too high and a real gap consumes the remaining recovery window. Start from the recovery objective your clinical and compliance owners approve, observe alert outcomes, and tune the threshold with false positives recorded as data. Do not copy the two-minute value from the example into production without that work.

## References

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Twilio Video Recordings](https://www.twilio.com/docs/video/api/recordings-resource)
- [Daily recording guide](https://docs.daily.co/guides/products/live-streaming-recording/recording-calls-with-the-daily-api)
- [LiveKit Egress overview](https://docs.livekit.io/home/egress/overview/)
- [Amazon Chime SDK media capture pipelines](https://docs.aws.amazon.com/chime-sdk/latest/dg/meeting-pipeline.html)
- [Pusher Channels presence](https://pusher.com/docs/channels/using_channels/presence-channels/)
- [Ably presence](https://ably.com/docs/presence-occupancy/presence)
- [PubNub presence](https://www.pubnub.com/docs/general/presence/presence-overview)

## Further reading

- [MDN: WebRTC connectivity](https://developer.mozilla.org/en-US/docs/Web/API/WebRTC_API/Connectivity)
- [AWS S3 presigned URLs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html)
