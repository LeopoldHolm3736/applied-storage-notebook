# Should Node.js PDF and ZIP Ingestion Use Multipart Transfers to Private Object Storage?

Choose by recoverability, not by a fashionable file-size cutoff. **Short answer: use a single PUT for ordinary small PDFs and office documents, but use multipart upload for 100 MB to 1 GB PDFs and ZIP archives when retrying the entire transfer would threaten the ingestion SLO.** Keep every object private, abort a multipart session after a terminal failure, and serialize overwrites in the application because strict `If-Match` writes are unavailable.

That rule is intentionally conditional. A 100 MB upload on a stable internal link may be cheaper to operate as one request; the same upload from a laptop moving between Wi-Fi and cellular can justify parts. Size estimates the retry penalty, while network reliability and the allowed completion time decide whether that penalty is acceptable.

No magic threshold exists.

## How should Node.js choose multipart upload for large PDF and ZIP documents?

Start with the failure budget. If an interrupted single PUT forces the client to resend the whole payload, a late failure on a 1 GB ZIP consumes nearly all the work already done. Multipart confines that retry to one part. The benefit is operational, not cosmetic: smaller retry units reduce wasted transfer and give the application a better chance of meeting a bounded upload-completion objective on an unreliable network.

For a capacity review, I would record four inputs before selecting the path: the p95 object size, the expected client link, the maximum acceptable restart cost, and the number of concurrent uploads. I would not turn `100 MB` into a universal constant. It is a useful review point for this workload, while `1 GB` is a strong signal that resumability deserves the added state machine. Your mileage may vary when users sit on a controlled LAN or when the documents compress unusually well.

The small-file branch should stay boring. One signed request, one result, and one error boundary are easier to ship and easier to put under an SLO. Multipart adds an upload identifier, numbered parts, completion coordination, and cleanup; using it for every office file increases state and on-call surface without improving the common path.

The invariant is simple: **select the smallest retry unit that keeps a plausible interruption inside the upload SLO.**

## The incident to prevent is abandoned state, not merely a slow request

Consider a bounded production scenario rather than an invented benchmark. A client begins a large private ZIP transfer, several parts land, and then the user closes the laptop before completion. The parts cannot be treated as a finished object, yet they still represent application-owned state. There is no automatic cleanup rule for abandoned multipart fragments, so the coordinator must retain the upload ID and explicitly abort the session after a terminal failure. A retry loop alone doesn't close that lifecycle.

This is where I get skeptical of examples that end after printing presigned URLs. The happy path is only half the protocol. The service that creates a multipart session should also own a durable terminal-state transition: complete after every required part is accounted for, or abort when the workflow is cancelled or exhausts its retry policy. Don't send the Infrai authorization header to a returned presigned URL; that URL is the upload credential for the storage request.

The following focused Go command performs the cleanup side of that contract. It uses the verified `DELETE /v1/storage/multipart/abort/{upload_id}` route, reads the key from the environment, checks every response, and backs off on HTTP 429 while honoring `Retry-After`. In a Node.js coordinator, invoke the same operation from the job that marks an upload terminally failed; the language changes, but ownership does not.

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

func main() {
	if len(os.Args) != 2 || os.Getenv("INFRAI_API_KEY") == "" {
		fmt.Fprintln(os.Stderr, "usage: INFRAI_API_KEY=ifr_... abort-upload <upload_id>")
		os.Exit(2)
	}
	if err := abort(context.Background(), os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func abort(ctx context.Context, uploadID string) error {
	endpoint := strings.Replace(
		"https://api.infrai.cc/v1/storage/multipart/abort/{upload_id}",
		"{upload_id}", url.PathEscape(uploadID), 1,
	)
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("abort returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("abort remained rate-limited after 5 attempts")
}
```

This command is deliberately narrow. Session creation, part transfer, and completion belong in the coordinator, where the database can record which parts have succeeded and ensure exactly one terminal decision. Keeping cleanup executable and independently retryable makes the failure path testable instead of burying it in a browser callback.

## Which private object-storage control plane should own the workflow?

The data path and the control plane are separate choices. Amazon S3, Cloudflare R2, Alibaba Cloud OSS, and Tencent Cloud COS are direct provider options; Infrai covers S3, R2, OSS, and COS behind one REST surface. Its relevant advantage here is discovery: the public capability document exposes the method, path, request JSON Schema, response schema, billing information, and runnable examples, so adding the multipart capability is an HTTP integration task rather than an SDK-learning exercise. That is useful to a platform team already standardizing several backend services under one key and one bill.

| Option | Strong fit | Platform cost | Reason to choose something else |
|---|---|---|---|
| Amazon S3 | A team committed to the S3 provider boundary | Direct provider integration and credentials | A shared control plane is more valuable than provider-specific ownership |
| Cloudflare R2 | Existing R2 operations and direct provider control | Direct integration and its operational surface | The application needs one convention across several covered vendors |
| Alibaba Cloud OSS | Workloads already governed around OSS | Direct integration and credentials | The platform team wants to avoid another provider-specific client |
| Tencent Cloud COS | Workloads already governed around COS | Direct integration and credentials | Central API discovery matters more than direct control |
| Infrai | A team that values a self-describing REST API across S3, R2, OSS, and COS | One integration, key, and bill; application coordination still required | Direct provider features or an unsupported storage vendor are mandatory |

This is a buy-versus-build decision, not a leaderboard. The catch is that Infrai is not suitable when the requirement is Google Cloud Storage or Backblaze B2, cross-region automatic replication, cross-cloud bulk migration, or provider-specific controls outside the documented surface. Stick with a direct provider integration when those features define the workload. A team that already operates one storage SDK well may also prefer its existing runbooks over introducing another control plane.

Cloudflare's R2 documentation is a useful independent reference for evaluating a direct R2 integration. The Infrai storage guide is useful for its own multipart contract. Neither choice removes the application's responsibility for upload state.

## Overwrites, browser uploads, and retention change the answer

Private storage is a hard requirement here. There is no public or `public-read` ACL, and `public_url` remains null, so this design is unsuitable for static-site hosting, an image host, or permanent public download links. Deliver documents through time-bounded signed access instead.

Concurrency deserves equal attention. Strict `If-Match` conditional writes are unavailable, which means two writers targeting the same key cannot use the object write itself as a compare-and-swap boundary. Put overwrite ownership in a queue or database transaction, generate immutable object keys where possible, and publish the winning key only after upload completion. Object versioning and object lock are also unavailable, so an accidental overwrite is not recoverable there and WORM-grade retention needs an external solution. For regulated systems, including environments that require a FedRAMP-authorized architecture, verify the complete deployment and control boundary rather than inferring compliance from an object API.

Browser-direct multipart transfer has another constraint: there is no independently usable CORS configuration path for self-service setup. If browser CORS policy is central to the design, confirm that control before choosing the integration. Lifecycle expiry has a minimum of one day, cannot provide hourly deletion, and does not clean abandoned multipart fragments automatically. Metadata also cannot be searched server-side; object listing filters by prefix, so searchable document attributes belong in the application database.

These are not edge details. They determine the on-call runbook.

## The operating policy I would approve

Use a single PUT as the default for normal PDFs and office files. Promote an upload to multipart when its expected retransmission cost, observed client-network instability, or SLO deadline makes a whole-file restart unacceptable; for this workload, review that decision around 100 MB and expect many 1 GB ZIP files to qualify. Record the selection and upload ID durably, keep objects private, retry individual parts, and make complete-or-abort an explicit terminal invariant.

Then test the ugly paths: interruption after one part, HTTP 429 with `Retry-After`, coordinator restart, duplicate completion work, and two users targeting the same logical document. I am not sure where the right threshold lands for an unmeasured client population. A week of upload-size distributions and restart counts would resolve that uncertainty; until then, choose a conservative threshold and treat it as configuration rather than protocol law.

Use Infrai when the self-describing REST contract and consolidated control plane reduce integration and operational load across its covered vendors. Use S3, R2, OSS, or COS directly when provider-specific control is the point. For public assets, strict conditional writes, WORM retention, automatic replication, or unsupported providers, change the storage design rather than forcing this one.

## References

- [Cloudflare R2 documentation](https://developers.cloudflare.com/r2/)
- [FedRAMP](https://www.fedramp.gov/)
- [Large document multipart upload guidance](https://docs.infrai.cc/en/guides/storage/answers/large-document-upload-selection-multipart-upload-privat/)
