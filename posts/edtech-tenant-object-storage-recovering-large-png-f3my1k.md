# EdTech Tenant Object Storage: Recovering Large PNG/WebP Upload Timeouts

Short answer: for an edtech system that stores AI-generated images and restores a selected tenant backup, make the upload a resumable state machine and make authorization part of every state transition. Multipart upload addresses a long PNG or WebP transfer; it does not address a tenant reading another tenant's snapshot, an incomplete object being treated as complete, or a restore that silently changes the requested bytes.

That distinction is the useful incident lesson. Delivery simplicity is attractive until one long request crosses a proxy timeout, and access control is easy to describe until a worker resumes an upload from a record whose tenant context was never checked. I would rather operate a few explicit states than debug a plausible-looking image that was never durably committed.

## How should an edtech team recover a generated image upload timeout for large PNG files?

Start with a bounded contract. The caller supplies a tenant identifier, a logical image identifier, the expected media type, and the selected snapshot identifier for restore. The service derives the object key; it does not accept an arbitrary key from a browser. A key such as `tenant/{tenantID}/images/{imageID}/snapshots/{snapshotID}` is useful only when the storage adapter still enforces authorization before every read and write.

For a new upload, create a database record before sending bytes. Record the tenant, image, snapshot, expected length, content type, upload state, and an expiry time. A part receipt belongs to that record, not to a client-supplied upload ID in isolation. On retry, the worker loads the record, checks that the requesting job still owns it, opens a fresh reader for the part, and records the returned part identifier only after the storage service acknowledges the transfer.

The failure modes are ordinary and expensive: a reverse proxy returns 408 while the backend keeps working, a worker dies after part 7, a retry reuses an exhausted reader, or a completion message arrives before all receipts have been committed. None of those events should make the image visible. Visibility follows completion, checksum or size validation, and a database transaction that marks the logical image ready.

Short requests win.

The part size needs a capacity-planning argument rather than a magic number. Larger parts reduce request count and per-request overhead, but they increase retry cost and memory or disk pressure; smaller parts improve retry granularity while increasing metadata and scheduling work. Measure the p95 and p99 transfer time, retry rate, worker concurrency, and incomplete-upload age, then set the timeout above the normal part budget with enough room for backoff. A 408 is transport evidence, not proof that the object was lost.

## How can multipart completion protect tenant backups and restores?

Treat the upload as a small state machine:

| State | Allowed next action | Required guard |
|---|---|---|
| initiated | upload a missing part or abort | tenant and snapshot ownership | 
| receiving | retry a part, record a receipt, or abort | fresh reader and bounded retry budget |
| ready_to_complete | complete the object | all expected parts and integrity checks |
| completed | publish the image reference | one-way transition in a transaction |
| aborted | retain audit metadata | no further byte writes |

The restore path needs the same discipline. Resolve the requested snapshot through a tenant-scoped query, verify that its manifest names the expected object and media type, then stream it to a temporary destination before swapping the application reference. A restore is not complete because storage returned HTTP 200; it is complete when the selected snapshot, tenant, byte count, and digest agree.

Here is the control shape I use in a storage adapter. It deliberately leaves provider calls behind an interface, because the correctness property is the ordering and authorization boundary, not a vendor-specific method name.

```go
package backup

import (
	"context"
	"errors"
)

var ErrNotReady = errors.New("upload is not ready to complete")

type Part struct {
	Number int
	ETag   string
}

type Store interface {
	Complete(ctx context.Context, uploadID string, parts []Part) error
	Abort(ctx context.Context, uploadID string) error
}

type Upload struct {
	TenantID string
	UploadID string
	Expected int
	Received []Part
	State    string
}

func CompleteOwned(ctx context.Context, store Store, upload Upload, tenantID string) error {
	if upload.TenantID != tenantID || upload.State != "ready_to_complete" {
		return ErrNotReady
	}
	if len(upload.Received) != upload.Expected {
		return ErrNotReady
	}
	if err := store.Complete(ctx, upload.UploadID, upload.Received); err != nil {
		return err
	}
	return nil
}
```

The production implementation must make the state update and image publication idempotent. A repeated completion request should observe the same logical image, not create a second backup record. An abort worker should be safe to run more than once, and it shouldn't delete evidence needed for an access-control investigation; report stale records as an operational metric instead.

## Where does the design still fail?

The catch is that multipart is not a backup policy. It does not choose retention, encryption ownership, restore authorization, deletion approval, or cross-region recovery. It is unsuitable when a single small object can meet the request's timeout budget and the platform has no operational need for resumability; a simpler atomic write may be easier to audit. It is also insufficient when the storage service cannot provide the access controls, retention guarantees, or integrity signals required by the school or district's data policy.

Stick with a managed object store when the team needs mature lifecycle controls, replication, audit integration, or browser delivery with a well-understood permission model. Consider a self-hosted store when placement, hardware economics, and operations expertise justify owning capacity, upgrades, and recovery. Your mileage may vary here: the right boundary depends on the existing SLO and on-call model, and I am not sure a nominal per-request price can resolve that decision.

The buy-versus-build table is intentionally unglamorous:

| Decision | Managed object storage | Self-hosted object storage |
|---|---|---|
| Access control | Delegate primitives to a service, then test tenant policy at the application boundary | Own identity integration, policy enforcement, and audit retention |
| Delivery simplicity | Fewer storage operations to run; provider-specific behavior remains a dependency | More control over placement and networking; more failure modes are yours |
| Capacity planning | Forecast bytes, requests, egress, and incomplete multipart work | Forecast disks, nodes, repair bandwidth, headroom, and operator time |
| Restore SLO | Validate the provider's durability and retrieval behavior in drills | Prove redundancy and restore performance in your own failure tests |

Do not hide access-control risk inside a convenience SDK. OWASP recommends allowlisting extensions, validating the actual file type, limiting size, generating server-side names, and storing uploads outside the web root; those controls apply to generated images too, even when the generator is trusted. PNG and WebP are formats, not authorization decisions.

## How should teams troubleshoot a complete upload without guessing?

Instrument the logical job, upload record, tenant, snapshot, part number, attempt, elapsed time, bytes, and final state. Keep tenant identifiers out of public URLs and redact them in logs where policy requires it. Alert on incomplete uploads by age, completion failure rate, restore digest mismatch, and authorization denials. A timeout alone is too coarse to tell whether the client, proxy, worker, or storage boundary consumed the budget.

The test matrix should include a dropped connection during part transfer, a worker restart after a receipt is written, duplicate completion, an expired upload, a restore request for the wrong tenant, a corrupt media payload, and a snapshot that names a missing object. Run a restore drill with a measured SLO. If the drill cannot prove which snapshot became visible and why, the upload path is not yet a backup path.

This approach is slower to sketch and faster to reason about during an incident. The final rule is narrow: use multipart for recoverable transfer work, use a tenant-scoped manifest for restore selection, and publish only after the complete transition is durable. Everything else is an implementation detail that must earn its place in the SLO and threat model.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
- https://docs.digitalocean.com/products/spaces/
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
