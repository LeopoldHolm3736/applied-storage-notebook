# PDF Digital Signature Boundaries for Server-Side Game Contract Audits Explained

A PDF digital signature proves that covered bytes have not changed since signing when verification succeeds; it does not prove that a player understood or accepted a game contract. Sign the final bytes on the server, preserve validation evidence beside the contract, and treat identity, authority, intent, time, and custody as separate claims. The deciding constraint is the audit trail: a cryptographically valid file can still sit beside a weak identity check, an unauthorized signing request, or no evidence of assent at all.

TL;DR: A PDF digital signature can show that a particular byte range has not changed since it was signed and can connect that signature to a certificate. It does not, by itself, establish who operated the key, whether that person had authority, whether the displayed contract matched the signer's intent, or whether the agreement is legally enforceable. Build those claims from separate evidence, then test each one independently.

For a gaming platform signing publishing, licensing, or tournament contracts server-side, that separation is an operational requirement. If one service both decides that a contract may be signed and holds the signing key, a single authorization defect can produce perfectly valid signatures on contracts that should never have existed. Cryptography will faithfully preserve the mistake.

## What does it mean when a PDF digital signature proves integrity?

A PDF signature is a CMS signature embedded through PDF's signature structures. In practical terms, the signer computes over designated byte ranges while leaving a placeholder for the signature container. A conforming verifier reconstructs the covered bytes, verifies the cryptographic signature, follows the certificate information, and reports whether later PDF revisions exist. ISO 32000-2 defines the document structures; CMS defines the signed-data syntax.

That produces several distinct findings, and collapsing them into one green check is where audits become misleading.

Stop there.

| Finding | What it supports | What remains unresolved |
|---|---|---|
| Cryptographic verification succeeds | The signed byte ranges match the signature value | Whether the key use was authorized |
| Certificate path is trusted under the verifier's policy | A trust anchor accepts the certificate chain | Whether the named human or service was the intended contracting party |
| Trusted timestamp validates | Evidence that the signature or document existed no later than the timestamp, under that timestamp policy | Whether consent occurred at that time |
| No disallowed later revision is found | The validated revision was not followed by an unacceptable change | Whether the original terms were visible, fair, or enforceable |

The distinction matters because PDFs may contain incremental updates: new objects can be appended without rewriting earlier bytes. A verifier therefore has to report both cryptographic coverage and the acceptability of subsequent revisions. A success boolean without the covered revision, validation time, trust policy, and modification result is too lossy for an audit record.

A certificate is also an assertion made within a public-key infrastructure, not a universal identity oracle. The verifier still needs a trust store, path-building rules, validity-time handling, and revocation policy. Different validation environments can reach different trust conclusions while agreeing that the mathematical signature matches.

## Model claims before choosing controls

Start with the claims an investigator may need six months later. For a game studio contract, separate artifact integrity, signer identity, organizational authority, user intent, time, and custody. The platform should never let one successful check stand in for all six.

The limitation is structural: no PDF signature format can reconstruct a missing approval event or repair an application that bound approval to the wrong contract version. Consider a tournament agreement rendered as version 7, approved by a league operator, and then regenerated as version 8 after a roster field changes. If the signing service receives only version 8 and a generic `approved=true` flag, it can produce a mathematically sound signature while destroying the link between the reviewed terms and the signed artifact. The safer design carries the version 7 digest through approval and rejects version 8 before any key operation. That rejection is useful evidence too: it records which invariant failed, which policy made the decision, and why the contract never reached publication. This approach is not suitable when the workflow cannot preserve immutable versions; fix that custody gap before adding server-side signing.

A useful record links an immutable contract identifier and version to the exact unsigned source inputs, the finalized PDF digest, the signing authorization decision, the resulting signed artifact, and the validation report. Record the policy version as data. Otherwise, a future verifier cannot tell whether an old result was evaluated under today's rules or the rules active when the signature was accepted.

Keep identity evidence narrow. An authenticated account may support the claim that a session passed a particular login policy. It does not prove that the account holder read a clause. Likewise, a server key can prove that the signing system possessed the private key during the operation, while saying nothing about which employee approved the request unless the approval event is separately bound to the transaction.

This is the capacity-planning trap too: teams size the signing workers, then forget that revocation retrieval, timestamping, validation, and durable evidence writes have their own latency and availability profiles. Define the SLO around the completed evidence package, not around the moment a signature value is produced. A contract that is signed but absent from the audit index is unfinished work.

## Build the signing boundary as a state machine

Use explicit states such as `prepared`, `approved`, `signing`, `signed`, `validated`, and `published`. Only a policy decision tied to the exact artifact digest should permit the transition into signing. If rendering changes even one byte after approval, generate a new version and require a new decision; never quietly carry approval across digests.

The following Go sketch focuses on the boundary rather than pretending to implement PDF or CMS encoding. Those formats have enough edge cases that a maintained standards-aware library should perform the actual signing and validation. The interfaces make the audit obligations visible.

```go
package contracts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"
)

type Approval struct {
	ContractID   string
	Version      int
	PDFSHA256    string
	PrincipalID string
	PolicyID     string
	ApprovedAt   time.Time
}

type ValidationReport struct {
	SignatureValid bool
	ChainTrusted   bool
	RevisionOK     bool
	ValidatedAt    time.Time
	PolicyID       string
}

type PDFSigner interface {
	Sign(ctx context.Context, pdf []byte) ([]byte, error)
}

type PDFValidator interface {
	Validate(ctx context.Context, signedPDF []byte, policyID string) (ValidationReport, error)
}

type EvidenceStore interface {
	Commit(ctx context.Context, approval Approval, signedPDF []byte, report ValidationReport) error
}

func Finalize(ctx context.Context, pdf []byte, approval Approval, signer PDFSigner, validator PDFValidator, store EvidenceStore) error {
	digest := sha256.Sum256(pdf)
	if hex.EncodeToString(digest[:]) != approval.PDFSHA256 {
		return errors.New("approved digest does not match rendered PDF")
	}

	signed, err := signer.Sign(ctx, pdf)
	if err != nil {
		return err
	}

	report, err := validator.Validate(ctx, signed, approval.PolicyID)
	if err != nil {
		return err
	}
	if !report.SignatureValid || !report.ChainTrusted || !report.RevisionOK {
		return errors.New("signed PDF failed release policy")
	}

	return store.Commit(ctx, approval, signed, report)
}
```

The commit must be idempotent under a transaction identifier.

Retries happen.

A timeout after signing can leave the caller unsure whether a key operation completed, so retrying blindly may generate multiple signed variants and confusing audit entries. Reconcile by contract version and approved digest, retain every attempt, and publish exactly one validated artifact.

Key custody is a buy-versus-build decision, but it is not reducible to hosting preference.

| Approach | Operational gain | Operational burden | Lock-in boundary |
|---|---|---|---|
| Managed signing service | Provider operates key hardware and some availability controls | External dependency, policy mapping, evidence export, and provider incident handling | API semantics, identity model, and audit format |
| Cloud key service plus application-owned PDF logic | Key material stays behind a defined signing API while document workflow remains portable | The team owns PDF correctness, certificate lifecycle, and validation | Key API and attestation format |
| Self-hosted key hardware and signing stack | Maximum control over custody and policy | Hardware lifecycle, ceremony, patching, capacity, and 24-hour on-call ownership | Hardware interfaces and internal implementation |

Choose according to failure ownership. If the platform team cannot staff certificate renewal, revocation behavior, hardware replacement, and parser security, self-hosting does not create independence; it creates an unstaffed dependency. Conversely, a managed control is incomplete if evidence cannot be exported and independently validated after the service relationship ends.

There is no universal winner.

## Verify the evidence, not the happy path

Run validation outside the signer process and, where feasible, with a separately maintained implementation. The critical test set includes a one-byte mutation inside the signed range, an appended revision that policy forbids, an expired certificate evaluated at the relevant validation time, an untrusted chain, unavailable revocation data, a mismatched approval digest, and a repeated request after an ambiguous timeout. Each case should produce a specific status rather than a generic invalid result.

There are at least four useful service-level signals: time from approved to evidence committed, fraction of signing attempts that reach validated state, age of the oldest unreconciled attempt, and certificate time remaining. Alert on states that threaten the user-visible contract workflow, but keep the raw validation reasons for forensic work. A spike in untrusted-chain results demands a different response from a spike in PDF parse failures.

Be strict about clocks. Record times in UTC, retain the timestamp authority response when one is used, and distinguish the application's event time from cryptographically supported time evidence. A database `created_at` value proves what that database recorded; it is not a trusted timestamp token.

That difference survives every architecture choice.

Test the presentation layer as well. The approved digest must correspond to the exact PDF that was presented for review, and the review flow must bind the visible contract version to the approval action. Digital signatures cannot detect that a user was shown version 7 while the server was authorized to sign version 8 if the surrounding application binds the wrong identifier.

## Roll back publication without erasing history

A signed PDF is evidence, so rollback should change availability and status rather than overwrite or delete the artifact. If post-sign validation fails, quarantine the result, block publication, and leave the attempt linked to its error report. If publication already occurred, mark that contract version superseded or withdrawn under the applicable business process, issue a replacement through a fresh approval, and preserve both versions.

Do not reuse an approval after rollback. The replacement may render identically, but a fresh transaction record removes ambiguity about intent, time, and policy. Recovery is complete only when the published pointer, evidence index, and downstream consumers agree on the active version.

The practical decision rule is terse: release only when the approved digest, cryptographic result, trust decision, revision policy, and durable audit write all agree. Everything else is a pending or failed contract operation, even if a PDF viewer displays a reassuring badge.

## References

- ISO 32000-2, Portable Document Format: https://www.iso.org/standard/75839.html
- RFC 5652, Cryptographic Message Syntax: https://www.rfc-editor.org/rfc/rfc5652
- RFC 5280, Internet X.509 Public Key Infrastructure Certificate and CRL Profile: https://www.rfc-editor.org/rfc/rfc5280
- RFC 3161, Time-Stamp Protocol: https://www.rfc-editor.org/rfc/rfc3161
- ETSI EN 319 142-1, PAdES digital signatures: https://www.etsi.org/deliver/etsi_en/319100_319199/31914201/
- NIST SP 800-57 Part 1 Revision 5, Key Management: https://csrc.nist.gov/pubs/sp/800/57/pt1/r5/final
