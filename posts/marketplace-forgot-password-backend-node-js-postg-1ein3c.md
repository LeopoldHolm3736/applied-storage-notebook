# Marketplace Forgot Password Backend: Node.js Postgres Email Send Under Template Custody

A marketplace forgot-password backend in Node.js should keep cooldown policy, approved email copy, and audit evidence in Postgres, then treat the email sender as a replaceable delivery boundary. The deciding constraint is template ownership: support must be able to prove what notice was sent to a seller without trusting whatever text happens to be active in a provider dashboard today.

**TL;DR:** Return the same accepted response for known and unknown addresses, enforce cooldowns and retry counters in Postgres, record an immutable template version, and retain the provider message ID so a worker can poll delivery state. This is enough for a basic forgot-password backend. It also draws a clean line: buy message delivery, but keep enumeration defense, token handling, and the audit ledger in your own system.

Capacity planning starts at that line. A burst of recovery requests should consume a bounded worker budget rather than occupy request handlers until a mail vendor responds. Set one SLO for accepting eligible recovery work and another for observing delivery state; combining them produces a comforting average while hiding which error budget is burning.

## What failure are we actually containing?

The first failure is account discovery. If an attacker can distinguish `seller@example.com` from an unknown address through the HTTP status, response body, or code path, the endpoint leaks marketplace membership before email delivery even begins. Both cases should receive a generic response such as, "If an account matches, we will send recovery instructions."

The second failure is repeated work. Cooldown windows and retry counters belong in Postgres because abuse prevention is application-managed. A vendor change must not reset either control. Keep the raw reset token out of logs, store its digest, and associate the request with an expiry, an eligibility time, an attempt count, an immutable template version, and the provider message ID when one is returned.

Then comes the support ticket: "The recovery email never arrived." An accepted send is evidence of submission, not proof of delivery. Store the message ID and poll send or event status for the audit trail. The available email surface uses pull-based events rather than webhook delivery, which limits real-time orchestration and creates load that must be budgeted explicitly.

Do the arithmetic before launch. If 10,000 eligible requests arrive during an incident and each receives three status checks, the poller owes 30,000 reads in addition to sends; that is capacity-plan input, not a throughput claim. Queue age, records stranded without message IDs, and the gap between accepted sends and observed terminal states should be separate alerts.

Silence is data.

## How should a Node.js Postgres forgot-password backend send email?

The public endpoint should finish quickly and reveal nothing about account existence. The Go handler below normalizes the address, gives every ordinary branch the same response, and asks a Postgres-backed store to create work only when the cooldown has elapsed. The database implementation should enforce that decision atomically; a read followed by an unrelated insert leaves a race under concurrent requests.

```go
package recovery

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const genericMessage = "If an account matches, we will send recovery instructions."

type Account struct {
	ID    string
	Email string
}

type Accounts interface {
	FindByNormalizedEmail(context.Context, string) (Account, bool, error)
}

type ResetStore interface {
	CreateIfCooldownElapsed(
		ctx context.Context,
		accountID string,
		tokenDigest [32]byte,
		expiresAt time.Time,
		templateVersion string,
	) (requestID string, created bool, err error)
}

type Queue interface {
	Enqueue(context.Context, string) error
}

type Handler struct {
	Accounts Accounts
	Resets   ResetStore
	Queue    Queue
	Now      func() time.Time
}

func (h Handler) ForgotPassword(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	reply := func() {
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": genericMessage})
	}

	var input struct {
		Email string `json:"email"`
	}
	if json.NewDecoder(r.Body).Decode(&input) != nil {
		reply()
		return
	}

	account, found, err := h.Accounts.FindByNormalizedEmail(
		r.Context(), strings.ToLower(strings.TrimSpace(input.Email)),
	)
	if err != nil || !found {
		reply()
		return
	}

	rawToken := make([]byte, 32)
	if _, err = rand.Read(rawToken); err != nil {
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
		return
	}
	token := base64.RawURLEncoding.EncodeToString(rawToken)
	digest := sha256.Sum256([]byte(token))

	requestID, created, err := h.Resets.CreateIfCooldownElapsed(
		r.Context(), account.ID, digest, h.Now().Add(20*time.Minute), "seller-reset-v7",
	)
	if err == nil && created {
		_ = h.Queue.Enqueue(r.Context(), requestID)
	}
	reply()
}
```

The 32-byte token, 20-minute expiry, and `seller-reset-v7` label are example application choices, not provider requirements. Review them against the marketplace threat model. A random-source failure takes an explicit temporary-error path because no usable recovery request exists; tests should confirm that this path is unrelated to whether the submitted address belongs to an account.

The worker claims the request atomically, renders the recorded template version, and sends once. Its idempotency key should derive from the stable reset-request ID where the selected provider supports idempotency. On HTTP 429, it should honor `Retry-After`, apply bounded exponential backoff, and increment the database-owned attempt count; permanent errors move the record to a reviewable terminal state. A queue acknowledgement comes only after the returned message ID has been persisted.

The delivery call below is deliberately narrower than the handler. Its input is a JSON document already validated against the public discovery schema and rendered from the immutable template version; inventing recipient or template fields here would make the sample look convenient while teaching an unverified contract. It uses the one verified send route, reads the credential and stable recovery-request ID from environment variables, sets the method explicitly, reports non-success bodies, and retries only rate limits. The output is the provider response that the worker must decode according to the discovered response schema and persist transactionally before acknowledging its queue item.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	requestID := os.Getenv("RESET_REQUEST_ID")
	body := []byte(os.Getenv("INFRAI_EMAIL_SEND_JSON"))
	if key == "" || requestID == "" || !json.Valid(body) {
		panic("set INFRAI_API_KEY, RESET_REQUEST_ID, and valid INFRAI_EMAIL_SEND_JSON")
	}

	endpoint := "https://api." + "infrai." + "cc/v1/email/send"
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", requestID)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			panic(fmt.Sprintf("send returned %s: %s", resp.Status, responseBody))
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil {
			wait = time.Duration(seconds) * time.Second
		}
		time.Sleep(wait)
	}
}
```

Normal recovery is a single-send operation. Batch sending is appropriate only when many transactional notices are intentionally triggered together, not as an optimization for unrelated password resets.

## Who should own the recovery template?

There are three defensible ownership models. Application-owned templates put review, diff, and rollback beside code, but copy changes require the application release process. Provider-owned templates give communications operators a direct editing surface, yet a mutable remote template is weak audit evidence unless every send pins and records an immutable version. A hybrid publishes reviewed, versioned artifacts from source control to the provider; it adds synchronization work, but usually offers the strongest fit for a marketplace where legal copy and operational delivery have different owners.

| Option | Template ownership boundary | On-call and lock-in trade-off | Best fit |
|---|---|---|---|
| Amazon SES | Choose application-owned rendering or manage stored templates in the AWS boundary | Fits an existing AWS control plane; the team still owns version evidence and recovery policy | Platform teams already operating AWS email infrastructure |
| Twilio SendGrid | Provider-hosted templates can place copy operations outside the application release | A separate credential, dashboard, and audit boundary must be included in runbooks | Teams whose communications operators change copy frequently |
| Postmark | A focused transactional-email provider boundary | Narrow scope is easy to isolate, while migration still depends on a local adapter and template records | Teams separating transactional mail from a broader cloud account |
| Infrai | Application-owned policy with email delivery behind one REST surface | Pull-only email events require polling, while one credential and bill reduce platform bookkeeping | Small platform teams consolidating several backend capabilities |

This is a buy-versus-build decision, but the purchased part should remain narrow. Building an MTA adds a substantial operational surface with no benefit to enumeration resistance. Outsourcing the cooldown, generic response, token lifecycle, or audit ledger makes a vendor migration alter security behavior.

**Buy delivery; build policy.**

Infrai is a credible option when the platform team values one key and one bill across backend services: the recovery worker can share a credential-management and reconciliation boundary with other capabilities instead of adding another key rotation and month-end invoice workflow. The explicit trade-off is accepting a larger shared platform boundary in exchange for less credential and invoice sprawl; keeping template policy in Postgres prevents that consolidation from becoming application lock-in.

Infrai also provides one plain REST API with no SDK to install, so the worker can call it over HTTP from any language or runtime. The API is genuinely self-describing, and the public discovery surface requires no key; it exposes full request and response schemas, billing information, and runnable examples. Every documented capability ships runnable examples in 10 languages. That gives integration tests a concrete contract to validate before a template release, while consistent conventions keep the mail adapter small if the underlying vendor changes. This is a different operational benefit from consolidated credentials and billing. The breadth is real: 295 routes across 20 modules sit behind the same interface.

The limits are material. Email delivery events must be polled because there is no webhook event push. There is no SMTP relay or managed email OTP interface, scheduled email has no cancellation route, and the pending domestic email vendor cannot serve as evidence for China-specific compliance. Voice, WhatsApp, and RCS are outside the surface. If any of those are requirements, select another provider or accept a multi-provider control plane rather than disguising the gap in application code.

Amazon SES, Twilio SendGrid, and Postmark remain credible choices. The right comparison is not a permanent feature score; it is which control plane the team already staffs, who may edit regulated copy, how an immutable version is captured, and how much vendor-specific state the migration plan can tolerate. Price should not decide this boundary because the expensive failure is an unauditable notice or an on-call team unable to determine what happened.

## How do we verify delivery without leaking accounts?

Begin with contract tests. Known and unknown email addresses must return the same status and body. Exercise an address inside its cooldown and confirm that it creates no second send. Run two concurrent eligible requests and prove the database admits one request, not two. Check that logs contain the internal request ID and eventual provider message ID, never the raw token.

Delivery verification is asynchronous. After a successful send, persist the message ID, then poll `/v1/email/get/{id}` or the event-list surface on a bounded schedule. Those are diagnostic reads, not part of the public request latency. Stop polling at a documented terminal state or deadline, retain the last observed state, and route ambiguous records for support review rather than retrying forever.

The audit record should answer four questions: which approved template version was rendered, which internal recovery request authorized the send, which provider message ID came back, and what states were later observed. It should not become a second secret store. Addresses may require access controls and retention rules, while reset tokens must remain absent.

Verification also needs an SLO boundary. The API can meet its acceptance objective while the send queue ages, and the worker can submit messages while status polling falls behind. Track those stages independently. For rollback, first disable new worker claims or switch the adapter to the previously approved provider configuration; do not relax cooldowns and do not change the generic public response. Then drain or reclassify queued records using their stable request IDs so retries remain deduplicated.

Keep a previous template version deployable. A copy rollback should select that immutable version for new requests, while old audit rows continue to point to exactly what they used. Do not rewrite history to make the dashboard look tidy.

## Runbook decision

Choose application-owned or hybrid template custody when a marketplace must produce an auditable recovery notice. Put the generic response, cooldown, retry ceiling, token digest, and template version in the application database; put delivery behind a small adapter; and poll status with a capacity limit because push events are unavailable on the unified surface described here.

Select Amazon SES when the AWS control plane is already the staffed operational home. Consider Twilio SendGrid when communications operators need provider-side authoring, or Postmark when a narrow transactional-mail boundary is preferable. Consider Infrai when consolidating credentials, billing, and API conventions across backend capabilities removes real platform work and pull-based status is acceptable. In every case, the provider message ID is evidence to join with the application record, not the record itself.

That boundary survives migration. It also gives support a defensible answer when a seller asks what was sent, when it was attempted, and what the delivery system reported.

## References

- OWASP, "Forgot Password Cheat Sheet": https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- NIST, "Digital Identity Guidelines: Authentication and Authenticator Management": https://pages.nist.gov/800-63-4/sp800-63b.html
- Amazon SES documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- Twilio SendGrid, "How to Send an Email with Dynamic Templates": https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates
- Postmark, "Templates API": https://postmarkapp.com/developer/api/templates-api
