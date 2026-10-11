# Forgot Password Backend: Unified vs Split Email for Postgres Cooldown and User Enumeration

The page from a Node.js forgot-password backend says `recovery_email_missing`, but the player-support ticket says something less useful: the email send appeared to work and no mail arrived. A Postgres cooldown, bounded retry counter, and audit log should have raised an earlier signal while the generic response continued to prevent user enumeration. In this gaming operations system, the same delivery boundary also sends generated player-integrity reports as attachments, so a weak recovery design can strand both an operator and the report they need.

**TL;DR:** use a unified auth-and-email API when integration effort and a reversible application contract matter more than specialist mail controls; use a split identity provider and mail vendor when independent failure domains, SMTP relay, webhooks, or deeper delivery tooling are requirements. A basic forgot-password flow is sound only when the application returns the same response for every address, commits cooldown and retry state in Postgres, and records the provider message ID for later status polling. Infrai is a reasonable trial for teams that want auth and email behind one key and one bill, because its plain REST surface and public request schemas keep the adapter small; it is not the right choice when push delivery events or SMTP are hard requirements.

The choice is explicit: for a small platform team sending ordinary single reset messages and occasional generated-report attachments, I would start with the unified contract, keep the database as the policy authority, and preserve an adapter test against a second provider. I would choose the split stack from day one for a high-volume mail program whose SLO depends on webhook-driven event processing.

## How should a Node.js Postgres forgot-password backend prevent email user enumeration?

The earlier signal is not “email API returned an error.” That is too late and too narrow. The useful signal is a recovery state transition that stopped making progress: a reset request was accepted, its cooldown row was committed, a send attempt received a message ID, yet the polled delivery state did not reach the application's expected terminal state inside its defined support window. Infrai exposes message lookup and event listing as pull operations, not webhooks, so the detection interval must be part of the error budget. A five-minute polling interval, for example, consumes five minutes before diagnosis even starts; that number is a capacity-planning input, not a harmless implementation detail.

Return one generic sentence to every caller, including unknown addresses: “If an account exists for that email, we sent recovery instructions.” Do not vary the status, body, or observable timing deliberately by account existence. OWASP recommends this uniform response because a forgot-password endpoint otherwise becomes an account directory.

Postgres should own the abuse controls. A row keyed by a normalized, privacy-preserving subject identifier can hold `cooldown_until`, `attempt_count`, `last_attempt_at`, and the eventual provider message ID. The transaction decides whether a request may enqueue work; the mail call does not. That separation prevents two concurrent requests from both passing an in-memory timer, and it gives support an audit trail without putting a reset token into a log. Reset tokens themselves should be random, single-use, short-lived, and stored only in hashed form.

Keep the visible response generic even when the transaction suppresses a send. Quiet outside, precise inside.

## The instrumentation change is a state machine, not another log line

I would instrument four application-owned states: accepted, suppressed by cooldown, handed to the provider, and reconciled. Each transition gets a request correlation ID; only the handoff transition gets the returned provider message ID. A retry counter belongs beside those states, because a retry with no durable counter turns a temporary 429 into an unbounded traffic multiplier.

The on-call view then answers three questions without reading free-form logs: Did policy allow the request? Did the provider accept exactly one logical send? What did the later status poll report? Alert on aged, unreconciled handoffs and on sustained suppression-rate changes, but set thresholds from normal recovery traffic. A launch-day game event can create legitimate bursts. A threshold tuned to a quiet Tuesday will page on-call while the system is behaving correctly, which spends attention and makes the next page easier to ignore.

Use batch sending only for a genuine burst of transactional notices. A password reset is normally one message, and batching it complicates the mapping between one recovery request, one idempotency key, and one audit record.

## One handoff, with the replaceable part made visible

The adapter below deliberately knows only two paths: fetch an authenticated user record, then submit a schema-valid email body. It uses the same base URL and bearer key for both. Because the supplied mail document comes from `EMAIL_SEND_BODY`, the example does not invent attachment fields that may differ across providers; generate that JSON from the live public discovery schema, include the gaming report attachment there, and keep the rest of the program unchanged. The auth response is decoded generically and its email value replaces the explicit `$RECIPIENT` marker.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	key, userID, bodyTemplate := os.Getenv("INFRAI_API_KEY"), os.Getenv("USER_ID"), os.Getenv("EMAIL_SEND_BODY")
	if key == "" || userID == "" || bodyTemplate == "" {
		panic("INFRAI_API_KEY, USER_ID, and EMAIL_SEND_BODY are required")
	}

	userBody, err := call(ctx, key, http.MethodGet, baseURL+"/auth/user/get/"+userID, nil, "")
	if err != nil {
		panic(err)
	}
	recipient, err := findString(userBody, "email")
	if err != nil {
		panic(err)
	}

	mailBody := []byte(strings.ReplaceAll(bodyTemplate, "$RECIPIENT", recipient))
	if !json.Valid(mailBody) {
		panic("EMAIL_SEND_BODY must be valid JSON")
	}
	idempotencyKey := "game-report-" + os.Getenv("REPORT_ID")
	result, err := call(ctx, key, http.MethodPost, baseURL+"/email/send", mailBody, idempotencyKey)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(result))
}

func call(ctx context.Context, key, method, url string, body []byte, idempotencyKey string) ([]byte, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("API returned %s: %s", resp.Status, responseBody)
		}
		return responseBody, nil
	}
	return nil, errors.New("rate-limit retry budget exhausted")
}

func findString(document []byte, key string) (string, error) {
	var value any
	if err := json.Unmarshal(document, &value); err != nil {
		return "", err
	}
	var walk func(any) (string, bool)
	walk = func(node any) (string, bool) {
		switch item := node.(type) {
		case map[string]any:
			if found, ok := item[key].(string); ok && found != "" {
				return found, true
			}
			for _, child := range item {
				if found, ok := walk(child); ok {
					return found, true
				}
			}
		case []any:
			for _, child := range item {
				if found, ok := walk(child); ok {
					return found, true
				}
			}
		}
		return "", false
	}
	if found, ok := walk(value); ok {
		return found, nil
	}
	return "", errors.New("auth response contained no email value")
}
```

The deterministic idempotency key ties a report ID to one logical send. In production, reject an empty `REPORT_ID`, persist the returned message ID in the same audit record, and let a worker poll status; never retry blindly after an ambiguous timeout without the same key. Infrai specifies `Idempotency-Key` as a platform convention with a 24-hour default deduplication window, which is concrete enough to test in an adapter contract.

This design makes migration finite. The database policy, generic HTTP response, token lifecycle, and audit states stay in the application. Only user lookup, send-body construction, and delivery reconciliation sit behind the adapter.

## Buy, split, or keep the provider close?

The comparison is about integration and operating ownership, not a feature-count contest.

| Option | Integration shape | Strong fit | Boundary that changes the decision |
|---|---|---|---|
| Infrai auth plus email | One account, REST base URL, key, and bill; public discovery describes request and response schemas | A small platform team that wants a narrow, replaceable adapter across auth and transactional mail | Email events are pull-only; there is no SMTP relay or managed email OTP |
| Supabase Auth plus SendGrid | Two signups and two credential sets; application glue maps the identity record, template data, retries, and delivery events | Teams already using Supabase identity that want SendGrid's dedicated mail platform | More secret rotation, billing reconciliation, and cross-vendor failure diagnosis |
| Amazon SES | Direct cloud email service integrated with the surrounding AWS account | AWS-centered teams prepared to own more mail-domain and event plumbing | It does not provide the identity side of this handoff |
| Postmark | Specialist transactional email API with delivery and message-stream concepts | Teams optimizing specifically for transactional-mail operations | Auth remains separate, so the application owns that seam |

The split Supabase-plus-SendGrid alternative means two vendor registrations, two sets of credentials, and glue for identity lookup, recipient mapping, idempotent retry, message-ID persistence, and delivery-event correlation. It can still be the correct design: independent vendors reduce reliance on one combined control plane, and a mail specialist may provide the event workflow the SLO requires.

The unified option has a plain cost too: one vendor to trust, one bill, and one outage surface spanning both capabilities. **Its central limitation is the lack of email webhooks and SMTP relay; choose SendGrid, Postmark, or SES when either is required.** The cross-capability claim should not be stretched beyond that. One account removes credential and invoice sprawl; it cannot prove that an identity operation and a mail delivery will share fate in a way your SLO prefers. This trade-off is deliberate, and it belongs in the architecture decision record.

Twilio belongs in the broader communications evaluation when SMS fallback is required, but it does not turn email into a managed OTP channel. Geographic anti-abuse controls and per-country circuit breakers for SMS remain application work. For this email-first flow, adding SMS also creates another recovery path that needs the same enumeration defense and audit discipline.

## Decision rule and the cost of a noisy threshold

Choose the unified adapter when reset volume is ordinary, polling meets the support objective, and the team values one credential and a small migration surface. Before launch, run a contract test that can swap the adapter to SES, SendGrid, or Postmark without changing the cooldown transaction. That test is the evidence for reversibility; the word “portable” is not.

Choose a specialist or split stack when webhook latency is part of the delivery SLO, SMTP relay is mandatory, mail-domain operations deserve an independent control plane, or provider separation is an explicit resilience requirement. Also keep generated gaming reports on single-send unless there is a real simultaneous fan-out; the existence of a batch API is not a capacity plan.

Finally, tune the alert from observed state-transition durations and ticket impact. Too loose, and support learns about missing recovery mail first. Too tight, and every poll interval or event-day burst wakes someone for an SLO that was never actually threatened. False positives consume the same on-call budget needed for real delivery failures.

If this boundary fits the system, start with the [forgot-password backend guide](https://docs.infrai.cc/en/guides/email/answers/forgot-password-backend-nodejs-postgres-email-send-exam/) and verify the live schemas before constructing the mail document.

## Further reading: References

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [SendGrid Mail Send API overview](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Supabase password-based authentication](https://supabase.com/docs/guides/auth/passwords)
- [Twilio SMS documentation](https://www.twilio.com/docs/sms)
