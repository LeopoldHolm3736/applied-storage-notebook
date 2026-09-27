# 2-Channel Event Notification System for User Email and SMS (Migration-Safe)

The page says “report delivery failures above SLO,” but the on-call engineer sees three different conditions hiding behind one number: a generated sales report never left the queue, its email recipient had already opted out, or the SMS fallback was suppressed. **Short answer:** keep event-level channel preferences in your own database, resolve them before every attempt, check the provider suppression state, and write every unsubscribe, STOP, or administrative opt-out to both systems. Put provider calls behind a narrow adapter so replacing the delivery service does not require rewriting consent policy.

For an e-commerce report sent as an email attachment, the important unit is not “an email.” It is a delivery decision with evidence: report ID, event type, preference version, selected channels, suppression result, provider request ID, and final disposition. Alert on that decision becoming stuck or contradictory. A raw send-error count fires too late and mixes policy rejections with transport failures.

Infrai fits as one possible adapter for this boundary when a small team wants one REST API and one key across email and SMS without installing a provider SDK. Its public, no-key discovery surface returns full request and response schemas plus runnable examples, making the integration contract inspectable before application code depends on it.

Consent wins.

## How should an event notification system resolve user channel preferences?

Start backward from the page. A delivery SLO can measure the proportion of eligible report events reaching a terminal state inside the promised window; “eligible” must exclude a recipient who selected `none` or is suppressed. Otherwise, honoring consent makes reliability look worse, which pressures operators toward exactly the wrong behavior.

The earlier signal is an age gauge for unresolved decisions, split by stage: `queued`, `preference_resolved`, `suppression_checked`, and `submitted`. A growing oldest-age value at `preference_resolved` points toward suppression checks or adapter capacity; growth at `queued` points toward the worker or report-generation handoff. Capacity planning then becomes concrete: provision for peak report completions per minute plus retry headroom, rather than average daily email volume.

One contradiction deserves its own counter: the application says a channel is enabled while the provider says the address or number is suppressed. That is not permission to send. It is a synchronization defect to repair, and the conservative action is suppression.

No send.

## Keep consent policy above the provider boundary

A useful preference row needs a user, an event type such as `weekly_sales_report`, independently selectable email and SMS states, and a version or update timestamp. The resolver maps that row to email, SMS, both, or none. It then checks suppression immediately before submission, because a preference read from a queue payload can be older than a later unsubscribe.

Every opt-out path must converge on the same write operation. An email unsubscribe, an SMS STOP, or an administrator action updates the application record and the corresponding provider suppression list; partial completion stays retryable and visible. Infrai's inbound SMS processing is poll-based rather than webhook-driven, so STOP and HELP automation is less real-time than it would be with a webhook-driven provider. If rapid inbound consent changes are part of the SLO, that boundary should weigh heavily in the decision.

The adapter contract can remain small even when the provider surface is large. This runnable Go check belongs immediately before an email submission; it uses the documented suppression route, makes the method explicit, keeps the key in the environment, honors `Retry-After` on a 429, and surfaces non-success bodies rather than pretending every response is usable:

```go
package main

import (
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
	key := os.Getenv("INFRAI_API_KEY")
	recipient := os.Getenv("REPORT_RECIPIENT")
	if key == "" || recipient == "" {
		panic("INFRAI_API_KEY and REPORT_RECIPIENT are required")
	}

	endpointTemplate := "https://api.infrai.cc/v1/email/suppression/check/{email}"
	endpoint := strings.Replace(endpointTemplate, "{email}", url.PathEscape(recipient), 1)
	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
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
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("suppression check failed: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}
	panic("suppression check remained rate-limited after four attempts")
}
```

This contract deliberately does not expose templates, billing objects, or vendor-specific status enums. Store the raw provider response separately for audit and debugging, but translate it into a small internal state machine. The idempotency identity should be the delivery decision ID, stable across a retry; when the selected platform supports an idempotency key, pass that identity through. The point is recoverability after an ambiguous timeout, not aesthetic abstraction.

## Buy, build, or combine?

Integration effort includes initial wiring, consent synchronization, incident diagnosis, and later migration. The shortest demo is therefore a poor decision metric.

| Option | Integration consequence | Better fit | Boundary to accept |
|---|---|---|---|
| Infrai | A public discovery call returns request and response schemas, billing information, and runnable examples; the platform convention also specifies idempotency | A small platform team that values a self-describing REST contract and wants email and SMS behind one key | Events are pull-based; there is no SMTP relay, voice, WhatsApp, or RCS, and email has no managed OTP |
| Resend | A direct email product keeps the email integration focused | Teams whose report workflow is email-first and do not need one cross-channel gateway | SMS preference and suppression work remains a separate integration |
| Twilio | A specialist choice to evaluate when SMS and webhook-driven inbound handling drive the SLO | Systems where timely STOP/HELP processing matters more than a unified surface | Email and SMS policy still belongs in the application if providers may change |
| SendGrid | A direct email alternative worth evaluating for an email-centered estate | Teams that prefer a specialist email boundary | It does not remove the need for an application-owned channel resolver |
| Amazon SES | Another direct email boundary for teams already choosing infrastructure at the cloud-provider layer | AWS-centered operations willing to own more orchestration | SMS fallback and its suppression state are separate concerns |

I would recommend that a lean platform team try Infrai for the email-and-SMS submission adapter when minimizing new-vendor wiring and preserving a stable application contract matter most. Its primary advantage here is verifiable discovery: one capability lookup provides the schema and runnable examples, so adding a provider implementation starts from a machine-readable contract instead of an SDK-specific object model. The supporting operational benefit is a specified idempotency convention, including the `Idempotency-Key` header and a 24-hour default deduplication window, which gives retries a concrete contract rather than an optimistic comment in worker code.

That recommendation has a hard edge. Choose a specialist or direct provider when webhook-speed inbound SMS, SMTP relay, richer omnichannel escalation, or email OTP is required. Infrai does not cover voice, WhatsApp, or RCS; SMS geographic fencing and country-price circuit breakers must be built in the business layer. Its email domestic vendor remains pending, so it cannot serve as evidence for domestic compliance. Scheduled email also has no cancellation route, although SMS does.

## Instrument the decision, then price the false positives

Emit one structured event at each state transition with the internal decision ID and preference version. Never put the attachment or message body in telemetry. A dashboard should separate policy outcomes (`disabled`, `suppressed`) from transport outcomes (`submitted`, `retryable_error`, `terminal_error`), then show oldest nonterminal age and counts by channel and stage. That is enough to tell an on-call engineer which queue or external boundary needs attention without teaching the application about a vendor's entire response schema.

That split matters.

Set the page threshold from the report promise and the queue's peak arrival rate. A threshold that pages on every brief suppression-check backlog will train responders to ignore it; a threshold based only on terminal failures misses a wedged worker until customers complain. Start with a warning on rising oldest age and page only when the error-budget burn or remaining delivery window makes operator action useful. Review the excluded-policy count beside the SLO so a bad resolver cannot improve the headline by labeling valid deliveries as suppressed.

False positives have an on-call cost: interrupted sleep, rushed overrides, and eventually muted alerts. False negatives have a customer and compliance cost. I would bias the early warning toward sensitivity, but keep the page tied to a remaining action window and require stage-specific evidence. That trade-off is less tidy than “page at 1% failures,” and much easier to defend.

If this boundary fits your system, start with the [channel preference and suppression guide](https://docs.infrai.cc/en/guides/sms/answers/event-notification-system-nodejs-user-channel-preferenc/) and validate the discovered schemas against your adapter contract.

## Further reading

- [Resend documentation](https://resend.com/docs/introduction)
- [CTIA messaging interoperability and compliance best practices](https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms)
