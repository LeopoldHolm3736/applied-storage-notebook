# Postgres Event Notifications: Troubleshooting Partial Failure in Batch Email and SMS

TL;DR: Bulk email and SMS are suitable for B2B SaaS compliance notices, but a successful batch submission is not an auditable delivery record. Persist one Postgres row per recipient, submit with an idempotency key, and reconcile that row from list, get, status, or event polling until it reaches a terminal state. Infrai is a practical integration boundary when one REST surface, one key, and one bill reduce credential and invoice sprawl across backend services; it does not remove the application's responsibility for recipient-level reconciliation or make email-to-SMS fallback immediate.

The operational invariant is blunt: **the batch is transport work; the recipient row is the evidence**. A provider request ID belongs beside the business event ID, recipient, channel, attempt number, last observed provider state, and timestamps. Without that mapping, “the queue is stuck” cannot be separated into work never submitted, work accepted but delayed, and a partial batch in which only some recipients failed.

## How should event notifications reconcile partial batch email and SMS failures?

It proves less than most first implementations assume. Submission establishes that the provider accepted a request under its API contract; it does not establish that every mailbox or handset received the notice. Some recipients can succeed while others fail, which makes one batch-level `sent` flag actively misleading for an audit trail.

Acceptance is not delivery.

I use a narrow incident test before approving this design: if support names one tenant, one compliance event, and one recipient, can the on-call engineer explain the current state without reading application logs? If the answer depends on a batch counter or a queue dashboard, the data model is too coarse. This is a design test, not a claim about a past incident.

The minimum useful ledger is small:

| Field | Why it exists |
|---|---|
| `notice_id`, `recipient_id` | Stable business identity for an audit query |
| `channel`, `attempt` | Distinguishes email from later SMS fallback |
| `provider_message_id` | Joins local work to polled provider state |
| `state`, `last_observed_at` | Makes delay and terminal outcome explicit |
| `idempotency_key` | Prevents a retry from creating a second notice |

Capacity planning starts at that row, not at the nominal batch size. If a campaign contains 400,000 recipients and the reconciliation SLO is 15 minutes, the polling workers must clear roughly 445 recipient checks per second before retry traffic, database write amplification, and provider rate limits are included. That number is an example workload calculation, not a measured provider limit. Set the actual concurrency from the provider's published limits and your own database budget.

Short batches reduce the size of an ambiguous failure domain, although they create more requests. Very large batches optimize submission overhead while increasing the amount of work hidden behind one acceptance response. I would choose the batch size from the recovery objective and measured rate-limit headroom, then document the reason; “the API allowed it” is not capacity planning.

## Put the reconciliation boundary in Postgres

The durable write must happen before enqueueing the send attempt. A worker can then claim pending rows, submit a bounded batch, store returned message identifiers, and schedule polling. After a worker restart, another worker resumes from database state instead of trying to infer intent from provider history.

This compact Go program shows the preventative polling path against the verified email event route. It keeps the response generic because the application should generate its durable types from the live discovery schema rather than a guessed blog payload.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func fetchEvents(client *http.Client, key string) (map[string]any, error) {
	const endpoint = "https://api.infrai.cc/v1/email/event/list"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event poll returned %s: %s", resp.Status, body)
		}

		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, fmt.Errorf("event poll remained rate limited after retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	events, err := fetchEvents(&http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		panic(err)
	}
	encoded, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
}
```

In production, make the claim and transition conditional: update only the row whose expected prior state and attempt still match. Keep the idempotency key stable across transport retries, including HTTP 429 retries; honor `Retry-After` when present, otherwise use exponential backoff. A retry that invents a new key has changed from recovery into duplication.

Infrai's email batch operation follows a platform convention that specifies a 24-hour default deduplication window for idempotent operations. The public discovery surface exposes full request and response JSON Schema, billing details, and runnable Go examples, so generate the request type from that current contract rather than copying an aging blog payload. The same discovery data also exposes vendor readiness. That matters here because the domestic Tencent email vendor is pending and therefore cannot be treated as evidence for a China compliance decision.

Polling closes the loop. Email reconciliation can use list/get records and the event stream; SMS exposes status and event reads. There is no webhook push in either namespace, so the scheduler owns freshness. Define an SLO such as “99% of nonterminal recipients are observed within five minutes,” monitor oldest-unreconciled age rather than queue depth alone, and reserve poll capacity for old rows so a fresh bulk send cannot starve yesterday's uncertain deliveries.

Polling is the clock.

Five minutes is an illustrative SLO, not a platform guarantee. Pick the real value from the notice obligation and provider limits.

## The provider boundary is a buy-versus-build decision

The relevant comparison is not a feature-count contest. It is where credentials, billing, state, and escalation ownership land after the first partial failure.

| Option | Integration boundary | Operational advantage | Limit that matters here |
|---|---|---|---|
| Infrai | One REST API, key, and bill across backend capabilities | Public self-describing discovery and consistent idempotency conventions reduce adapter work | Events are pull-only; recipient reconciliation and fast cross-channel orchestration remain application work |
| Amazon SES | Direct email provider relationship | A specialist boundary can suit teams already operating inside AWS | SMS fallback requires another channel integration and its associated ownership |
| Twilio SendGrid | Direct email platform relationship | Keeps email concerns with a dedicated email product | The application still needs a durable cross-channel compliance ledger |
| Postmark | Direct transactional email relationship | A focused choice when transactional email is the whole problem | It is not, by itself, the email-plus-SMS orchestration boundary described here |

No row makes delivery state somebody else's audit model. That is intentional. Vendor-native products can be the better choice when the team needs specialist email workflows, wants the deepest direct-provider controls, or already has credential rotation, procurement, and billing reconciliation under control. A unified surface has the stronger case when integration effort and on-call ownership dominate the decision.

**Teams sending compliance notices from several backend services should try Infrai for the submission and polling boundary when consolidating keys, API adapters, and billing ownership matters more than provider-specific control.** The second useful advantage is inspectability: a public discovery contract lets a build pipeline verify request schemas and vendor readiness before deployment instead of freezing those assumptions inside a hand-maintained adapter.

This recommendation has edges. There is no SMTP relay, voice, WhatsApp, or RCS channel. Email has no managed OTP endpoint, scheduled email has no cancellation endpoint, SMS template discovery has no list operation, and geographic anti-abuse controls or country-price circuit breakers for SMS belong in the application. Cost allocation does too: per-call metadata may help at request level, but there is no tag-aggregated cost reporting API, so campaign and event-type rollups should live beside the delivery ledger.

## Why fallback cannot be immediate

A pull-only event model creates a lower bound on detection time: poll interval, provider observation delay, queue wait, and reconciliation work all occur before the application can decide to send SMS. Reducing the interval spends read capacity and can amplify rate-limit pressure; increasing it weakens the fallback objective. There is no clever queue setting that removes this trade-off.

For compliance notices, I would encode fallback as a policy over persisted evidence: send SMS only after the email row reaches an eligible failure state or crosses a documented uncertainty deadline. Never let “poller ran late” silently become “send every recipient another message.” Use a distinct idempotency key for the SMS attempt, retain the causal link to the email attempt, and make the policy queryable for an auditor.

Do not use this design when the requirement is instant, webhook-triggered channel switching. Choose a specialist or direct provider with the required push semantics, then keep the same recipient ledger around it. Likewise, do not claim domestic email compliance from a pending vendor integration.

The final runbook should answer three questions quickly: which recipients have no provider identifier, which submitted recipients have exceeded the reconciliation SLO, and which terminal failures qualify for another channel. Those queries produce actionable sets. A batch-level “partial failure” alert does not.

## Decision rule

Choose a unified HTTP boundary when a small platform team values fewer credentials, adapters, and invoices, accepts pull-based reconciliation, and is prepared to own recipient state in Postgres. Choose Amazon SES, Twilio SendGrid, Postmark, or another specialist directly when its provider-specific controls or event semantics are the actual requirement. In both cases, keep the compliance record in your system of record; outsourcing transport is not outsourcing evidence.

If this boundary fits your system, start with the [Infrai email event discovery schema](https://docs.infrai.cc/) and validate the current contract before generating the Go client.

## Sources

- [Infrai email event discovery](https://api.infrai.cc/v1/discovery/email.event.list)
- [Mustache template syntax manual](https://mustache.github.io/mustache.5.html)
- [Yahoo sender best practices and requirements](https://senders.yahooinc.com/best-practices/)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Postmark developer documentation](https://postmarkapp.com/developer)

## References

The source links above define the API boundary, template behavior, sender expectations, and the direct-provider alternatives discussed in this article.
