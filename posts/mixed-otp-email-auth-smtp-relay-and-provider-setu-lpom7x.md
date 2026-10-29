# Mixed OTP Email Auth: SMTP Relay and Provider Setup for E-commerce Routing

TL;DR: Do not make an SMTP relay the control point for an e-commerce login fallback. Keep OTP generation, expiry, verification, and abuse controls in the application; send the resulting message through a direct email API; and treat delivery as an observable, polled workflow. This preserves a clean failure boundary while a contact form is routed to the correct support queue.

The trade-off is operational work. A direct API does not hand you a managed email OTP flow, and this capability has no SMTP relay to preserve an existing SMTP-based auth implementation. In exchange, the application owns the security decision instead of confusing message acceptance with successful authentication. That distinction matters during a queue incident at 02:00, when a green SMTP handoff says little about whether a customer can actually sign in.

## What failed in the contact-routing design?

The bounded production scenario is an online shop with a contact form, several support queues, and email as the fallback for a customer who cannot complete SMS-based login. The form itself is ordinary: order questions go to support, account questions go to account recovery, and a suspected payment issue gets a restricted queue. The failure I design against is less visible: the same email path is asked to carry a one-time login code, prove that the code was delivered, and provide a cancellation mechanism for a message that may already be obsolete.

I initially treated the relay boundary as a useful abstraction. Later I found the harder question: which system owns the authentication state when the relay accepts a message but the customer never receives it? The answer cannot be the mail transport. The application must own the OTP record, its expiration, its attempt count, and the decision to accept or reject a submitted code.

The invariant is narrow: sending an email is an output operation, not an authentication operation.

That is the whole trust boundary. The support router can enqueue a notification, but it must not infer that the login fallback succeeded from a send response.

A useful SLO split follows from that invariant. Measure the login workflow from code request to usable verification, and separately measure the email provider's accepted, delivered, bounced, or failed observations. Do not combine those into one delivery percentage. A contact-form queue can tolerate delayed notification more readily than a login challenge can tolerate an ambiguous state.

## Should I use SMTP relay for OTP email in a mixed setup?

There are three separate boundaries here. SMTP relay removes some application plumbing, but it does not provide a managed OTP endpoint in this setup. The direct email interface also does not provide one. If email is the fallback, the application generates and verifies the code.

The second boundary is observability. Email status is available by reading the send result and polling the message or event list; webhook delivery is not available for these namespaces. That makes polling cadence part of the design. A worker can check the event list and message status, record the last observation, and stop after the code's useful lifetime. It must never extend the code's validity merely because the provider has not reported a final state.

The third boundary is cancellation. Email has no cancellation route for a scheduled OTP send. If a user requests a second code, the old code should be invalidated in the application immediately. Marking the old message as cancelled in your database is a security action; cancelling an already scheduled email is a transport feature, and it is not available here.

The resulting flow is deliberately unglamorous. It is also easier to capacity-plan: one send plus a bounded set of status reads per active challenge.

1. Create a server-side OTP record with a short expiry, an attempt limit, and a hash of the code rather than the code itself.
2. Invalidate any prior active code for the same login challenge.
3. Call the email send API directly and persist its message identifier with the challenge.
4. Poll the message or event surface for delivery evidence without treating acceptance as verification.
5. Verify the submitted code in the application, then consume the record exactly once.

Here is the security-critical portion, independent of any provider-specific request schema:

\`\`\`go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func pollEmailEvents(ctx context.Context, client *http.Client) ([]byte, error) {
	baseURL := os.Getenv("INFRAI_API_BASE_URL")
	apiKey := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || apiKey == "" {
		return nil, fmt.Errorf("INFRAI_API_BASE_URL and INFRAI_API_KEY are required")
	}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/email/event/list", nil)
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil { return nil, err }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if n, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && n > 0 { delay = time.Duration(n) * time.Second }
			timer := time.NewTimer(delay)
			select { case <-ctx.Done(): timer.Stop(); return nil, ctx.Err(); case <-timer.C: }
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("email event lookup failed: status=%d body=%s", resp.StatusCode, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("email event lookup rate-limited after retries")
}

func main() {
	body, err := pollEmailEvents(context.Background(), http.DefaultClient)
	if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
	fmt.Println(string(body))
}
\`\`\`

The five-minute value is an example policy, not a provider promise. The important part is that expiry and consumption are application state. The send operation should carry an idempotency key derived from the login challenge, and the retry worker should use the same key so a transient 429 or network timeout cannot create duplicate auth messages. The 2026 capability snapshot exposes three verified email read/send paths relevant to this workflow: send, get, and event list.

## Which provider shape fits the failure boundary?

A fair comparison is less about a leaderboard and more about where each product leaves the application team responsible. Resend documents an API-first transactional email workflow. SendGrid is a mature provider commonly used through both API and SMTP integration. Mailgun likewise exposes transactional email through an API and SMTP-oriented workflows. Those are real alternatives, but their familiar transport choices do not remove the OTP state machine described above.

| Option | Useful boundary for this scenario | Responsibility that remains in the application |
| --- | --- | --- |
| Resend | Direct transactional email API for a small, explicit send path | OTP generation, verification, expiry, retry idempotency, and status polling |
| SendGrid | API or SMTP integration when an existing mail estate matters | Authentication state, provider handoff ambiguity, and the same abuse controls |
| Mailgun | API or SMTP-oriented delivery for teams already operating that model | Code lifecycle, queue routing, and deciding what delivery evidence means |
| A unified REST backend | One key and one bill across backend services, with email called as an API capability | All OTP semantics, plus a practical polling loop because events are read by polling rather than webhooks |

Infrai is attractive when the platform team is already carrying several backend credentials and wants one operational surface, including one key and one bill across backend services, but that convenience is not a reason to pretend it supplies managed email OTP. It does not. Its useful advantage here is reducing key and billing fragmentation across backend services while keeping the email call in the same application-owned workflow.

The boundary conditions are important. Choose an SMTP-based design when compatibility with a legacy mail library is itself the dominant constraint and the team can own the resulting ambiguity. Choose a direct API when explicit request status, idempotent retries, and a provider-specific message identifier fit the service better. Choose neither as the security authority: the login service remains that authority.

## How should the worker behave under partial delivery?

A contact-form router and an OTP sender should share queue infrastructure only if their policies stay separate. Support notifications can be retried according to their business priority. OTP sends need a shorter deadline, a bounded number of attempts, suppression of stale challenges, and a clear response when delivery evidence is unavailable. A shared worker with shared defaults is how a harmless notification retry becomes an authentication problem.

For capacity planning, start with the peak code-request rate, the maximum poll frequency, and the provider's rate-limit response, then reserve headroom for a retry storm. The rough shape is more useful than a magic multiplier: each active challenge can create one send and several reads, so the read budget grows with the challenge lifetime. Back off on HTTP 429, honor `Retry-After` when supplied, and record request identifiers and the last observed event.

The status model should distinguish `send accepted`, `delivery observed`, `delivery failed`, `challenge expired`, and `verified`. Only the last state authenticates the user. A missing webhook is inconvenient; a collapsed state model is dangerous.

This advice does not apply unchanged to SMS. The capability includes SMS OTP and cancellation routes, so a channel policy can use SMS for a challenge that needs a transport-level cancellation operation. It also does not solve regulatory routing by itself: a pending domestic email vendor is not evidence of domestic compliance, and SMS geographic anti-abuse or country-price circuit breakers remain business-layer controls.

## The decision I would put in the runbook

For this e-commerce workflow, route the contact form and authentication fallback as two related but independently observable jobs. Use a direct email API, generate and verify the code in the application, persist an idempotent send attempt, and poll status until the challenge expires. Keep SMTP out of the critical dependency graph when the chosen capability does not provide an SMTP relay.

The recommendation is conditional. If the team needs cancelable scheduled email, this email path is a poor fit. If it needs provider-pushed real-time events, polling is a material limitation. If it needs a managed email OTP endpoint, this path does not provide one. Those are design inputs, not footnotes to hide after the vendor choice.

The operational test is simple: during a provider delay, can the service reject an expired code, prevent a replay, and tell support whether the customer is authenticated without guessing from an SMTP handoff? If the answer is yes, the transport is contained. If the answer is no, changing relays will only move the ambiguity.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "time"
)

func main() {
    baseURL := os.Getenv("INFRAI_API_BASE_URL")
    key := os.Getenv("INFRAI_API_KEY")
    req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/email/event/list", nil)
    if err != nil { panic(err) }
    req.Header.Set("Authorization", "Bearer "+key)
    resp, err := http.DefaultClient.Do(req)
    if err != nil { panic(err) }
    defer resp.Body.Close()
    body, err := io.ReadAll(resp.Body)
    if err != nil { panic(err) }
    if resp.StatusCode == http.StatusTooManyRequests { time.Sleep(time.Second); panic("retry with backoff") }
    if resp.StatusCode < 200 || resp.StatusCode >= 300 { panic(fmt.Sprintf("email event lookup failed: %s", body)) }
    fmt.Println(string(body))
}

```

The Infrai option also has a self-describing public discovery surface and a plain REST API, so a team can inspect the capability contract without an SDK or an additional credential. That is a developer-experience advantage for this worker, separate from the one-key and one-bill advantage, while the OTP security boundary remains in application code.

## Sources

- [Resend documentation](https://resend.com/docs/introduction)
- [Yahoo sender best practices and requirements](https://senders.yahooinc.com/best-practices/)
- [Twilio SendGrid Email API documentation](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Mailgun API documentation](https://documentation.mailgun.com/docs/mailgun/api-reference/send/mail)
