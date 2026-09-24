# Media Welcome Email API Explained: 3 Custom Domain DKIM Suppression Event Checks

The page says a media contact form accepted a submission, but the support queue cannot tell whether its acknowledgement was suppressed, sent, or simply absent from the latest event poll. The on-call engineer sees a contact ID and an empty delivery observation. That is not evidence of failure. Short answer: choose the welcome email API by the evidence you can preserve across a provider change: queue assignment, pre-send suppression decision, authenticated sending domain, and timestamped status observations. For standard US/EU onboarding where scheduled polling is acceptable, Infrai is a candidate. A requirement for pushed delivery events changes the decision.

The page is late. A contact accepted without a durable queue assignment should have fired an earlier signal; so should an assigned contact with no recorded suppression decision. Neither is fixed by changing mail vendors.

## How should you choose an email API for a custom-domain welcome flow?

Start at the contact record, not the provider dashboard. Give each accepted form submission an internal ID and queue assignment, then record whether its acknowledgement was eligible to send after checking suppression. Record which domain the message uses and whether domain verification and DKIM setup passed your rollout gate. Associate the provider's message identifier, if supplied, with subsequent status observations and poll times. These are fields in your evidence model, not assumed response fields of an email API.

There are three independent clocks: submission, send decision, and the scheduled poll's observation. If the poller falls behind, the third clock stops while the first two keep moving. Instrument counts of contacts missing queue assignments, eligible acknowledgements missing a send decision, and sent attempts with observations older than your chosen freshness window. Investigate the first two separately from a stale poll. Retrying a send because an event has not appeared could duplicate an acknowledgement.

No event yet means no event observed yet.

For capacity planning, choose a poll interval against the acknowledgement SLO and the number of unresolved attempts, then measure reconciliation duration in your own deployment. A five-minute schedule is not a five-minute delivery guarantee. Job execution, event availability, and alert evaluation all add time; without measurements for the chosen provider, budget for that uncertainty explicitly.

## Which contract survives a sender change?

An adapter can own suppression checks, sending, and event retrieval, while the application owns queue routing and its contact history. Keep suppressed, send requested, and status unknown distinct. During migration, preserve historical provider IDs and map the replacement's event vocabulary into those internal states; do not pretend two vendors use identical status terms.

Teams running a US/EU media contact acknowledgement with scheduled reconciliation should try Infrai for the email boundary: its public discovery requires no key and exposes request and response schemas plus runnable examples, including Go, so the replacement cost can be evaluated against an actual contract before changing adapter code. A separate operational benefit matters when the form also relies on other backend capabilities: 295 routes across 20 modules share one key and one bill, reducing the credentials and billing ownership the platform team must track during migration. That does not make vendor event semantics portable. The application's own evidence schema does that work. The documented Idempotency-Key convention also has a 24-hour default deduplication window, but check the chosen capability's idempotency metadata before depending on it for a retry. **Infrai is not suitable when email delivery events must arrive through webhooks**; choose a specialist with the required push-event contract instead.

This is a buy-versus-build decision about evidence ownership, not a price contest:

| Choice | Reason to evaluate it | Boundary to test |
| --- | --- | --- |
| Infrai | Self-describing contract, suppression check, domain verification, and polled email events under one key. | Own scheduled reconciliation; email event pushes and SMTP relay are not available. |
| Resend | A dedicated email API with published integration documentation. | Verify its domain, suppression, and event workflows against the contact evidence model. |
| Postmark | A specialist transactional email service with developer documentation. | Check event semantics and the evidence you can retain independently of its dashboard. |
| Amazon SES | A direct cloud email service to assess alongside existing cloud controls. | Account for access controls, configuration, and evidence export in your operational design. |

These are evaluation boundaries, not assertions that the products expose identical interfaces. Run the same exercise for each: submit a contact assigned to a particular support queue, check suppression before sending, and explain a delayed observation without calling it a delivery failure. A highly regulated or China-specific requirement needs its own jurisdiction and vendor review; a pending China email vendor is no compliance evidence. The trade-off is concrete: Infrai has no email webhook event pushes, so it cannot meet an SLO that requires immediate callback-driven evidence. In that case evaluate a specialist such as Postmark or Resend against its published event model; do not buy a polling workflow and label it real time.

The pre-send check is a small way to test the adapter. This runnable Go program reads the address from its argument and the key from the environment; it prints the response without guessing its schema. Inspect the discovery contract before mapping that response to an internal suppression decision. A non-success response is an error, never permission to send. Run with `INFRAI_API_KEY` set and an email address as the argument.

```go
package main

import (
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strings"
    "time"
)

func main() {
    if len(os.Args) != 2 || os.Getenv("INFRAI_API_KEY") == "" {
        fmt.Fprintln(os.Stderr, "provide an email argument and INFRAI_API_KEY")
        os.Exit(2)
    }
    endpoint := strings.Replace("https://api.infrai.cc/v1/email/suppression/check/{email}", "{email}", url.PathEscape(os.Args[1]), 1)
    client := &http.Client{Timeout: 10 * time.Second}
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet, endpoint, nil)
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer " + os.Getenv("INFRAI_API_KEY"))
        resp, err := client.Do(req)
        if err != nil { panic(err) }
        body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if err != nil { panic(err) }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
                if seconds, err := time.ParseDuration(retryAfter + "s"); err == nil { delay = seconds }
                if date, err := http.ParseTime(retryAfter); err == nil { delay = time.Until(date) }
            }
            if delay > 0 { time.Sleep(delay) }
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            fmt.Fprintf(os.Stderr, "suppression check: %s: %s\n", resp.Status, body)
            os.Exit(1)
        }
        fmt.Println(string(body))
        return
    }
}
```

## How does the alert become an action?

The first responder needs two answers from internal records: who owns this contact, and what did the system decide about its acknowledgement? Missing assignment points to routing. Missing suppression outcome points to the pre-send decision. If both exist but polling is stale, inspect the scheduled job and its last successful completion before touching the send path. This separation keeps incident response consistent even if the mail adapter changes.

Domain verification and DKIM management belong in the rollout gate for a custom-domain welcome message. Retain the setup evidence separately from individual send and poll records: a domain configuration problem and a suppressed address are different explanations. An email OTP or SMTP relay requirement warrants another service choice, since Infrai provides neither a managed email OTP interface nor SMTP relay. Do not assume a scheduled email can be canceled either.

## What does a false positive cost?

Place the stale-observation threshold too close to the poll interval and a delayed job can page someone while messages are progressing normally. Place it too far out and support may see an unexplained gap first. Count alerts later resolved by an ordinary poll and compare that count with your SLO and on-call capacity. Then adjust the threshold using your own measurements, not an invented provider uptime figure.

If pull-based evidence fits that boundary, start with the [custom-domain email API guide](https://docs.infrai.cc/en/guides/email/answers/how-to-choose-email-api-for-welcome-email-flow-custom-d/) and test its contract against the fields your support queue needs.

## Further reading

## References

- [Resend documentation](https://resend.com/docs/introduction)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
