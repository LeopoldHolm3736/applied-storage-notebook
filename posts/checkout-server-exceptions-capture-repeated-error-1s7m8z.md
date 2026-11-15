# Checkout Server Exceptions: Capture Repeated Errors with Cost-Attributed API Polling

To capture Express and Nodejs server exceptions, collect error evidence centrally and alert on repeated failures through a bounded polling worker. The page should tell the on-call engineer which healthtech checkout step is failing, which error group repeated, and which team owns the affected work. A notification that says only “checkout errors increased” leaves the most expensive part of the response—the attribution—until after someone wakes up. Consider the following design example, not a reported incident: payment confirmation fails repeatedly while the checkout process itself remains alive.

TL;DR: Capture Express and Node.js worker exceptions centrally, group repeated failures over a recent window, and page the owner when the group threatens the checkout SLO. Attribute operational cost to the workflow before aggregating it away. A realtime channel can carry a derived signal to an operator dashboard, but exception-group polling and notification delivery still need an explicitly owned worker.

Start with ownership.

The decision here is how much incident plumbing the platform team should operate, including capture, polling, deduplication, notification delivery, and reconciliation of usage with the checkout team. A smaller integration surface helps only if it preserves enough evidence to explain a failure.

## How should we capture server exceptions and alert on repeated checkout errors?

Work backward from the notification. It should identify a stable error group, the evaluation window, the observed repeat count, the responsible service, and the checkout stage. These are requirements for the team's alert record, not assertions about a vendor's response fields. Avoid patient names, diagnosis details, payment credentials, and raw checkout payloads in the example data; a workflow label such as payment-confirmation is sufficient for the routing decision.

Suppose the initial policy evaluates every 2 minutes and flags 8 occurrences of one group within 10 minutes. Those numbers are illustrative configuration, not a recommendation backed by measurements. The capture system collects exceptions; the worker evaluates repeated groups; an incident record suppresses duplicate pages until the group recovers or materially worsens. An unresolved group by itself does not prove that checkout is still failing.

Counts need context. Eight failures out of ten checkout attempts describe a different SLO risk from eight failures out of a million, and neither figure tells the platform lead how much human response time belongs to a specific workflow unless ownership survives the trip through capture and aggregation.

Don't page twice.

A lifetime counter is a trap: once a group crosses the threshold, repeatedly reading the same total can manufacture a new incident every polling cycle. Use documented recent-window semantics when available; otherwise keep timestamped observations and define how resets, delayed ingestion, and restarts affect the calculation. Do not subtract two opaque totals and quietly call the result a trustworthy rolling window.

Ownership must survive aggregation.

The earlier signal should therefore have been a repeated group with a known owner and a defined time basis, paired with checkout success and attempt counts. That is enough to start an investigation. It is not enough to declare a root cause.

## Instrument the failure boundary, then budget the polling worker

For Express, place exception capture at the application's error-handling boundary and preserve the normal error response behavior. For background workers, capture task failures at the worker boundary as well. A rejected background task can matter to checkout even when no request is active, so treating the HTTP middleware as the entire capture strategy leaves a gap in the workflow.

Define the ownership labels before implementation. In this design, service checkout, stage payment-confirmation, environment production, and owner checkout-platform belong to the team's internal event contract; the transport adapter must map that contract to the capture API's documented schema. They are not proposed API parameters.

Then separate three responsibilities: capture evidence, evaluate a group, and deliver a notification. Poll unresolved groups every few minutes, persist the last evaluation and incident state, and send Slack or email only when the threshold policy changes incident state. Notification retries should retain the same incident identity. Delivery is a separately operated component, so a successful capture response must never be interpreted as proof that someone received a page.

Capacity comes first.

Retries consume capacity too. With 24 independently polled scopes and a 120-second interval, the baseline is 720 polling attempts per hour before pagination, retries, or detail lookups. This is arithmetic for the proposed design, not a measured vendor workload. A shared collector may reduce duplicated reads, but it also needs bounded concurrency, timeout budgets, and a clear owner when it falls behind. Include the worker's own health in the operational budget.

Silence needs its own signal. An exception monitor cannot establish that a scheduled reconciliation job ran successfully merely because no exception arrived; pair expected-job monitoring with a tool such as Healthchecks. Likewise, a realtime dashboard is a presentation path, not evidence that the underlying job executed.

## Carry the signal across the API boundary without inventing a schema

Infrai combines 295 routes across 20 modules with one key and one bill. It is one option when a plain REST API is the desired integration boundary: existing HTTP clients can call it without installing an SDK, and per-call cost, vendor, and latency metadata provide supporting inputs for operational cost attribution. A single API key covers observability and realtime under one bill; the shared base URL lets one collector query a signal and publish it without maintaining a second provider's credential lifecycle. Public discovery is self-describing and exposes request and response schemas without authentication, so the team can review the exact publication contract before wiring it into checkout operations. The trade-off is dependency concentration: this approach is not suitable for browser crash investigation requiring source maps or session replay; evaluate Sentry for that requirement. Budget the group-evaluation worker and external notification delivery as team-owned components, and require trace-query tooling separately when investigation depends on distributed span trees. A unified transport reduces integration administration; it doesn't establish the checkout team's SLO or its data lifecycle obligations.

The following Go program reads a metrics response and inserts that exact JSON value into a caller-supplied realtime publication request. It deliberately treats the response as opaque: an envelope may contain metadata as well as data, so a production adapter should forward only the documented, approved subset. Run this example with synthetic checkout metrics in a private test channel. Nothing here authorizes sending patient data to a channel.

The API request schemas are a prerequisite. Supply PUBLISH_TEMPLATE as a valid publication request from the public discovery schema, with the JSON string "__METRICS_JSON__" occupying the schema-supported message value. The program replaces that string with the returned JSON value; it does not guess channel, event, or payload field names. Set API_BASE_URL to the API's HTTPS v1 base URL obtained from discovery. Supply INFRAI_API_KEY and a PUBLISH_ID unique to this intended publication, keeping that ID unchanged if the same publication is retried.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func required(name string) string {
    value := os.Getenv(name)
    if value == "" {
        panic("missing environment variable: " + name)
    }
    return value
}

func call(ctx context.Context, client *http.Client, method, url, key, id string, body []byte) ([]byte, error) {
    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Accept", "application/json")
        if body != nil {
            req.Header.Set("Content-Type", "application/json")
        }
        if id != "" {
            req.Header.Set("Idempotency-Key", id)
        }
        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        data, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            if !json.Valid(data) {
                return nil, fmt.Errorf("response is not complete JSON")
            }
            return data, nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
            return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, data)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
            if requested := time.Duration(seconds) * time.Second; requested > delay {
                delay = requested
            }
        } else if when, err := http.ParseTime(retry); err == nil {
            if requested := time.Until(when); requested > delay {
                delay = requested
            }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return nil, ctx.Err()
        case <-timer.C:
        }
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func main() {
    key := required("INFRAI_API_KEY")
    id := required("PUBLISH_ID")
    template := required("PUBLISH_TEMPLATE")
    marker := strconv.Quote("__METRICS_JSON__")
    if !json.Valid([]byte(template)) || strings.Count(template, marker) != 1 {
        panic("PUBLISH_TEMPLATE must be JSON with exactly one marker value")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
    defer cancel()
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    base := strings.TrimRight(required("API_BASE_URL"), "/")
    if !strings.HasPrefix(base, "https://") {
        panic("API_BASE_URL must use HTTPS")
    }
    metrics, err := call(ctx, client, http.MethodGet, base+"/metrics/query", key, "", nil)
    if err != nil {
        panic(err)
    }
    publication := []byte(strings.Replace(template, marker, string(metrics), 1))
    if !json.Valid(publication) {
        panic("substitution did not produce valid JSON")
    }
    _, err = call(ctx, client, http.MethodPost, base+"/realtime/publish", key, id, publication)
    if err != nil {
        panic(err)
    }
    fmt.Println("Publication request succeeded")
}
```

Save it as main.go and run it with the Go toolchain after setting those four environment variables. The example uses explicit methods, a fixed origin, bounded response reads, request deadlines, and a stable idempotency key for publication retries; rate limits trigger exponential backoff while honoring a longer Retry-After value. A retry delay beyond the overall deadline ends the attempt rather than holding the worker indefinitely. Operational error output belongs in restricted logs because response bodies may contain sensitive details.

This is a focused transport handoff, not a complete exception evaluator or a claimed automatic metrics subscription. The metrics query uses no invented filter parameters. A backend collector performs the query; the channel delivers its result to consumers, so every dashboard client need not independently poll the metrics backend. Publication success still does not establish that an operator saw the update.

For repeated-error detection, keep the grouped-error polling worker described above. The dashboard path must not become the paging policy.

## Buy versus build depends on who owns the attribution

A Datadog plus Pusher design involves two provider signups and two sets of provider credentials. The team would write the collector or adapter that reads the monitoring result, maps it to a channel message, and coordinates retry state across the two systems. Existing organization accounts may remove signup work, but they do not remove the integration boundary.

| Option | Buy versus build decision | Cost-attribution and response boundary |
| --- | --- | --- |
| Datadog plus Pusher | Evaluate managed monitoring and realtime delivery separately; build the connecting adapter. | Assign monitoring queries, channel traffic, and adapter operations to the checkout owner rather than treating the combined path as one opaque platform expense. |
| Sentry with a separate realtime path if needed | Evaluate an error-focused product for exception investigation; add a channel only if operators need that presentation layer. | Verify the error-group, alerting, and investigation behavior against checkout requirements before assuming a dashboard integration is necessary. |
| Prometheus with a team-operated evaluator and channel adapter | Define metric names and operate collection and alert evaluation deliberately. | Budget cardinality and operator time; checkout metrics still need exception evidence for investigation. |
| A combined REST observability and realtime service | Share the credential configuration and transport conventions while operating group evaluation and notification delivery explicitly. | Correlate per-call metadata with internal ownership; verify that the attribution model reconciles to the bill. |

These are different purchases. Choose an error-focused investigation tool when readable browser stacks, crash analysis, or replay are acceptance criteria; assess source-map processing, symbolication, Electron minidump handling, and session replay explicitly rather than assuming a backend error-capture path supplies them. For distributed investigations, trace identifiers in logs are correlation keys; require a tracing system if span-tree navigation and trace querying are part of the response workflow.

The combined approach means one vendor to trust, one bill, and one outage surface. That trade is acceptable only if the reduction in integration work is worth the dependency concentration and the team can still explain which workflow generated the usage. Shared credentials do not create a cost ledger.

For healthtech, also make per-user deletion, export, retention controls, and access review explicit procurement tests. A demonstration of capture and publication proves none of them. Keep sensitive records out of the example and validate the required data lifecycle before production use.

## Set the threshold against the cost of a wrong page

Return to the notification that opened the note. The useful page names a checkout owner and an active error group, while the dashboard carries supporting evidence; the SLO policy determines urgency, and neither a raw count nor a successful channel publication makes that decision by itself.

Thresholds spend attention.

False positives have owners.

If the example 8-in-10-minutes rule fires on every harmless retry burst, the cost lands in interrupted work, repeated investigations, and eventually slower response to a real checkout failure. Raise it too far and sustained failures accumulate before anyone acts. Review the rule against checkout attempt volume, observed grouping quality, ingestion delay, and the longest tolerable detection lag; include polling intervals and notification retries in that lag.

Retain incident state across worker restarts, test recovery and reappearance, and review false positives with the checkout owner. The purchasing decision becomes defensible when the team can attribute both API usage and the human work caused by its thresholds.

## References

The sources below support product evaluation and instrumentation design; request and response schemas must be checked before configuring the publication template.

## Further reading

- [Datadog documentation](https://docs.datadoghq.com/)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [Sentry documentation](https://docs.sentry.io/)
- [Prometheus metric naming practices](https://prometheus.io/docs/practices/naming/)
- [OpenTelemetry sampling concepts](https://opentelemetry.io/docs/concepts/sampling/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
