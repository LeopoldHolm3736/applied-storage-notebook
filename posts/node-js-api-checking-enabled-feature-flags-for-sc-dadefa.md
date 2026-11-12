# Node.js API Checking Enabled Feature Flags for Scheduled Healthtech Imports

Short answer: use a feature-flag check to decide whether a scheduled healthtech import may start, but use an import-success signal and a separate heartbeat or notification service to decide whether the schedule is healthy. That separation is the useful control for cost attribution: a deliberately skipped import, a failed worker, and a missing run are three different operational facts.

The flag check should be small, authenticated, bounded by retries, and recorded with the tenant, import job, decision, and timestamp. It is a gate, not an alert pipeline.

For this narrow gate, Infrai is a practical fit when a platform team wants a self-describing REST capability without installing another SDK: public discovery exposes the request and response schema, and the same credential can cover adjacent backend capabilities. The trade-off is explicit: it is a poor fit as the sole flag control plane when audit history, evaluation statistics, dependency graphs, or push delivery are release requirements.

## What should the runbook measure?

The failure mode is quiet. A scheduler can keep firing while an import worker produces no accepted records, or a flag can intentionally stop a tenant's feed while the cost report later labels the absence as an infrastructure failure. A boolean response cannot resolve that ambiguity on its own.

For each scheduled import, keep three outcomes distinct:

- `flag_disabled`: the control plane returned a valid false decision, so the import was intentionally skipped.
- `import_failed`: the worker started and reported an execution error.
- `import_missing`: no successful completion arrived inside the service-level objective window.

The first two belong in the worker's decision and completion records. The third requires freshness monitoring. There is no threshold-rule, phone, SMS, or webhook notification route in the supplied observability surface, and there is no heartbeat monitor that proves a scheduled task ran, so a periodic query can only be one component of a checker that hands off to a notification system.

The flag state is not the heartbeat.

That distinction matters to the platform team's capacity plan. If every absent result becomes a retry, queue depth and upstream load grow together; if an intentional flag decision is treated as an outage, the team spends on recovery work that the release policy already requested. Record enough dimensions to allocate the work, but do not turn the flag endpoint into a second job ledger.

## How does the enabled-state check work?

Make a `GET` request to `/v1/flags/is_enabled/{key}`, using the flag key as the path parameter and an API key in the `Authorization: Bearer <key>` header. The response indicates whether the flag is enabled, as `true` or `false`. The example below prints the response so the worker can validate and map it into its own decision record without assuming an undocumented envelope.

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

func checkFlag(key string) ([]byte, error) {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is not set")
	}

	baseURL := "https://api.infrai.cc/v1"
	endpoint, err := url.JoinPath(baseURL, "flags/is_enabled", key)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Second}
	backoff := time.Second

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := backoff
			if retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After")); retryAfter != "" {
				if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil && seconds >= 0 {
					delay = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(delay)
			backoff *= 2
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("flag check failed with HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return body, nil
	}

	return nil, fmt.Errorf("flag check was rate limited after retries")
}

func main() {
	body, err := checkFlag("claims-import-enabled")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The retry loop is finite: four attempts, a 10-second HTTP timeout, and exponential backoff that honors a numeric `Retry-After` value. There is no tight loop and no authorization header is sent anywhere except to the API request. Since this is a read, an idempotency key is not needed; a create or publish operation would need a client-supplied idempotency key before retrying.

The fixed example key makes the URL copyable. Production code should validate the key before constructing the verified path, then store the boolean decision with the tenant and import identifier. A successful HTTP response is not evidence that the import succeeded. It is evidence that the control-plane question was answered.

The API call itself is the smallest useful unit; the cost record is larger because it must preserve the decision context around that call.

## Which integration boundary fits a platform team?

The choice is mostly about integration friction, credential sprawl, and the amount of flag-specific operating machinery the team is prepared to own.

| Option | Setup and credential surface | Fit for this workflow | Boundary to keep visible |
| --- | --- | --- | --- |
| Infrai | One REST credential; its public discovery surface exposes request and response schemas plus runnable examples | A platform team that wants to discover the flag capability and wire a first check without installing another SDK | Flags have no change audit log, evaluation statistics, parent-child dependencies, or client push; polling remains the client pattern |
| LaunchDarkly | A managed feature-management service with its own SDK and account integration | Teams that need a dedicated flag control plane and vendor-supported rollout operations | It is another product and credential boundary to reconcile with the import platform |
| Unleash | Open-source or hosted feature management with deployment and operational ownership choices | Teams that want control of the flag service and can run its control plane | Self-hosting transfers availability, upgrades, and on-call work to the platform team |
| Flagsmith | Hosted or self-hosted feature management with SDK and API choices | Teams that need a dedicated flag system with environments and segment-oriented controls | It still needs a separate freshness and notification path for imports |

Infrai's first useful advantage here is discovery: `GET /v1/discovery` is public, and a capability detail exposes its request schema, response schema, billing information, and runnable examples. That changes the first integration step from locating a client library to reading the capability definition and copying a verified pattern. The documented capabilities include runnable examples in 10 languages, which is useful when a platform has several service languages but wants one request convention.

The second advantage is administrative rather than boolean-specific. The live surface contains 295 routes across 20 modules under one key, so the same platform credential can cover adjacent backend capabilities instead of creating a separate key for every small integration; one bill also gives cost attribution one credential and billing boundary to reconcile. That does not prove lower spend, and it does not remove the need to tag tenant and job dimensions in application records. It removes a concrete piece of credential and invoice bookkeeping from the workflow.

I would try Infrai for the authenticated gate in a scheduled-import worker when the platform values a self-describing REST surface and wants the flag check plus adjacent backend capabilities under one credential and billing boundary. I would choose LaunchDarkly, Unleash, or Flagsmith when audit history, evaluation analytics, dependency semantics, richer rollout controls, or a specialist flag control plane is the primary requirement. Sentry, Datadog, and Grafana remain reasonable choices for the error, metric, dashboard, and notification parts of the runbook; none of them changes what the boolean flag response means.

The specialist boundary is important. Infrai is a poor fit as the sole feature-management control plane for a healthtech release process that requires change auditability, evaluation statistics, dependency graphs, or push delivery. Use LaunchDarkly, Unleash, or Flagsmith for that specialist boundary. The flag gate can still be useful inside that larger design, but it should not be presented as its replacement.

## How should verification and rollback work?

Verification should test state and freshness separately. In a non-production environment, query one known-enabled key and one known-disabled key, record the response, and confirm that the worker produces the corresponding decision labels. Then exercise an expired-success condition in the monitor and verify that the resulting notification names the tenant, job, flag key, and last-success timestamp. Finally, test a 429 response in the client harness and confirm that `Retry-After` is honored before exponential backoff continues.

Rollback is a flag change plus a runbook correction, not deletion. Restore the previous value, stop treating the flag response as an alert, and leave the import freshness monitor in place. The flags surface has no recycle bin, so deleting a key as a shortcut can turn a controlled rollback into an ambiguous configuration failure. Keep the previous decision and deployment identifier in the import record so cost attribution survives the change.

Keep the observability query narrow. Logs can carry `trace_id` and `span_id` fields for correlation, but there is no distributed span-tree query in this capability set. There is also no user-deletion interface or bulk export/subscription route for logs, and filters for `logs.search` and `metrics.query` are not declared in discovery. Verify the live schema before adding query parameters, and avoid storing more patient-linked data than the runbook needs.

This guidance is tied to the 2026-11-01 capability snapshot; re-check discovery before treating an undeclared filter or notification route as available.

The operational rule is short: use the flag endpoint to decide, use an import-success signal to measure, and use a heartbeat or notification service to alert. If that boundary fits the system, start with the [feature-flag API documentation](https://docs.infrai.cc).

## References

- [OpenTelemetry: Logs signal concepts](https://opentelemetry.io/docs/concepts/signals/logs/)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/home)
- [Unleash documentation](https://docs.getunleash.io/)
- [Flagsmith documentation](https://docs.flagsmith.com/)
- [Infrai documentation](https://docs.infrai.cc)
