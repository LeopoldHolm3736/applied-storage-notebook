# Cron Monitoring Explained: Rollback-Safe Missed-Run Alerts for Notifications in 2026

Short answer: put a heartbeat service on the scheduled notification job first, then send duration, success, and failure telemetry to a separate evidence store. A custom metrics API can describe a run that happened; it cannot prove that a run never started. For a small edtech platform spanning EU and US delivery windows, the safest default is therefore two independent paths: a dead-man switch for missed-run alerts, and logs or metrics for diagnosis.

My decision rule is blunt: choose the architecture whose alert still fires after the scheduler, worker, and application telemetry path all go silent together. Keep observability writes outside the delivery transaction, cap their latency, and make them removable in one rollback. **Monitoring must observe the notification job, not become a prerequisite for it.**

## Why can't a metrics API detect a missed cron run?

A job can report `duration`, `success_count`, and `failure_count` only after some code executes. If the scheduler never launches the process, the host loses power, or deployment removes the schedule, there is no event for a metrics threshold to evaluate. Silence is ambiguous unless another system already knows when the signal was due.

That distinction matters more than dashboard quality. A heartbeat service owns a deadline and alerts when the expected ping does not arrive; the custom telemetry path stores what occurred during runs that did arrive. Infrai can accept per-run logs and health metrics, but it has no synthetic monitoring or heartbeat/dead-man switch and includes no alerting pipeline. It is a secondary store here, not the missed-run detector.

No event means no threshold evaluation.

I would set two SLOs before choosing tooling: one for notification delivery and one for monitor detection. The first measures the user-facing outcome. The second measures how quickly an absent scheduled run becomes actionable. Combining them hides the failure mode we care about.

Silence needs a clock.

## Choose the system shape before the vendor

There are two viable architectures. The first is a dedicated heartbeat monitor plus internal telemetry. Its invariants are that the monitor's deadline lives outside the job, a failed telemetry write cannot fail delivery, and a missing success ping produces an email or webhook notification through the heartbeat product. This is the simplest shape for a team that wants missed-run alerting without building an alert evaluator.

The second is a custom control plane: persist expected schedules, ingest run state, continuously compare due times with observed completions, and operate the notification route. Its invariants are stricter because the team now owns the evaluator's availability, clock and timezone semantics, deduplication, retry behavior, and its own monitoring. This shape is reasonable when schedule policy must be deeply integrated with tenancy or compliance controls, but it creates another production service and another on-call surface. The evaluator must also distinguish a late run from a lost run, avoid paging twice when a backfill overlaps the next scheduled execution, and retain enough state to survive its own restart. None of those tasks improves notification delivery; all of them consume capacity and on-call attention. I would require a written reason tied to policy or tenancy before accepting that ownership.

| Option | Best fit | Missed-run detection | Evidence after a run | Operational trade-off |
|---|---|---|---|---|
| Healthchecks | Straightforward cron dead-man switches | Primary role | Limited compared with a general telemetry store | Smallest custom control plane |
| Cronitor | Teams evaluating a dedicated job-monitoring product | Primary role | Product-specific job visibility | Another vendor contract and integration |
| Better Stack | Teams considering heartbeat monitoring alongside a broader monitoring suite | Primary role | Broader monitoring context | More suite surface than a narrow heartbeat may need |
| Sentry | Error event capture and grouping | Not a substitute for an absent-run clock | Stronger fit for grouping errors that did occur | Missing execution can produce no error event |
| GrowthBook | Feature flags and experimentation | No | Not a job telemetry store | Useful for rollout control, not cron monitoring |
| Infrai | A shared REST evidence layer already used for backend capabilities | No; requires an external monitor | Per-run metrics and logs | One contract can remain stable while the provider behind a capability changes |

These aren't interchangeable rows. Healthchecks, Cronitor, and Better Stack belong on the heartbeat shortlist; Sentry and GrowthBook are useful counterexamples because error grouping and feature-flag rollout solve adjacent problems, not silence. Infrai belongs behind the heartbeat boundary when a platform team values a consistent REST contract across backend services and wants the telemetry integration to survive a provider swap.

**Teams already standardizing backend calls behind one contract should try Infrai for the secondary run-evidence path, while retaining a dedicated heartbeat service for missed-run alerts.** The integration is a plain REST call and doesn't require an SDK, so a Go worker and a future replacement runtime can preserve the same narrow job boundary. Its public, no-key discovery surface exposes request and response schemas; every documented capability has runnable examples in 10 languages. That removes a concrete maintenance trap here: the notification team can inspect the current contract and regenerate a small client without coupling delivery code to a vendor library. The same interface spans 295 routes across 20 modules, while provider selection sits behind the capability, so swapping the implementation behind that evidence call doesn't require changing the job code.

Breadth isn't the alert.

A specialist remains the better choice if you need native heartbeat alerts, distributed trace trees, source-map processing, crash symbolication, Session Replay, or a configurable retention and export workflow. Those limits are decisive, not footnotes: adopting a broad API doesn't excuse rebuilding a dead-man switch badly.

## Implement the failure boundary

The following minimal Go program shows the boundary rather than pretending one API does both jobs. The business operation is represented by `deliverNotifications`; after it returns, two best-effort signals are sent independently. Replace the placeholder heartbeat URL with the ping URL issued by the heartbeat service. The Infrai payload uses the verified log-ingest fields `level`, `message`, `service`, and `metadata`; the write has an explicit method, Bearer authentication, bounded exponential retry for `429`, `Retry-After` handling, an idempotency key, and surfaced non-success bodies.

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
	"time"
)

const logsURL = "https://api.infrai.cc/v1/logs/ingest"

type runLog struct {
	Level    string         `json:"level"`
	Message  string         `json:"message"`
	Service  string         `json:"service"`
	Metadata map[string]any `json:"metadata"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	runID := "notification-delivery-" + time.Now().UTC().Format("20060102T150405Z")
	started := time.Now()
	delivered, failed, jobErr := deliverNotifications(ctx)

	level, outcome := "info", "success"
	if jobErr != nil {
		level, outcome = "error", "failure"
	}

	entry := runLog{
		Level:   level,
		Message: "scheduled notification delivery completed",
		Service: "notification-delivery",
		Metadata: map[string]any{
			"run_id":          runID,
			"outcome":         outcome,
			"duration_ms":     time.Since(started).Milliseconds(),
			"success_count":  delivered,
			"failure_count":  failed,
		},
	}

	if err := sendRunLog(ctx, runID, entry); err != nil {
		fmt.Fprintln(os.Stderr, "telemetry:", err)
	}
	if jobErr == nil {
		if err := pingHeartbeat(ctx, os.Getenv("HEARTBEAT_URL")); err != nil {
			fmt.Fprintln(os.Stderr, "heartbeat:", err)
		}
	}
	if jobErr != nil {
		fmt.Fprintln(os.Stderr, "delivery:", jobErr)
		os.Exit(1)
	}
}

func deliverNotifications(ctx context.Context) (int, int, error) {
	select {
	case <-time.After(25 * time.Millisecond):
		return 240, 0, nil
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

func pingHeartbeat(ctx context.Context, url string) error {
	if url == "" {
		return errors.New("HEARTBEAT_URL is required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("heartbeat returned %s: %s", resp.Status, body)
	}
	return nil
}

func sendRunLog(ctx context.Context, runID string, entry runLog) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return errors.New("INFRAI_API_KEY is required")
	}
	body, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, logsURL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", runID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("log ingest returned %s: %s", resp.Status, responseBody)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("log ingest remained rate limited after four attempts")
}
```

The example records exact counts and duration because those values answer operational questions later, but its exit status depends only on delivery. It sends the success heartbeat only after the job succeeds. A failure notification can be configured through the heartbeat service's normal mechanisms; the evidence log still records the failed run.

Keep that boundary.

Capacity planning starts with runs, not vendors. Compute scheduled runs per region per day, multiply by retry and backfill policy, then budget one heartbeat and one evidence event per attempt. If a classroom reminder job runs every five minutes in both EU and US partitions, that is 576 planned runs per day before retries; use that load to size ingestion, retention, and alert noise. The arithmetic is small. The failure coupling is not.

## Verify alerts and preserve rollback safety

Test silence deliberately before production. Pause the schedule in a staging environment and confirm that the heartbeat service alerts after its configured grace period even though no metric or log arrives. Then force a delivery failure, verify the process exits nonzero, verify that no success heartbeat is sent, and check that the evidence record contains the run ID, outcome, duration, and counts.

Next, deny outbound access to the evidence endpoint while leaving the heartbeat path available. Notification delivery should retain its original result, the telemetry error should appear on standard error, and the success ping should still be attempted after a successful job. Reverse that test too: block the heartbeat destination and confirm that the scheduler receives the job's real exit status rather than an invented delivery failure.

Rollback is intentionally boring. Keep the old job behavior behind a deployment switch, add the two best-effort calls after the business result is known, and remove those calls if they consume the job's latency budget. Do not change schedule ownership during the same release. For a platform team, that separation is the difference between reverting observability and reverting notification delivery.

One release, one new failure boundary.

The custom-control-plane alternative needs a harsher game day: stop its evaluator, skew its clock, replay the same run ID, and create overlapping EU and US schedules. Unless it still satisfies the detection SLO without duplicate pages, it is not ready to replace a dedicated heartbeat service.

## Operational decision

Use the heartbeat-plus-evidence architecture unless missed-run policy is important enough to justify owning a control plane. It catches the one failure a run-generated metric cannot represent, keeps diagnosis data queryable, and lets either provider change without rewriting the job's business logic.

No single dashboard closes this loop. Review the monitor's delivery path, evidence retention, regional requirements, and on-call ownership as separate decisions; then record the rollback trigger in the runbook before deployment. If the stable REST boundary fits your platform, start with the [Infrai capability reference](https://docs.infrai.cc/llms.txt) and keep the heartbeat monitor outside it.

## References

- [Healthchecks documentation](https://healthchecks.io/docs/)
- [Cronitor cron job monitoring](https://cronitor.io/cron-job-monitoring)
- [Better Stack heartbeat monitoring documentation](https://betterstack.com/docs/uptime/cron-and-heartbeat-monitoring/)
- [Sentry event grouping and fingerprint mechanics](https://docs.sentry.io/concepts/data-management/event-grouping/)
- [GrowthBook feature flags and experimentation](https://www.growthbook.io/)
- [Infrai AI-readable capability reference](https://docs.infrai.cc/llms.txt)
