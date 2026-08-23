# Transactional Email Practices for Unsubscribe, Bounce Handling, and Polling APIs

Short answer: treat each generated health report as a durable delivery job, poll the email transport until it reaches a terminal state, and apply unsubscribe, complaint, and bounce suppression before every attempt; keep US and EU sending identities separate, warm each domain with traffic it can sustain, and page on the age of undelivered reports rather than on a single API error.

The page fires at 02:17: `report_delivery_age_seconds` has crossed the patient-notification SLO for 14 minutes. The on-call sees 63 reports still marked `accepted`, spread across two regions, while report generation itself is healthy. That distinction matters. If the page merely said "email failed," the responder would have to reconstruct the boundary between generation, attachment storage, submission, and final delivery while the queue continued to age.

Don't retry yet.

## How should Node.js transactional email polling handle bounce suppression without webhooks?

A Node.js service that has no webhook path should make polling a first-class control loop, not a timer attached to an HTTP request. Persist a stable job ID, the transport's message ID, recipient, region, sending identity, current state, next poll time, attempt count, and a content fingerprint. The worker submits only jobs that pass policy checks, then another worker polls status until the result is terminal. A process restart must lose no state.

The state machine can stay small: `queued -> submitted -> delivered`, with terminal branches for `bounced`, `complained`, `unsubscribed`, and `expired`. Keep `unknown` non-terminal because a delayed status response isn't evidence that a second copy should be sent. This is where an apparently harmless implementation causes duplicate health reports: a poll times out, the worker assumes submission failed, and it calls send again with a new idempotency key. The safer rule is to reconcile the original message ID before permitting another submission.

Polling cadence is a capacity-planning decision. If there are 600,000 outstanding messages and every job is polled every 10 seconds, the status plane must absorb 60,000 reads per second before it does any useful send work. Back off as a message ages, add jitter so workers don't synchronize, and put a hard concurrency limit around the adapter. A `429` is a control signal: reschedule the poll, preserve the message ID, and leave the delivery state unchanged.

A transport-neutral boundary keeps that policy out of application handlers:

```go
package delivery

import (
    "context"
    "errors"
    "time"
)

type State string

const (
    Submitted State = "submitted"
    Delivered State = "delivered"
    Bounced   State = "bounced"
    Complained State = "complained"
)

type Status struct {
    MessageID string
    State     State
    CheckedAt time.Time
}

type Transport interface {
    Status(ctx context.Context, messageID string) (Status, error)
}

type Job struct {
    MessageID string
    NextPoll  time.Time
    Attempts  int
}

func Reconcile(ctx context.Context, tx Transport, job Job, now time.Time) (Status, time.Time, error) {
    if job.MessageID == "" {
        return Status{}, time.Time{}, errors.New("missing message ID")
    }

    status, err := tx.Status(ctx, job.MessageID)
    if err != nil {
        delay := time.Duration(1<<min(job.Attempts, 8)) * time.Second
        return Status{}, now.Add(delay), err
    }
    if status.State == Delivered || status.State == Bounced || status.State == Complained {
        return status, time.Time{}, nil
    }
    return status, now.Add(30 * time.Second), nil
}
```

The example deliberately does not define a vendor URL. In production, the adapter owns authentication and response mapping, while the job store owns retry timing and idempotency. A Node.js implementation should preserve that same boundary even though its syntax differs; coupling status semantics to one SDK makes a later transport change much larger than it needs to be.

The catch is latency. Polling is not suitable when a workflow needs near-real-time delivery events or when status-read volume would threaten the control-plane budget; use signed webhooks when an inbound endpoint and its verification path are operationally acceptable. Stick with polling when inbound connectivity is prohibited, but accept that detection time is bounded by the polling interval.

## Transport integration boundary and adapter contract

Integration effort is larger than the first successful send. A managed transport can remove mail-server operation from the team's on-call scope, but the application still needs durable jobs, suppression policy, regional routing, observability, and reconciliation; a self-hosted transport gives the platform team more control over the path, while also making queue operation, reputation monitoring, capacity, upgrades, and incident response its responsibility. A split design usually keeps business policy and the delivery ledger in the application while placing transport behind an adapter. That boundary is attractive when provider portability matters, although it has a cost: engineers must define normalized states carefully, test every adapter against the same contract, and retain transport-specific evidence for diagnosis instead of flattening every outcome into `failed`. Choose the smallest ownership surface that can meet the SLO and regional constraints. A team without mail operations expertise should be skeptical of self-hosting; a team that cannot accept an external control plane should be equally skeptical of outsourcing the entire workflow.

Measure that boundary.

| Ownership choice | Integration work retained by the team | Main limitation |
|---|---|---|
| Managed transport | Job ledger, policy, polling, and observability | External control-plane dependency |
| Self-hosted transport | The full transport and policy stack | Larger on-call and capacity burden |
| Split control plane | Stable policy core plus adapter contracts | More contract testing and state mapping |

## Incident reliability timeline from page to leading signal

A provider error counter fires too early and too often. One rejected submission can be retried without user impact, while a growing queue can violate the notification objective even if every API call returns successfully. The leading indicators are oldest eligible job age, the number of submitted jobs without a terminal result, poll lag, and suppression-check failures. Delivery outcomes remain useful, but they trail the condition the on-call can still influence.

Define the SLO around the user-visible event: a generated report becomes available for delivery, then reaches a terminal notification state within the agreed window. Separate generation latency from delivery latency. Otherwise a fast mail system can hide a slow report pipeline, or the report generator can be blamed for a transport backlog.

This is also where regional isolation earns its keep. US and EU queues need independent concurrency limits, identities, dashboards, and alert routing; sharing one global worker pool lets a burst in one region consume the other's polling capacity. Keep the report itself out of logs. Store identifiers and state transitions needed for reconciliation, and treat attachment access and authentication as a separate security boundary rather than embedding reusable credentials in observability data. NIST's digital identity guidance is a useful primary reference for that authentication boundary.

I'm not sure what queue-age threshold fits your clinical workflow, and a generic number would be theater. Resolve it from the notification commitment, observed generation time, and the remaining error budget, then test the alert with delayed synthetic jobs.

## Recipient governance for unsubscribe and suppression policy

Suppression is a policy decision, not cleanup after delivery. Before the first submission and before any resubmission, check a canonical recipient key against unsubscribe, hard-bounce, and complaint records. Perform that check in the same durable workflow that claims the job, so a late unsubscribe cannot race a retry sitting in another queue. Preserve the reason and effective time; don't rely on a single boolean that erases why delivery stopped.

Transactional messages still need classification. A patient-requested report and a promotional follow-up can share an address while having different lawful and operational expectations, so the job should carry a message class and the policy engine should decide which suppression scopes apply. The system must not quietly relabel bulk traffic as transactional to bypass an unsubscribe.

Bounce handling needs the same skepticism. A terminal hard bounce should close the job and update suppression before another queued report is eligible. A temporary outcome can return to a bounded retry schedule, but retries need an expiry tied to the usefulness of the report notification. After expiry, record a terminal state and move the case to the product's alternate notification workflow. Infinite retries are backlog concealment.

Amazon SES documentation is a primary example of a managed email system's sending and deliverability concepts, but the architecture here does not depend on its API. The application owns the durable policy record; the transport supplies observations. That ownership line reduces integration effort during a migration because unsubscribe semantics, regional routing, and SLO calculations stay stable while only the adapter changes.

## Capacity cost of regional domain warmup

Domain warmup should look like a capacity rollout, not a calendar ritual. Split traffic by sending identity and region, begin with the recipients most likely to expect the report, observe bounce and complaint outcomes, and raise the allowed send rate only when the prior stage stays inside the team's thresholds. Pause expansion when the signal worsens. The exact schedule varies with list quality and mailbox-provider response, so a universal day-by-day volume table would pretend to know facts that only production telemetry can supply.

Use a release gate with an explicit rollback:

| Decision | Evidence | Action |
|---|---|---|
| Hold | Too little terminal-result data | Keep the current cap |
| Expand | Queue age is healthy and negative outcomes remain inside policy | Raise the regional cap by one planned step |
| Roll back | Queue age or negative outcomes cross the guardrail | Restore the previous cap and inspect recipient selection |

A dedicated new domain may improve isolation, but it also creates another reputation surface to operate. Reusing an established identity reduces rollout work, yet it can couple health-report traffic to unrelated mail. Neither choice wins universally. Choose according to blast radius, ownership, and the team's ability to maintain separate authentication and monitoring, then document the decision before traffic begins.

The instrumentation change is small but consequential: emit a state-transition event for every job, update queue-age gauges from the durable store, and derive rates from terminal outcomes by region and identity. Don't place recipient addresses, report names, attachment URLs, or message bodies in those events. A synthetic report with a non-sensitive attachment can exercise generation, storage, submission, and polling without exposing patient data.

False positives carry a real cost. If the queue-age page fires below the normal polling delay, on-call engineers learn to ignore it; if the warmup guardrail reacts to one event instead of a meaningful window, capacity repeatedly collapses and the backlog itself becomes the incident. Page on sustained risk to the SLO, send lower-confidence changes to a dashboard or ticket, and revisit thresholds after each deliberate capacity step.

Quiet isn't the goal.

Trustworthy paging is.

## References

- https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- https://pages.nist.gov/800-63-3/sp800-63b.html

## Further reading

- https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- https://pages.nist.gov/800-63-3/sp800-63b.html
