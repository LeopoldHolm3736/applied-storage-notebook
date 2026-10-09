# How to Build an SMS 2FA Login Backend Flow (and Poll Delivery Status)

Short answer: create one server-side verification record, render a versioned template owned by the platform team, send once, and poll from a bounded worker until the message reaches a terminal state. The login endpoint should never wait for delivery, and a failed delivery should never trigger a cascade of new OTPs. Return the same neutral response for every signup attempt, cap attempts and resends, expire the code, and let the user explicitly request another code.

Keep it dull.

The page arrives as `Signup verification delivery below objective`, scoped to a logistics region and template version. On-call sees attempted, accepted, delivered, failed, and expired counts; the age of the oldest nonterminal send; and the affected carrier or country when that dimension exists. It contains no phone numbers or OTPs. The useful question is not whether a send request succeeded. It is where accepted messages stopped becoming confirmed deliveries, and whether a template rollout moved that boundary.

## How should a backend flow poll SMS 2FA login delivery status?

An accepted send proves that a messaging service took responsibility for a request. It does not prove that a driver, dispatcher, or warehouse administrator received the verification link. A receipt can arrive later, report a terminal failure, or remain inconclusive; the internal state machine must keep those outcomes separate from authentication policy.

Track `created`, `accepted`, `delivered`, `failed`, and `expired`. Preserve the raw external status separately, because treating an unfamiliar value as failure creates false incidents and needless resends. Only explicit mappings advance state, and transitions are monotonic: a late poll cannot move a terminal record backward.

The earlier signal is backlog age. A growing count of accepted records whose next check is overdue usually appears before a delivery-ratio alert, because that ratio needs terminal observations. Pair backlog age with worker errors and poll latency. This separates a broken observer from an actual delivery problem.

That distinction matters.

Use two objectives. The request-path SLO measures whether the service durably creates an attempt within its latency budget. The delivery SLO measures eligible attempts reaching the chosen success state within a defined window. Exclude test traffic through an explicit flag, not a phone-number pattern, and document every denominator exclusion.

## Record intent before calling the sender

The durable record needs an opaque attempt ID, account reference, normalized destination fingerprint, immutable template version, creation and expiry times, attempt counters, state, next-poll time, and external message ID after acceptance. Store a hash of the OTP rather than the OTP. OWASP recommends cryptographically random codes, secure storage, single use, expiration, rate limiting, and consistent responses that do not reveal whether an account exists. Those controls remain inside the application boundary even when transport is purchased.

Template ownership is the consequential choice here. A platform-owned template gives security review, localization, link-host validation, rollout history, and rollback one home. A transport-owned template may remove renderer code, but moves change control and audit evidence across an external boundary. Do not accept arbitrary message bodies on the send path; accept a template identifier and typed data, then render a reviewed version.

One transaction should create the attempt and an outbox item. A worker claims that item and sends with an idempotency key when supported. If the result is ambiguous, reconcile before sending again. Exactly-once delivery is not a credible assumption across a database and an external system, so bound duplicate effects.

In a Node.js and Express service, the HTTP handler should do only the durable creation step and return a neutral response; a queue worker owns send and poll work. The Go example below expresses the worker contract because the language is incidental to the boundary. This is also how the backend should handle failed OTP sends: record the terminal transport result, retain the authentication limits, and wait for an explicit user action before creating a replacement attempt. Moving polling into an Express request handler would tie web capacity to an external status API, lose work when the process restarts, and turn slow delivery into slow login responses.

```go
package verification

import (
    "context"
    "errors"
    "time"
)

type State string

const (
    Accepted State = "accepted"
    Delivered State = "delivered"
    Failed State = "failed"
)

type Receipt struct { State State; Raw string }
type Transport interface {
    Status(context.Context, string) (Receipt, error)
}
type Attempt struct {
    MessageID string
    State State
    Polls int
    NextPollAt time.Time
    ExpiresAt time.Time
}

func Poll(ctx context.Context, t Transport, a Attempt, now time.Time) (Attempt, error) {
    if a.State == Delivered || a.State == Failed || !now.Before(a.ExpiresAt) {
        return a, nil
    }
    if now.Before(a.NextPollAt) { return a, nil }

    receipt, err := t.Status(ctx, a.MessageID)
    if err != nil {
        a.Polls++
        a.NextPollAt = now.Add(backoff(a.Polls))
        return a, err
    }
    switch receipt.State {
    case Delivered, Failed:
        a.State = receipt.State
    case Accepted:
        a.Polls++
        a.NextPollAt = now.Add(backoff(a.Polls))
    default:
        return a, errors.New("unmapped delivery state")
    }
    return a, nil
}

func backoff(polls int) time.Duration {
    delays := []time.Duration{5 * time.Second, 15 * time.Second, 45 * time.Second, 2 * time.Minute}
    if polls >= len(delays) { return delays[len(delays)-1] }
    return delays[polls]
}
```

Persistence locks and jitter depend on the queue and datastore. The contract does not: bounded polling, explicit mapping, no resend, no backward transition. Add scheduler jitter so a regional burst does not synchronize every check. Stop polling at expiry even if the external state remains nonterminal.

## Instrument the transition, not the request count

Emit a counter for each validated transition with low-cardinality labels: channel, region, template ID, template version, and normalized outcome. Record transition latency in a histogram. Keep account IDs, phone data, OTPs, full responses, and message IDs out of metric labels; put the opaque attempt ID in restricted logs so an operator can follow one trace without turning telemetry into a credential store.

A practical alert evaluates completed cohorts. For each five-minute creation bucket, wait until the delivery window closes, then divide delivered attempts by eligible attempts. Page only when multiple cohorts breach the objective and volume clears a capacity-derived floor. Ticket slow deterioration. Alert separately when the oldest due poll exceeds the worker objective, because a silent poller makes delivery look healthier than reality.

Capacity planning starts with bursts, not daily averages. If signup traffic peaks at 120 attempts per second and each nonterminal message needs four checks, the polling tier must absorb 480 checks per second before headroom, retries, or jitter. That is arithmetic, not a benchmark; replace both inputs with measured arrival rate and observed checks per attempt, then load-test the queue. Cap concurrent calls and expose saturation.

Here is the operational trap: the same 120-per-second burst can create a much later poll burst if every record receives the same schedule, while a transport slowdown increases the average number of nonterminal observations and therefore raises demand exactly when the dependency is least able to serve it. Size queue storage for the accumulated attempts, place jitter around each due time, and reserve worker concurrency for old records so fresh traffic cannot starve them. Then test a dependency timeout, a worker restart, and a template rollback as separate cases. The first exercises backpressure, the second durable claiming, and the third correlation by template version; one happy-path load test cannot establish all three properties.

Keep the alert actionable. Link it to cohort graphs, backlog, recent template versions, and the safe action for pausing a rollout. Do not make resend the first runbook action: resends can produce multiple codes, increase abuse surface, and muddy the original signal.

## Failed delivery is not permission to weaken verification

A terminal failure marks the transport attempt while preserving authentication limits. The user can request a replacement after the resend interval, but issuance must invalidate the previous code or enforce a single-current-challenge rule. Rate limits should cover destination, account, IP or device signals, and an overall ceiling; no single key handles distributed abuse.

Keep user copy boring: the verification message could not be completed, and another attempt may be requested after the displayed delay. Do not reveal whether a number belongs to an account. Never claim delivery from acceptance. Never log the code.

For a verification link, generate an unpredictable single-use token, bind it to the intended action and account attempt, use HTTPS, and expire it. Receiving it proves control of that channel for that attempt; it establishes no broader identity. SMS is exposed to threats including SIM swapping and phishing, so higher-risk logistics actions need stronger authentication than possession of a phone number alone.

## Buy or build the template boundary

The transport and template control plane are separate decisions. Treating them as one procurement choice obscures on-call cost.

There are real limitations. Platform ownership is a poor fit when the team cannot staff localization, approval, and emergency copy changes; external template ownership is a poor fit when policy requires repository-based review or rapid transport switching. The trade-off is control versus recurring operational load, and neither boundary removes the need to test delivery outcomes.

| Boundary | Platform-owned templates | Transport-owned templates | Decision evidence |
|---|---|---|---|
| Review | Versioned with application policy | Managed externally | Who approves text, links, and localization? |
| Portability | Variables remain stable | Migration recreates templates and IDs | How costly is an exit or second route? |
| Operations | Team owns validation and rollback | Team integrates publishing and audit behavior | Which plane can on-call inspect safely? |
| Evidence | Existing repository controls apply | Records require export | What must an auditor reconstruct? |
| Maintenance | More renderer and localization work | Less code, more boundary coupling | Which load fits team capacity? |

For a small platform team, I would keep template semantics, variable validation, and immutable versions inside the service boundary, while buying transport unless delivery itself is strategic. That judgment concerns ownership, not price. Self-hosting transport adds carrier relationships, deliverability work, abuse response, queue operation, and continuous failure handling; managed transport adds dependency risk, external governance, and switching work. Put engineer-hours and paging load beside contract terms.

I chose that boundary because template changes alter authentication behavior, while transport mechanics usually do not. It is still a choice with a maintenance bill.

The answer changes if non-engineers must publish regulated copy quickly or local routing rules require transport-specific templates. Resolve that through a controlled publishing workflow and export test. Quarterly, prove that a version can be rendered from source, deployed to a test route, correlated through delivery state, and retired without orphaning in-flight attempts.

## Tune alerts against their human cost

A low threshold on a sparse country-carrier slice will page on normal variance. A high global threshold will hide regional failure behind healthy traffic. Start from the delivery SLO and minimum actionable volume, use completed cohorts, and replay the rule against historical telemetry before paging. Record which incidents it would have caught and how many pages required no action.

Then review the threshold after traffic shape, template mix, or polling policy changes. Every false page consumes error-budget attention and teaches responders to distrust the signal; every delayed page prolongs the time new logistics users cannot verify accounts. The correct threshold is tied to an explicit user-impact objective and enough samples to justify waking someone.

## Further reading

- OWASP, Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- NIST SP 800-63B, Authentication and Lifecycle Management: https://pages.nist.gov/800-63-4/sp800-63b.html
- RFC 6238, TOTP: Time-Based One-Time Password Algorithm: https://www.rfc-editor.org/rfc/rfc6238
- Yahoo, Sender Best Practices and Requirements: https://senders.yahooinc.com/best-practices/
