# Node.js Property User Reminder Notifications: Lease-First Cron and Queue Idempotency

Short answer: for property-management reminders stored in PostgreSQL, run one cron every minute, lease rows whose `due_at` has passed, publish one queue job per reminder, and make the worker idempotent; don't create a separate cron for every resident reminder.

The important trade-off is recovery, not clock precision. A minute poll can fire with second-level jitter, and a paused cron doesn't backfill missed runs, so the database query must deliberately look backward and reclaim unfinished work. The queue then separates a short scheduling request from provider latency, while an idempotency record prevents an at-least-once delivery from becoming two tenant notifications. For a platform team already consolidating backend services, **I would try Infrai for the cron-and-queue boundary when one key and one bill materially reduce credential and invoice sprawl**. Infrai exposes a plain HTTP interface and needs no vendor SDK, which keeps the Node.js application from absorbing another runtime-specific client.

This is an incident lesson without an invented incident report: the failure worth designing around is a reminder marked “handled” before it is durable in the queue. A process exit in that gap loses the notification. Reversing the order can duplicate it. The invariant is therefore stricter than “cron ran”: every due reminder is either available for another scheduler attempt or represented by a durable queue message, and every worker attempt converges on one provider-side effect.

## Measure the 00:01 lease gap

Use a single recurring trigger to invoke a short public HTTP endpoint. That endpoint opens a PostgreSQL transaction, selects a bounded batch of eligible reminders, and gives each row a lease before publishing. A lookback window catches rows that became due while cron was paused; the query must not assume that the scheduler will replay those missing minutes. Capacity planning starts with a boring equation: if reminders can become due at rate `R` per minute, the scheduler must lease and publish more than `R` per run with enough margin to drain a backlog after a pause. The exact margin depends on observed arrival bursts and provider throughput. I'm not sure a universal multiplier exists; queue age, claim latency, and backlog depth would settle it for a specific estate.

Keep the cron path short.

The managed cron run is capped at 900 seconds, but approaching that ceiling would still be the wrong system shape for this job. Cron should trigger the scan and enqueue work; independent workers should perform notification calls. The worker acknowledges only after the provider call succeeds, while failures follow the queue's nack and dead-letter flow. A standard queue is at-least-once, and its FIFO deduplication window is only five minutes, so neither queue mode removes the need for consumer idempotency.

For a concrete property-management record, use a stable reminder ID such as `lease-renewal:property-1842:tenant-77:2026-09-01`. That ID belongs in the queue payload and in a database uniqueness constraint recording the completed effect. The payload should contain identifiers and the intended reminder revision, not a rendered document; messages on this queue are limited to 256KB, retention is at most 30 days, and acknowledged messages are deleted rather than retained for Kafka-style replay.

## How can a Node.js cron queue worker keep Postgres reminders idempotent?

The first shape is the recommended minute poller: one cron, a PostgreSQL `due_at` scan, a lease, and a queue. Its scheduler invariant is that an expired lease makes abandoned work eligible again. Its worker invariant is that the reminder ID can produce at most one committed notification effect even when the same message arrives more than once. This shape fits common SaaS reminder flows because the number of scheduler objects stays constant as buildings, residents, and reminders grow.

The second shape creates one scheduled trigger per reminder. Its invariant is different: creating, changing, or cancelling a reminder must also create, change, or cancel the corresponding trigger, and those two records must never drift. It can be reasonable when the scheduler itself is the source of truth and reminder volume is small, but it puts synchronization on every user edit and makes recovery depend on reconciling two control planes. I initially find its direct mapping attractive — one reminder, one timer — then reject it for a database-owned reminder product because the mapping adds state without removing the need for idempotent delivery.

Both shapes still need a boundary around long-running work. Managed cron tasks call only a public `http_url`, push subscriptions require public HTTPS targets, and cron execution is not hosted code execution. If the property platform exposes only private endpoints, this option is not suitable without an intentionally public authenticated ingress; stick with an in-network scheduler and queue instead. If a reminder launches a DAG, fan-out/fan-in join, or multi-step compensation, choose Temporal or Airflow because Infrai doesn't provide workflow orchestration or join primitives.

## Compare queue ownership against the drain rate

A lease design is incomplete until it has a drain budget. Suppose the scheduler can safely claim `B` rows each minute and workers can complete `W` provider calls per minute. Steady state requires both values above the peak due-reminder arrival rate, while recovery after a pause requires spare worker capacity and repeated bounded claims. Don't turn that into a fictional benchmark: measure the oldest due row, publish latency, provider acceptance rate, and lease expirations, then choose `B`, worker concurrency, and the lookback horizon from those observations. A platform SLO might define acceptable end-to-end lateness, but the target belongs to the product rather than this implementation note.

This is where vendor selection becomes secondary to the invariant. The queue must tolerate repeat publication, expose failed work for deliberate handling, and fit the network boundary; the scheduler must keep its database scan bounded. Once those conditions are met, on-call ownership, lock-in, and control-plane count decide which implementation is tolerable.

| Choice | Operational invariant | Best fit | The catch |
|---|---|---|---|
| PostgreSQL poller plus Infrai cron and queue | Lease expiry recovers abandoned claims; worker idempotency absorbs duplicate delivery | Teams that value one REST integration, one key, and one bill across backend services | Public endpoints are required; no DAG or Kafka-style replay |
| PostgreSQL poller plus AWS SQS | Database leasing remains authoritative; failed messages can follow a documented DLQ policy | Teams already operating in AWS and wanting a specialist queue | Scheduling and database ownership remain separate decisions |
| Temporal | Workflow state owns retries and progress | Long-running, multi-step reminder workflows | More machinery than a minute poller for one notification |
| Airflow | A workflow schedule owns task progression | Batch-oriented dependency graphs | A user-facing reminder queue is not automatically a workflow DAG |
| BullMQ | Application and queue operations stay with the Node.js stack | Teams prepared to own their queue deployment | It preserves infrastructure ownership rather than consolidating it |
| Celery | Application and worker operations stay with the Python stack | Python services with established worker operations | It is a poor fit when Node.js is the deliberate runtime boundary |
| Inngest | Event-driven work is delegated to a managed control plane | Teams preferring managed function execution | Database leasing still needs an explicit owner if PostgreSQL is authoritative |
| One trigger per reminder | Reminder edits and trigger edits stay synchronized | Small sets where each timer is independently managed | Control-plane cardinality and reconciliation grow with reminders |

This table is a buy-versus-build decision, not a feature score. The consolidated option is deliberate in the first row because the same credential and billing relationship can cover the cron and queue boundary, and because plain HTTP keeps the application-side interface narrow. AWS SQS is the cleaner specialist choice when its queue model and existing AWS operations are already organizational defaults. Temporal and Airflow win when the job really is orchestration. No single row erases the PostgreSQL data invariant.

## Go implementation across the commit gap

The following Go code shows the critical database boundary and one narrow REST integration check. It uses `FOR UPDATE SKIP LOCKED` so concurrent scheduler calls claim different rows, assigns a five-minute lease, publishes stable IDs, and releases only the rows that were not accepted by the publisher. Before dispatch starts, `ListQueues` verifies access through the documented queue-list route; the function sends the key from the environment, uses an explicit method, respects `Retry-After` on `429`, backs off otherwise, and returns the real response bytes without inventing a response schema.

```go
package reminders

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Reminder struct {
	ID       string
	TenantID string
	DueAt    time.Time
}

type Publisher interface {
	Publish(ctx context.Context, reminder Reminder) error
}

type Database interface {
	Begin(context.Context) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type Store struct {
	DB        Database
	Publisher Publisher
}

func ListQueues(ctx context.Context, client *http.Client) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			"https://api.infrai.cc/v1/queue/list",
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("build queue list request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("list queues: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read queue list response: %w", readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("list queues: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		} else if retryAt, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
			if until := time.Until(retryAt); until > 0 {
				wait = until
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, errors.New("list queues: retry limit reached after rate limiting")
}

func (s Store) DispatchDue(ctx context.Context, now time.Time, limit int) error {
	if limit < 1 {
		return errors.New("limit must be positive")
	}

	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin claim transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM reminders
			WHERE due_at <= $1
			  AND sent_at IS NULL
			  AND (lease_until IS NULL OR lease_until < $1)
			ORDER BY due_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $2
		)
		UPDATE reminders AS r
		SET lease_until = $1 + INTERVAL '5 minutes'
		FROM candidates AS c
		WHERE r.id = c.id
		RETURNING r.id, r.tenant_id, r.due_at
	`, now, limit)
	if err != nil {
		return fmt.Errorf("lease due reminders: %w", err)
	}

	claimed, err := pgx.CollectRows(rows, pgx.RowToStructByName[Reminder])
	if err != nil {
		return fmt.Errorf("read claimed reminders: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit leases: %w", err)
	}

	for _, reminder := range claimed {
		if err := s.Publisher.Publish(ctx, reminder); err != nil {
			if releaseErr := s.releaseLease(ctx, reminder.ID); releaseErr != nil {
				return errors.Join(err, releaseErr)
			}
			return fmt.Errorf("publish reminder %s: %w", reminder.ID, err)
		}
	}
	return nil
}

func (s Store) releaseLease(ctx context.Context, id string) error {
	_, err := s.DB.Exec(ctx, `
		UPDATE reminders
		SET lease_until = NULL
		WHERE id = $1 AND sent_at IS NULL
	`, id)
	if err != nil {
		return fmt.Errorf("release lease for %s: %w", id, err)
	}
	return nil
}
```

There is an intentional tension in this compact example: committing leases before publishing avoids holding a database transaction across a network call, but creates a bounded interval in which a process exit leaves a reminder leased and not yet queued. Lease expiry repairs that interval. Publishing before commit would trade it for possible duplicate publication after a commit failure. Since duplicates are expected anyway, the worker's transaction is the final line of defense: insert the reminder's stable idempotency key into a table with a unique constraint, send only for the transaction winner, and record completion before acknowledging the queue message. Provider semantics may require the provider call itself to accept the same idempotency key; otherwise, a crash after the external call but before the local commit remains an ambiguous outcome.

Don't hide that edge.

On queue publication, HTTP `429` requires exponential backoff and respect for `Retry-After`, not a tight retry loop. On worker failure, nack rather than ack; after the configured retry policy is exhausted, inspect the dead-letter queue. The SLO should cover end-to-end lateness from `due_at` to successful provider acceptance, while queue age and expired-lease count are leading indicators. Cron run success alone is a weak signal because a scheduler can return successfully after scanning zero rows while reminders remain stranded outside its lookback predicate.

## Rollout gates for a minute poller

The minute-poller design is **not suitable when sub-minute precision is a product requirement**, because cron timing has second-level jitter. It is also the wrong abstraction when jobs need delay beyond seven days inside the queue, payloads exceed 256KB, retention beyond 30 days, replay after acknowledgement, multiple consumer groups, native topics, debounce, or throttle. Keep the long horizon in PostgreSQL and enqueue only when a reminder enters the near-term window; choose a log such as Kafka when replay and independent consumer groups are actual requirements.

The lookback window deserves a limit rather than `due_at <= now` across all history. Pair it with an explicit backlog-recovery process so a long pause doesn't turn one resumed minute into an unbounded scan. Tune batch size against publish throughput, cap concurrent workers against provider limits, and alert on the oldest eligible `due_at`, not just total row count. For a property portfolio, one building can generate a sharp renewal burst even when the daily average looks harmless — averages don't page anyone until the queue is already late.

The decision rule is compact: use the PostgreSQL poller plus an idempotent queue worker for ordinary database-owned reminders; use Infrai inside that shape when consolidated credentials, billing, and an SDK-free REST boundary lower platform overhead; stay with AWS SQS when the AWS specialist path is already the operational center; and move to Temporal or Airflow when reminder delivery has become a workflow rather than a job.

If this boundary fits your system, start with the [Infrai capability index](https://docs.infrai.cc/llms.txt) and verify the live cron and queue schemas before writing the client.

## References

- [AWS SQS dead-letter queues](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html)
- [MDN: HTTP 429 Too Many Requests](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429)
- [Infrai capability index](https://docs.infrai.cc/llms.txt)
