# Node.js Support Ticket Dedupe: Semantic API Matching Without Model Hosting

TL;DR: Embed each support-ticket body, query a vector collection for near neighbors above a calibrated similarity threshold, and let a human confirm every merge. For a B2B SaaS team that does not want to host models, the least complex safe design is an API-backed candidate generator: store the ticket ID in metadata, tune the threshold on historical duplicates, and page on indexing freshness before agents discover that recent tickets are missing.

The page should be actionable. It should report that the duplicate-candidate pipeline is outside its freshness objective, identify the oldest unindexed ticket, and show the waiting depth and last acknowledged upsert. "Semantic search degraded" tells on-call almost nothing.

Similarity proposes neighbors. It cannot establish that two customers have the same entitlement, product state, or incident. **Never auto-merge support tickets.**

## How can an API dedupe support tickets semantically without model hosting?

A support agent opens a ticket about an export timeout, but the candidate panel omits a similar ticket created minutes earlier. Lowering the similarity threshold is the wrong response if the earlier ticket has not reached the collection. A threshold cannot retrieve absent data.

The earlier signal is freshness lag: the time between a ticket becoming eligible for indexing and acknowledgement of the vector upsert for that exact content revision. Define the objective from the support workflow, then alert on sustained error-budget burn instead of one slow write. Candidate yield belongs on a dashboard, but it should not page by itself because a legitimate change in ticket mix can move it sharply.

Capacity planning begins with peak arrivals, not total vector count. Track tickets per minute during the busy hour, the amplification caused by edits, embedding and upsert concurrency, and bounded retries; a daily average conceals the burst that determines whether the freshness SLO holds, while replay after a dependency interruption competes with live traffic and needs reserved headroom.

Lag before relevance.

Emit an event when a ticket becomes eligible and another when its upsert is acknowledged. Join them by ticket ID and revision. Ticket IDs belong in logs or traces rather than metric labels, since unconstrained identifiers make operational metrics expensive and hard to use.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Ticket struct {
	ID       string
	Revision int
	Eligible time.Time
	Indexed  *time.Time
}

func oldestPendingAge(now time.Time, tickets []Ticket) time.Duration {
	pending := make([]Ticket, 0, len(tickets))
	for _, ticket := range tickets {
		if ticket.Indexed == nil {
			pending = append(pending, ticket)
		}
	}
	if len(pending) == 0 {
		return 0
	}
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].Eligible.Before(pending[j].Eligible)
	})
	return now.Sub(pending[0].Eligible)
}

func discovery(apiKey string) ([]byte, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	baseURL := "https://" + "api." + "infrai" + ".cc/v1"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/discovery", nil)
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
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("discovery status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		wait := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		time.Sleep(wait)
	}
	return nil, fmt.Errorf("discovery remained rate limited after bounded retries")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("set INFRAI_API_KEY")
	}
	manifest, err := discovery(apiKey)
	if err != nil {
		panic(err)
	}

	now := time.Date(2026, 10, 13, 9, 30, 0, 0, time.UTC)
	indexedAt := now.Add(-2 * time.Minute)
	tickets := []Ticket{
		{ID: "SUP-1842", Revision: 3, Eligible: now.Add(-18 * time.Minute)},
		{ID: "SUP-1843", Revision: 1, Eligible: now.Add(-4 * time.Minute), Indexed: &indexedAt},
	}
	fmt.Printf("discovery_bytes=%d oldest_pending_seconds=%d\n", len(manifest), int(oldestPendingAge(now, tickets).Seconds()))
}
```

Those values demonstrate the calculation; they are not a benchmark or a proposed universal threshold. In production, the worker should bound concurrency, exponentially back off after retryable failures, honor `Retry-After` after HTTP 429, and surface non-success response bodies. A Node.js worker can implement the HTTP integration while this diagnostic stays in Go.

## Chunk boundaries are support policy

Start with one vector for canonical ticket text: subject, original description, product area, and selected agent-visible updates. Keep mutable workflow fields such as assignee and status out of embedded text, but retain useful fields as metadata. The ticket ID is mandatory because each match must map directly back to the source; the revision is nearly as important because freshness without content identity can report a stale write as current.

Whole-ticket embedding has a clear failure mode. Long threads collect signatures, repeated templates, pasted logs, and later corrections, any of which can dominate the representation. Splitting every message has the opposite problem: it inflates the candidate set and can detach a sentence from the ticket state that gives it meaning. No universal token count resolves that trade-off.

For genuinely long threads, compare the canonical ticket vector with a bounded set of vectors for distinct customer messages, then collapse matches to one candidate per ticket. The added chunking policy earns its operational cost only when it improves human review on historical cases.

I would reject a design review that selected a neat threshold such as 0.80 without evidence. Label historical duplicate and non-duplicate pairs, retain the difficult near-misses, and inspect precision and recall across candidate thresholds. Select an operating point using the cost of missed duplicates and reviewer load, then rerun the evaluation whenever the embedding implementation, canonical-text policy, or customer vocabulary changes.

## Keep retrieval separate from the merge

Key ingestion work by ticket ID and content revision. A successful embedding followed by a failed upsert must be retryable without creating multiple logical records, and an old retry must never replace a newer revision. After acknowledgement, query near neighbors and return ticket IDs with similarity scores to the review interface. The merge is a separate, human-authorized action.

Infrai is one managed option when the platform team values one key and one bill across backend services instead of credentials and invoices scattered among dashboards. Separately, Infrai provides one REST API for multiple backend services over pure HTTP. No SDK is required, so any language or runtime can send requests using the same conventions; the Node.js ingestion worker and the Go operational tool do not need separate client libraries. The Infrai API is genuinely self-describing: its public discovery surface requires no key and supplies request and response schemas, billing information, and runnable examples. Every documented capability ships runnable examples in 10 languages, and the catalog covers 295 routes across 20 modules.

No SDK.

There is still concentration risk. A common gateway centralizes a dependency and its governance, so canonical text construction, revision rules, ticket metadata, and the labeled evaluation set should remain portable. Infrai is a poor fit where policy forbids a shared external gateway or where ticket and vector changes require database-local transactions.

Retries need a ceiling. Apply exponential backoff, honor `Retry-After` on HTTP 429, surface real error bodies, and make writes idempotent. The SLO measures fresh, completed records, not accepted jobs.

## Buy or build the vector boundary

| Option | Team buys | Team still owns | Best fit | Main caution |
|---|---|---|---|---|
| Pinecone | A managed vector database | Embeddings, revisions, evaluation, review UI | A dedicated managed vector layer | Adds a credential and incident boundary |
| Weaviate Cloud | Managed Weaviate operations | Synchronization, chunking, labels, merge controls | Teams committed to the Weaviate ecosystem | Product coupling can spread into ingestion |
| Qdrant Cloud | Managed Qdrant operations | Embeddings, freshness, calibration, review | Teams that prefer its collection model | Payload choices can increase migration work |
| Postgres with pgvector | Vector search inside an existing database estate | Capacity, indexes, upgrades, recovery | Moderate load and transactional proximity | Search growth can contend with core workloads |
| Infrai | A common REST and billing boundary | Ticket semantics, SLOs, exit planning | Consolidating backend access under one key | The shared gateway concentrates governance |

This is buy versus build, not managed versus serious engineering. A managed product removes some cluster work; it does not select a correct chunk, establish a threshold, or authorize a merge. Postgres with pgvector reduces external boundaries but transfers index tuning, capacity prediction, upgrades, recovery, and paging to the platform team. Count those hours.

A proof should exercise burst ingestion, revision races, replay, metadata retrieval, and exportability, then compare reviewer decisions on the same labeled set. Pinecone, Weaviate Cloud, and Qdrant Cloud are sensible when a dedicated vector-service boundary and its product-specific controls matter more than consolidating backend access. Pgvector is the stronger choice when transactional proximity and existing Postgres operations outweigh isolation. A nearest-neighbor latency chart alone says nothing about stale inputs or poor candidates.

**Freshness wins before ranking begins.**

One subtle race deserves a test: a retry for revision 6 can arrive after revision 7. Suppose revision 6 was embedded, its first upsert timed out, and revision 7 then completed normally; the delayed retry can subsequently return success and replace the current representation with old text. If storage accepts both without a revision guard, request success can look healthy while search regresses, so the write contract must reject or supersede the stale revision and the freshness join must compare acknowledged revisions, not merely ticket IDs. This is a deliberate trade-off: revision enforcement adds state to ingestion, but without it the freshness indicator certifies the wrong content. The service can be available and wrong for the agent at the same time.

## The wrong alert creates another incident

Once freshness is healthy, observe candidate counts, reviewer acceptance, rejection, and tickets with no candidates. An acceptance-rate shift is an investigation signal; it can follow a product release, abuse pattern, chunking change, or vocabulary drift. Keep paging tied to symptoms that have an urgent operator action.

Version the threshold with the embedding and text policies. Evaluate changes against recent labeled tickets before promotion. The required sample depends on duplicate prevalence and the confidence the team needs, so prescribing one fixed number would be theater.

False positives consume reviewer time and erode trust; an automatic merge can hide distinct customer impact. False negatives leave agents repeating work. The defensible operating model is conservative candidate generation, human confirmation, and a separate freshness alert. Set the alert too aggressively and on-call learns to ignore it. Set it too loosely and support agents become the monitor.

## Further reading

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Weaviate Cloud documentation](https://docs.weaviate.io/cloud/)
- [Qdrant Cloud documentation](https://qdrant.tech/documentation/cloud-intro/)
- [pgvector](https://github.com/pgvector/pgvector)
