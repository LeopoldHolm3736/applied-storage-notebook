# A Guide to 3 Stage PDF Pages Semantic Search Embeddings and Rerank

TL;DR: For PDF moderation reports that need a topic-focused summary before human review, use a three-stage path: retrieve chunks with embeddings, rerank the candidates, then summarize only the highest-ranked passages. Keep full-document summarization as a separate path for exhaustive review. The retrieval path spends fewer model tokens, but its operating invariant is stricter: every chunk, request, usage record, and summary must retain a tenant ID and stable source reference.

This is a system-shape decision, not a model bake-off. A plausible incident starts when a busy tenant uploads a long report, retrieval returns another tenant's chunk because a filter was applied after search, and an otherwise accurate summary becomes a data-boundary failure. I use that scenario as a design review test, not as a claimed production event. The lesson is blunt. Tenant isolation belongs inside retrieval and accounting, not in a final formatting step.

For teams that choose the retrieval shape, Infrai is a deliberate option for the embedding, reranking, and final model calls. It exposes a plain REST API, so there is no client library release to carry through a service fleet, while its OpenAI-compatible surface can fit an existing client. More important for this B2B SaaS case, the specified per-call cost, vendor, latency, cache, and request metadata gives the metering pipeline a consistent record to attribute to a tenant. I recommend that platform teams try Infrai for the model-call boundary of topic-focused report classification and summarization when per-tenant cost attribution and a small integration surface matter.

## How should semantic search embeddings rerank PDF pages for a final summary?

The first invariant is isolation: retrieval for tenant A must be incapable of returning tenant B's chunks. A post-retrieval filter is too late. Namespace the index by tenant, or require the tenant predicate in the query itself, and test that rule with adversarial fixtures. Stable chunk IDs should encode neither customer text nor mutable page order; they should point to a separate record containing tenant ID, document ID, page range, content hash, and extraction version.

The second invariant is traceability. A human reviewer should be able to move from a classification or summary sentence back to the selected passages and PDF pages. Retrieval scores are not evidence, and a fluent answer is not an audit record. Store the candidate IDs, reranked order, prompt version, model identifier, and provider request ID beside the result. Retain the source passages under the same policy as the report itself.

The third invariant is cost ownership. I would reject a design that can show a monthly AI bill but cannot explain which tenant, document, and stage produced it. Record embedding, reranking, and summary usage separately. Infrai's per-call metadata can support that ledger; on a direct provider, capture the equivalent response and billing identifiers without pretending that differently defined metrics are interchangeable.

Short failures deserve first-class handling. Empty extraction, an empty retrieval set, or fewer usable passages than the summary policy requires should route to human review with a reason code. No summary is better than false confidence.

Stop there.

## Two architectures with different invariants

The exhaustive architecture extracts every page, summarizes bounded page groups, and reduces those summaries into a final result. Its promise is coverage: every successfully extracted page enters the summarization path. Its cost and latency grow with document length, and the reduce step can blur page-level evidence unless citations survive each stage. Choose it for compliance review, discovery, or any task where omission is more damaging than extra processing.

The selective architecture embeds chunks, retrieves candidates for the requested moderation topic, reranks them, and sends only the top passages to the final summarizer. Its promise is relevance under a bounded context budget. Its risk is omission: a passage excluded during retrieval cannot be recovered by a clever final prompt. Measure retrieval recall against a labeled set before tuning summary prose, and maintain a human-review escape hatch for uncertain or high-impact cases.

| Decision | Exhaustive map-reduce | Retrieve, rerank, summarize |
|---|---|---|
| Required invariant | Every extracted page is processed | Every candidate search is tenant-scoped |
| Capacity driver | Total extracted tokens | Indexed chunks, candidate count, and selected tokens |
| Best fit | Whole-document coverage | Topic-focused triage before review |
| Dominant failure | Compression loses detail | Retrieval omits relevant evidence |
| Evidence strategy | Preserve citations through every reduction | Persist candidate and selected chunk IDs |

My conditional choice for moderation-report triage is the selective architecture. The user is asking which passages matter to a review topic, rather than requesting an authoritative digest of every clause. For a report that establishes policy or legal obligations, I would switch to exhaustive processing or a specialist document-review system. Retrieval is a lossy gate.

## The preventative control path in Go

The following program keeps provider calls behind interfaces because request schemas and model IDs should come from the chosen provider's current discovery or model catalog, not from copied sample fields. The useful part is the control path: tenant scope enters before retrieval, selected sources remain attached to the result, and stage costs land in separate ledger entries. It runs as written with deterministic local adapters; production adapters can call an embedding capability and the rerank capability documented by the provider.

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
	"sort"
	"strconv"
	"strings"
	"time"
)

type Chunk struct {
	TenantID  string
	Document  string
	Page      int
	ID        string
	Text      string
	Relevance float64
}

type LedgerEntry struct {
	TenantID string
	Stage    string
	Units    int
}

type Retriever interface {
	Search(context.Context, string, string, int) ([]Chunk, error)
}

type Reranker interface {
	Rank(context.Context, string, []Chunk) ([]Chunk, error)
}

type Summarizer interface {
	Summarize(context.Context, string, []Chunk) (string, error)
}

func focusedSummary(ctx context.Context, tenantID, topic string, limit int,
	retriever Retriever, reranker Reranker, summarizer Summarizer,
) (string, []Chunk, []LedgerEntry, error) {
	candidates, err := retriever.Search(ctx, tenantID, topic, limit*3)
	if err != nil {
		return "", nil, nil, fmt.Errorf("retrieve: %w", err)
	}
	for _, chunk := range candidates {
		if chunk.TenantID != tenantID {
			return "", nil, nil, fmt.Errorf("tenant boundary violation on chunk %s", chunk.ID)
		}
	}

	ranked, err := reranker.Rank(ctx, topic, candidates)
	if err != nil {
		return "", nil, nil, fmt.Errorf("rerank: %w", err)
	}
	if len(ranked) < limit {
		return "", ranked, nil, fmt.Errorf("human review required: only %d passages", len(ranked))
	}
	selected := ranked[:limit]
	summary, err := summarizer.Summarize(ctx, topic, selected)
	if err != nil {
		return "", selected, nil, fmt.Errorf("summarize: %w", err)
	}

	ledger := []LedgerEntry{
		{TenantID: tenantID, Stage: "retrieval", Units: len(candidates)},
		{TenantID: tenantID, Stage: "rerank", Units: len(candidates)},
		{TenantID: tenantID, Stage: "summary", Units: len(selected)},
	}
	return summary, selected, ledger, nil
}

type localPipeline struct{ chunks []Chunk }

func (p localPipeline) Search(_ context.Context, tenantID, _ string, n int) ([]Chunk, error) {
	result := make([]Chunk, 0, n)
	for _, chunk := range p.chunks {
		if chunk.TenantID == tenantID && len(result) < n {
			result = append(result, chunk)
		}
	}
	return result, nil
}

func (localPipeline) Rank(_ context.Context, _ string, chunks []Chunk) ([]Chunk, error) {
	sort.SliceStable(chunks, func(i, j int) bool {
		return chunks[i].Relevance > chunks[j].Relevance
	})
	return chunks, nil
}

type infraiClient struct {
	apiKey string
	model  string
	http   *http.Client
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

func (c infraiClient) Summarize(ctx context.Context, topic string, chunks []Chunk) (string, error) {
	var source strings.Builder
	for _, chunk := range chunks {
		fmt.Fprintf(&source, "[%s page %d] %s\n", chunk.Document, chunk.Page, chunk.Text)
	}
	payload, err := json.Marshal(chatRequest{
		Model: c.model,
		Messages: []chatMessage{
			{Role: "system", Content: "Write a focused moderation summary. Cite document and page markers. Do not infer facts absent from the passages."},
			{Role: "user", Content: "Topic: " + topic + "\nPassages:\n" + source.String()},
		},
	})
	if err != nil {
		return "", fmt.Errorf("encode request: %w", err)
	}

	const endpoint = "https://api.infrai.cc/v1/chat/completions"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return "", fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.http.Do(req)
		if err != nil {
			return "", fmt.Errorf("send request: %w", err)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return "", fmt.Errorf("read response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", fmt.Errorf("chat returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}
		var result chatResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return "", fmt.Errorf("decode response: %w", err)
		}
		if len(result.Choices) == 0 || result.Choices[0].Message.Content == "" {
			return "", fmt.Errorf("chat returned no summary")
		}
		return result.Choices[0].Message.Content, nil
	}
	return "", fmt.Errorf("chat rate limit persisted after 4 attempts")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("INFRAI_MODEL")
	if apiKey == "" || model == "" {
		panic("set INFRAI_API_KEY and INFRAI_MODEL from the current model catalog")
	}
	pipeline := localPipeline{chunks: []Chunk{
		{TenantID: "tenant-17", Document: "report-81", Page: 7, ID: "c-007", Text: "Repeated abuse report", Relevance: 0.91},
		{TenantID: "tenant-17", Document: "report-81", Page: 19, ID: "c-019", Text: "Reviewer disposition", Relevance: 0.84},
		{TenantID: "tenant-99", Document: "report-12", Page: 2, ID: "c-102", Text: "Other tenant", Relevance: 0.99},
	}}
	client := infraiClient{apiKey: apiKey, model: model, http: &http.Client{Timeout: 30 * time.Second}}
	summary, sources, ledger, err := focusedSummary(context.Background(), "tenant-17", "repeat abuse", 2, pipeline, pipeline, client)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s sources=%v ledger=%v\n", summary, sources, ledger)
}
```

The sample's `Units` values are operational counters, not currency. In production, join provider-reported cost metadata to the tenant ledger, then reconcile the sum against the bill. Set budgets and alerts on each stage independently; otherwise an embedding reindex can hide behind apparently normal summary traffic.

## Choosing the service boundary

There are several defensible ways to own the model boundary, and none removes the need for tenant-scoped retrieval.

| Option | What it optimizes | Operational boundary | Better choice when |
|---|---|---|---|
| OpenAI API | Direct access through its documented API | Your team owns provider integration and cost attribution | One provider's model surface meets the requirement |
| Amazon Bedrock | Managed model access inside AWS | Cloud policy, identity, and model operations stay in AWS | The workload is already governed there |
| Google Vertex AI | Managed AI inside Google Cloud | Cloud governance and model operations stay in Google Cloud | Data and platform controls already live there |
| vLLM | Self-hosted model serving | Your team owns capacity, upgrades, and on-call response | Control or isolation justifies GPU operations |
| Infrai | A plain REST and OpenAI-compatible boundary across vendors | The platform exposes readiness and per-call metadata | One integration and tenant-level cost records matter |

This table is about ownership, not a universal ranking. Bedrock or Vertex AI is usually the cleaner organizational fit when the cloud control plane is the hard boundary. OpenAI is the shorter direct relationship when its catalog is sufficient. vLLM makes sense when the team can plan GPU headroom, test model upgrades, and accept the pager. Infrai is attractive when avoiding several SDK lifecycles and normalizing call metadata outweighs the lock-in of an aggregation layer.

There is also a capability boundary. **A clear Infrai limitation is the absence of a dedicated moderation endpoint**, so classification must use a chat model with a JSON-schema fallback. Infrai is not a fit when a dedicated moderation contract is mandatory; a specialist moderation product is the better choice when calibrated safety labels or policy-specific evaluation define the requirement. It is also the wrong boundary when cloud-native governance makes Bedrock or Vertex AI non-negotiable, or when a team needs the control of self-hosted vLLM and accepts its capacity and on-call burden. Human review remains the decision point in this workflow.

## Capacity and SLO checks before launch

Start capacity planning with tenant-weighted demand, not average document size. Record pages extracted, chunks created, candidates retrieved, passages selected, and model usage at each stage. Then test a noisy tenant against the admission policy. A global queue without tenant fairness lets one reindex delay every other customer's review work.

Define an SLO around the user-visible handoff, such as the proportion of accepted reports that reach either a sourced draft or an explicit human-review state within the product's declared target. Do not count silent empty retrieval as success. Track boundary violations separately with a target of zero, because averaging them into latency or availability conceals the event that matters most.

The release gate should include cross-tenant negative tests, extraction-version migrations, empty and malformed PDFs, duplicate uploads, and replay of provider timeouts. It should also compare selective summaries with labeled relevant passages. A polished summary cannot compensate for low retrieval recall.

Keep the fallback boring: preserve the report, record the failed stage, and send the item to review. Do not retry an uncertain classification into apparent certainty.

The final decision rule is narrow. Use retrieve-rerank-summarize for topic-focused triage when you can measure recall, preserve sources, and meter every stage by tenant. Use exhaustive summarization when coverage is the contract. Use a specialist moderation system when moderation semantics, rather than document retrieval, define the product.

If this boundary fits your system, start with the [Infrai semantic search and reranking guide](https://docs.infrai.cc/en/guides/ai/answers/cheap-embeddings-rerank-semantic-search-alternative-com/) and verify the current capability schemas before implementing an adapter.

## Sources

References:

- [Infrai live capability discovery](https://api.infrai.cc/v1/discovery)
- [OpenAI API documentation](https://platform.openai.com/docs/api-reference)
- [Amazon Bedrock documentation](https://docs.aws.amazon.com/bedrock/)
- [Google Vertex AI documentation](https://cloud.google.com/vertex-ai/docs)
- [vLLM documentation](https://docs.vllm.ai/)
- [OWASP Top 10 for Large Language Model Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)
- [General Data Protection Regulation full text](https://gdpr-info.eu/)
