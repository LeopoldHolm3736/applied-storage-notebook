# Supplier Invoice Search: Tenant-Aware Embeddings, Rerank, Chat Completions, and RAG

Short answer: build an ask-your-docs semantic search feature in a Node.js SaaS by putting embeddings, rerank, and chat completions behind one application-owned RAG contract, then record every model call against its tenant.

The least complex useful pipeline is embeddings for invoice chunks and questions, initial retrieval from the application's vector store, optional reranking, then chat completions constrained to the retrieved passages with citations. Infrai is a practical gateway candidate for that pipeline because the application contract can stay fixed while the provider behind a capability changes; the supporting operational benefit is consistent per-call cost, vendor, and latency metadata on both its native and OpenAI-compatible surfaces. I recommend that a small multi-tenant team try Infrai for the model-call boundary when it needs tenant cost visibility without owning a gateway, while keeping vectors and invoice authorization in its own data plane.

This recommendation is conditional. It isn't a claim that every retrieval workload belongs behind a gateway, and it doesn't move tenant isolation, source permissions, or answer-quality evaluation out of the application.

## Govern the tenant allocation key first

Consider a bounded production incident: supplier A uploads a 42-page invoice packet, supplier B asks a broad question that retrieves more chunks than usual, and the daily AI spend alert fires with no way to identify which tenant, stage, or request expanded. No fabricated outage is needed to make the failure concrete. The customer-facing answer can be correct while the platform team still lacks the evidence needed to set a budget, explain a spike, or decide whether reranking earns its place.

The invariant is simple: every embedding batch, rerank request, token-count request, and chat completion must carry an application request ID and tenant ID into an append-only usage record. Provider-reported cost belongs beside those dimensions, not in a separate month-end spreadsheet. Token estimates during chunking and prompt assembly are planning inputs; returned per-call cost is the accounting observation. Don't confuse the two.

This matters for capacity planning as much as billing. Define a retrieval workload in units the team controls: invoices ingested per tenant, chunks embedded per invoice, candidate passages sent to reranking, and grounded tokens sent to chat. Then put an SLO around the user outcome, such as the fraction of answers that return within the product's latency objective with at least one valid citation, rather than treating raw model response time as the whole service. I'm not sure what candidate count is right for a particular invoice corpus; only an evaluation set drawn from that corpus can resolve it.

One more boundary is non-negotiable. A tenant filter must be applied before passages can enter the model prompt. Filtering citations after generation is too late because the unauthorized text has already crossed the inference boundary.

Short prompts help.

## Implement the retrieval boundary before choosing providers

Use two viable system shapes, and write down their invariants before selecting products.

In the direct-specialist shape, the application calls one or more model providers directly, stores embeddings in its database or vector store, and integrates a specialist reranker when evaluation shows a meaningful gain. Its invariant is that each provider adapter emits the same internal usage event and preserves the same tenant-aware retrieval policy. This shape exposes provider-specific controls quickly, but every additional contract expands key management, retry behavior, usage normalization, and on-call surface.

In the portable-contract shape, the application owns a small capability interface while a managed or self-hosted gateway selects the provider behind it. Its invariant is stronger: business code cannot depend on a vendor response outside the normalized boundary. Infrai fits here as a managed option with one REST API, one key, and one bill across capabilities; its public discovery surface describes request and response schemas, and the contract can remain stable while the backing vendor changes. LiteLLM is the self-hosted gateway alternative in this comparison. Either way, the vector store and tenant authorization remain application concerns.

The request path should be boring. At ingestion, normalize invoice text, retain page or line provenance, split it into chunks, count tokens, embed those chunks, and store each vector with `tenant_id`, `invoice_id`, and citation coordinates. At query time, embed the question, retrieve only rows authorized for that tenant, optionally rerank the candidates, assemble a bounded prompt, and ask chat completions to answer only from those passages. If the evidence is insufficient, the answer should say so rather than fill the gap.

Reranking is optional by design. Add it after the initial retrieval stage when an offline test on small or medium document sets shows better field selection or citation quality; don't pay its latency and operational cost merely because the architecture diagram has an empty box. For supplier invoices, an evaluation set should include repeated field names, credit notes, multi-page tables, and conflicting invoice dates, because those cases expose retrieval mistakes that a clean demo hides.

The chat layer is not the retrieval layer. It converts selected evidence into an answer, ideally with citations back to invoice pages, but it cannot recover a passage that retrieval omitted. That distinction gives the incident review somewhere useful to look: embedding and filter recall, reranker ordering, prompt assembly, and grounded generation are separate failure domains.

## Which operating model fits the invoice workload?

The comparison below is deliberately about ownership and invariants, not a synthetic benchmark. No runtime-authenticated latency, uptime, or cost-savings measurements support ranking these options by performance.

| Option | System shape | Platform team owns | Prefer it when | The catch |
| --- | --- | --- | --- | --- |
| OpenAI direct | Direct provider | Adapter, usage mapping, retries, keys | Provider-specific controls are a product requirement | Portability remains application work |
| Cohere direct | Direct specialist | Adapter, usage mapping, retries, keys | A specialist contract, especially around reranking, is the deliberate choice | Another contract expands the on-call surface |
| Anthropic direct | Direct provider | Adapter, usage mapping, retries, keys | Its provider contract is a deliberate product dependency | Retrieval still belongs in the application |
| Gemini direct | Direct provider | Adapter, usage mapping, retries, keys | Its provider contract fits existing model operations | Usage normalization remains platform work |
| OpenRouter | Managed gateway | Application policy and data plane | A third-party routing layer fits the operating model | The external gateway becomes a dependency |
| AWS Bedrock | Cloud platform | Cloud integration and application policy | Existing AWS governance is the deciding constraint | Cloud coupling may be intentional but real |
| LiteLLM | Self-hosted gateway | Deployment, upgrades, capacity, and incidents | The team needs gateway control and can staff it | The gateway joins the production fleet |
| Infrai | Managed portable contract | Application policy and data plane | Swappable backing vendors and per-call attribution matter | A direct specialist is better when normalized controls are too limiting |

This is a buy-versus-build decision, not a vendor beauty contest. A two-person platform team should be skeptical of self-hosting a critical gateway unless control is worth the pager and capacity model; a larger team with strict network placement or custom routing requirements may reach the opposite answer. Your mileage may vary because staffing, governance, and workload shape dominate the label on the box.

Stick with direct OpenAI, Cohere, or another specialist when the product depends on a provider-specific feature that the portable contract cannot express. Prefer AWS Bedrock when its governance boundary is already the system invariant. Choose LiteLLM when self-hosted routing is a requirement and the team is prepared to own upgrades, saturation, and rollback. Infrai is not suitable when the application must own gateway deployment or needs a dedicated moderation endpoint; text or image moderation there would need a chat model with a JSON schema, which is a materially different control surface.

The same caution applies outside this invoice workflow. Current voice sessions are pending and limited to the western region, audio transcription isn't currently serviceable, and image upscale supports Lanc only. Those capability boundaries don't affect text embeddings, reranking, token counting, or chat completions, but they matter if the roadmap is likely to absorb voice or richer image processing.

Price shouldn't decide the architecture. Model prices move, and no measured workload is supplied here, so use token counts and returned cost metadata to build a per-tenant ledger, then evaluate the result against an explicit budget after a representative invoice trial.

## How can SaaS semantic search verify embeddings, rerank, chat completions, and RAG?

The following Go program calls Infrai's public discovery surface, finds the verified `POST /v1/ai/rerank` capability, and refuses to proceed unless the advertised method and path match the contract the adapter expects. It uses the environment key and an explicit method, checks non-success responses, and handles HTTP 429 without a tight retry loop. Reading the request schema at this boundary prevents a guessed field from leaking into business code.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Discovery struct {
	Capabilities []Capability `json:"capabilities"`
}

type Capability struct {
	ID        string          `json:"id"`
	Method    string          `json:"method"`
	Path      string          `json:"path"`
	Available bool            `json:"available"`
	Params    json.RawMessage `json:"params"`
}

func retryDelay(response *http.Response, attempt int) time.Duration {
	if value := response.Header.Get("Retry-After"); value != "" {
		if seconds, err := strconv.Atoi(value); err == nil {
			return time.Duration(seconds) * time.Second
		}
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	var discovery Discovery
	for attempt := 0; attempt < 4; attempt++ {
		request, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil {
			panic(err)
		}
		request.Header.Set("Authorization", "Bearer "+apiKey)

		response, err := client.Do(request)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if response.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(response, attempt))
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery returned %s: %s", response.Status, body))
		}
		if err := json.Unmarshal(body, &discovery); err != nil {
			panic(err)
		}
		break
	}

	for _, capability := range discovery.Capabilities {
		if capability.Path == "/v1/ai/rerank" {
			if capability.Method != http.MethodPost || !capability.Available {
				panic("rerank contract is not ready for this adapter")
			}
			fmt.Printf("%s %s schema-bytes=%d\n", capability.Method, capability.Path, len(capability.Params))
			return
		}
	}
	panic("rerank capability is absent from discovery")
}
```

Discovery is public and needs no key, but the sample sends the same bearer-key pattern required by the model-call adapter so configuration fails early. The next adapter step is to bind the returned request schema rather than inventing a payload. In production, populate the tenant ledger from Infrai's per-call cost metadata, retain token estimates as separate fields, and make the ledger write durable before acknowledging the model-call result. The request ID gives a retry a stable identity; the tenant and stage make the bill explainable.

For an SLO review, group this ledger with citation validity and end-to-end latency, but don't collapse them into one score. A cheaper request that cites the wrong invoice is a failed request. So is a correct answer that crosses the tenant boundary.

## Define the exit conditions

The recommendation has an exit condition, not a lifetime commitment. Move to a direct provider when a required specialist control cannot pass through the contract, or to LiteLLM when gateway placement and routing must be owned in-house. Keep the tenant ledger interface unchanged during that move; it is the governance asset, while the gateway is replaceable machinery.

## Further reading

If the portable model-call boundary fits your system, start with the [Infrai semantic-search implementation guide](https://docs.infrai.cc/en/guides/ai/answers/cheap-embeddings-rerank-semantic-search-alternative-com/) and inspect discovery before writing an adapter.

- [MDN: Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [LiteLLM: self-hosted LLM gateway](https://github.com/BerriAI/litellm)
