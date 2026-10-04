# How to Moderate Text Prompts for Image Generation Without a Dedicated Endpoint

An image pipeline is only as enforceable as the decision at its gate: free-form model prose is not a moderation result. **Short answer:** when a platform has no dedicated moderation endpoint, classify the complete user-controlled prompt with a chat model, require a strict JSON-schema response of `allow`, `review`, or `block`, validate it locally, and invoke image generation only for `allow`.

That choice adds a model call and another failure domain. It is still the defensible choice when structured-output correctness is the primary requirement, because malformed output can fail closed instead of quietly becoming permission. The raw prompt and every user-editable style field must enter the same classification input; otherwise a clean subject paired with a hostile style suffix walks around the gate.

No valid JSON, no image.

## The prelaunch failure budget

I start the prelaunch exercise with a narrow failure budget: no unclassified request may reach image generation, and no parser error may default to approval. This is an exercise, not a claim about a production incident. One test sends an ordinary product-photo prompt. A second puts the disallowed material in a user-editable `style` value. A third makes the classifier return syntactically invalid JSON. The invariant is blunt: **absence of a valid `allow` decision means no image call**.

The trap is easy to miss during a happy-path review. Teams often moderate `prompt` and concatenate `style`, negative prompts, or template additions afterward. At that point the reviewed string is not the generated string. Assemble one canonical moderation subject first, preserving field boundaries, and pass exactly those user-controlled components to the classifier.

This gate also needs an SLO of its own. Track the share of requests that produce schema-valid decisions, the rate sent to human review, and the fraction that fail closed because the classifier or network is unavailable. Do not call a fallback approval a reliability improvement. It transfers an availability miss into a safety miss.

That distinction matters.

## How can you moderate text prompts for image generation without an endpoint?

The following program is deliberately limited to the preventative path: it calls the OpenAI-compatible chat surface, requests schema-constrained JSON, validates the response again in Go, and prints the only enforceable decision. Set `INFRAI_BASE_URL`, `INFRAI_API_KEY`, and `INFRAI_CHAT_MODEL`; model identifiers should come from the live model catalog rather than being embedded in source. Infrai is relevant here as one option because the interface is plain REST, so there is no client SDK version to maintain, and the same API key can cover the later image-generation call. Keeping the host in deployment configuration also prevents an unlinked engineering note from becoming an undocumented service-discovery mechanism.

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
	"strings"
	"time"
)

type Decision struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	decision, err := moderate(ctx, os.Getenv("INFRAI_BASE_URL"), os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_CHAT_MODEL"),
		"Studio photograph of a red running shoe",
		"high-contrast catalog image on a white background",
	)
	if err != nil {
		panic(err) // Fail closed: do not call image generation.
	}
	fmt.Printf("decision=%s reason=%s\n", decision.Action, decision.Reason)
	if decision.Action != "allow" {
		return
	}
	// The image-generation request belongs here, after the allow check.
}

func moderate(ctx context.Context, baseURL, apiKey, model, prompt, style string) (Decision, error) {
	if baseURL == "" || apiKey == "" || model == "" {
		return Decision{}, errors.New("INFRAI_BASE_URL, INFRAI_API_KEY, and INFRAI_CHAT_MODEL are required")
	}

	requestBody := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": "Classify image-generation input. Return only the requested JSON."},
			{"role": "user", "content": "PROMPT:\n" + prompt + "\nSTYLE:\n" + style},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "prompt_moderation",
				"strict": true,
				"schema": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"properties": map[string]any{
						"action": map[string]any{"type": "string", "enum": []string{"allow", "review", "block"}},
						"reason": map[string]any{"type": "string"},
					},
					"required": []string{"action", "reason"},
				},
			},
		},
	}
	body, err := json.Marshal(requestBody)
	if err != nil {
		return Decision{}, err
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			strings.TrimRight(baseURL, "/")+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return Decision{}, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return Decision{}, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return Decision{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				wait = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(wait):
				continue
			case <-ctx.Done():
				return Decision{}, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return Decision{}, fmt.Errorf("classifier returned %s: %s", resp.Status, strings.TrimSpace(string(responseBody)))
		}

		var result chatResponse
		if err := json.Unmarshal(responseBody, &result); err != nil || len(result.Choices) == 0 {
			return Decision{}, errors.New("classifier response did not contain a choice")
		}
		var decision Decision
		if err := json.Unmarshal([]byte(result.Choices[0].Message.Content), &decision); err != nil {
			return Decision{}, fmt.Errorf("invalid decision JSON: %w", err)
		}
		if decision.Action != "allow" && decision.Action != "review" && decision.Action != "block" {
			return Decision{}, fmt.Errorf("invalid decision action %q", decision.Action)
		}
		return decision, nil
	}
	return Decision{}, errors.New("classifier remained rate limited")
}
```

The final comment marks a security boundary, not omitted permission logic. A real service should place its image call behind a function that accepts a validated `Decision`, rather than expecting every caller to remember an `if` statement. Keep the route count small, the authorization header scoped to the API host, and the error body visible to operators without echoing sensitive prompts into general-purpose logs.

## Buy or build the moderation layer

There are at least four credible shapes for this system. Their interfaces and policy surfaces differ, so a migration is more than changing a hostname.

| Option | Integration shape | Structured-decision fit | Operational trade-off |
|---|---|---|---|
| OpenAI Moderation | Dedicated moderation API | Typed category results reduce prompt-engineering dependence | Adds a provider-specific policy taxonomy and service dependency |
| Azure AI Content Safety | Dedicated text and image safety service | Severity-oriented results support explicit application thresholds | Requires threshold ownership and Azure resource operations |
| Google Cloud Vision SafeSearch | Image-oriented detection in Cloud Vision | Useful for assessing generated image output by likelihood category | Does not replace pre-generation text classification |
| Infrai chat classification | Chat completion with strict JSON schema | The application owns the compact `allow`, `review`, `block` contract | No dedicated moderation endpoint; classifier quality and policy prompt remain application responsibilities |

OpenRouter and Together are viable routing or inference layers for chat-model classification, especially when model choice across providers matters, but routing does not remove the need to validate the decision or define failure behavior. Anthropic Claude and Google Gemini can also be placed behind an application-owned classification contract when their structured-output behavior meets the team's tests. None of those names settles the architectural question: a team still has to specify categories, keep an evaluation set, decide how refusals map into `review` or `block`, and fail closed on transport, parsing, and schema errors. A self-hosted classifier offers maximum policy control and avoids a hosted moderation dependency; it also puts model evaluation, capacity planning, patching, burst headroom, and the pager on your team.

My decision rule is conservative. Buy a dedicated moderation service when its taxonomy matches the policy and the extra vendor boundary is acceptable. Use chat plus JSON schema when a compact application-owned decision contract and a plain REST integration matter more, provided the team is prepared to test classifier drift. Self-host only when data control or policy specialization pays for the standing operational load.

## Capacity is part of the safety design

Every attempted image now consumes classifier capacity before it consumes image capacity. Plan both. At 40 incoming prompts per second with a 2x burst assumption, the moderation tier must admit 80 classifications per second without opening a bypass; the image tier can be sized from the observed `allow` fraction, while `review` traffic needs a separately bounded queue and an explicit response-time objective.

Three metrics deserve alerts: schema-valid decision rate, fail-closed rate, and attempted gate bypasses. Latency belongs on the same dashboard, but it is not the first correctness signal. A fast parser that treats an empty action as approval is fast in the wrong direction.

Correctness comes first.

Policy changes need versioning too. Record a policy version and classifier model identifier with the decision, then sample outcomes for review without retaining more prompt data than the application's privacy rules permit. Roll a new policy as a controlled change; do not silently replace it and leave reviewers unable to explain why yesterday's prompt passed.

## Where this pattern stops working

Text-only preflight cannot judge the pixels a model will actually emit. If the risk model includes generated-image content, add a post-generation image moderation stage before delivery; Google Cloud Vision SafeSearch and Azure AI Content Safety are examples of services with image-analysis surfaces. The preflight gate still matters because it prevents known-bad requests from reaching an expensive generator, but it is not proof about the output.

The pattern is also a poor fit when regulation or organizational policy mandates a particular audited taxonomy, when human review must happen before any automated processing, or when the chosen chat model cannot reliably honor the required schema. In those cases, use the required dedicated system. No amount of retry logic repairs a policy mismatch.

For this specific pipeline, structured output correctness wins: classify the fully assembled user input, validate locally, fail closed, and allow image generation only after an explicit `allow`. Everything else, including vendor choice, follows from that invariant.

## Sources

- [OpenAI Moderation guide](https://platform.openai.com/docs/guides/moderation)
- [Azure AI Content Safety documentation](https://learn.microsoft.com/en-us/azure/ai-services/content-safety/)
- [Google Cloud Vision SafeSearch documentation](https://cloud.google.com/vision/docs/detecting-safe-search)
- [OpenRouter documentation](https://openrouter.ai/docs)
- [RFC 9110 HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110)
