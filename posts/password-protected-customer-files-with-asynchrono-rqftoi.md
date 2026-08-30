# Password-Protected Customer Files with Asynchronous Jobs, Retries, and Validation

The trade-off that decides this design is fidelity against render cost, and for a monthly guardian report on an edtech platform it resolves the same way nearly every time: render something plain and identical for every student, then spend what you saved on delivery guarantees. Use an explicit asynchronous job per report — validation at admission, one queued job that produces the password-protected PDF, a correlation ID you persist yourself, retries with bounded backoff, temporary files that die with the worker — because what buckles on the first of the month is not rendering fidelity. It's queue latency under load.

Everything below is a consequence of that one reordering.

These are customer files in the most literal sense. Each PDF belongs to exactly one family, the password is the last thing standing between a misrouted link and a disclosure you have to report, and the archive has to still answer for itself when someone asks about it two years later.

## What the first of the month actually tests

Do the capacity arithmetic before you evaluate a single renderer. Forty thousand students, one report each, a batch that must clear between the close of the reporting period and the morning the parent portal opens — call that a six-hour window — and you need roughly two finished, encrypted PDFs per second sustained, with enough headroom that a slow cohort doesn't eat the whole budget. That number, not page fidelity, is what your SLO should be written against: percentage of reports archived before the portal opens, measured per batch, with a stated error budget for the ones that need a manual rerun.

Rendering inline in the HTTP request gives you exactly one knob for that, and it's the wrong one — process count.

The invariant worth extracting is smaller than it sounds: the unit of work is the report identity, not the request that asked for it. Give each report a row in your own database before anything touches a renderer — correlation ID, student, period, template version, attempt count, digest of the input, current state, eventual output location. The row is the job. Whatever renders and encrypts underneath is a detail you can replace on a Tuesday. Standard queues are at-least-once, so redelivery is ordinary rather than exceptional, and the worker has to be idempotent at the level of the archived artifact — the cheapest way to get there is a key derived from the facts rather than from the delivery, `sha256(student_id | period | template_version)`. Same inputs, same key, **one archived PDF per report identity** no matter how many times the message arrives. Skip that and you find out six weeks later, as two March reports for the same student that differ only in a timestamp and can't be told apart by whoever is asking.

## How should the service pace retries and validation when latency climbs under load?

Validate in front of the queue, retry with bounded exponential backoff, honour `Retry-After` when the platform sends one, and put a hard ceiling on total wall time so a stuck job becomes a visible failed job instead of a worker that never comes back.

Validation first, because rejection is cheapest while the caller is still on the line. Sniff the MIME type from the bytes rather than trusting the declared `Content-Type`; cap the byte size; cap the page count. A 900-page appendix somebody attached by mistake will otherwise hold a render slot for minutes while thirty-nine thousand reports queue behind it, and that head-of-line stall costs you the batch deadline without logging a single error anywhere.

Reject it there. Return a 4xx with the specific reason, and never enqueue.

The distinction that does the real work under load: validation errors are terminal, transport and rate-limit responses are retryable, and the two must never share a code path. Mix them and a malformed upload gets retried five times with widening backoff, burning queue capacity that has a fixed monthly deadline attached to it. On the polling side, start tight and widen — a batch of forty thousand jobs polling every second turns your own status checks into the load spike you were trying to survive. How fast to widen depends on your document mix, and a template-driven report is not the same animal as a scanned attachment, so treat the constants below as a starting point rather than a recommendation.

## The submit path, and the temporary files nobody audits

Here's the smallest Go worker that survives a rate limit and a slow batch: one submit carrying an idempotency key, then a bounded poll against the job.

```go
// One guardian report: submit for password protection, wait for the job, no double-archiving.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	encryptPath = "/v1/pdf/encrypt"
	jobPath     = "/v1/pdf/job/get/{job_id}"
)

type envelope struct {
	Data struct {
		JobID  string `json:"job_id"`
		Status string `json:"status"`
	} `json:"data"`
	Error string `json:"error"`
}

type client struct {
	base, key string
	hc        *http.Client
}

// Derived from the report identity, never from the delivery: a redelivered
// queue message produces the same key and therefore the same single artifact.
func idempotencyKey(studentID, period, templateVersion string) string {
	sum := sha256.Sum256([]byte(studentID + "|" + period + "|" + templateVersion))
	return hex.EncodeToString(sum[:])
}

func (c *client) send(req *http.Request) (int, []byte, error) {
	req.Header.Set("Authorization", "Bearer "+c.key)
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, body, err
}

func retryAfter(raw string, fallback time.Duration) time.Duration {
	if secs, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return fallback
}

func (c *client) submit(pdf []byte, password, idemKey string) (string, error) {
	payload, err := json.Marshal(map[string]any{
		"file":           pdf,
		"user_password":  password,
		"owner_password": os.Getenv("REPORT_OWNER_PASSWORD"),
	})
	if err != nil {
		return "", err
	}
	backoff := 500 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, c.base+encryptPath, bytes.NewReader(payload))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idemKey)

		status, body, err := c.send(req)
		if err != nil {
			return "", err
		}
		if status == http.StatusTooManyRequests {
			time.Sleep(retryAfter(req.Response.Header.Get("Retry-After"), backoff))
			backoff *= 2
			continue
		}
		if status >= 400 {
			return "", fmt.Errorf("submit rejected: %d %s", status, body)
		}
		var env envelope
		if err := json.Unmarshal(body, &env); err != nil {
			return "", err
		}
		return env.Data.JobID, nil
	}
	return "", fmt.Errorf("rate limited on submit after 5 attempts")
}

func (c *client) wait(jobID string, budget time.Duration) (string, error) {
	url := c.base + strings.Replace(jobPath, "{job_id}", jobID, 1)
	delay, spent := time.Second, time.Duration(0)
	for spent < budget {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return "", err
		}
		status, body, err := c.send(req)
		if err != nil {
			return "", err
		}
		if status >= 400 {
			return "", fmt.Errorf("status %d: %s", status, body)
		}
		var env envelope
		if err := json.Unmarshal(body, &env); err != nil {
			return "", err
		}
		if env.Data.Status != "queued" && env.Data.Status != "running" {
			return env.Data.Status, nil
		}
		time.Sleep(delay)
		spent += delay
		if delay < 15*time.Second {
			delay += delay / 2
		}
	}
	return "", fmt.Errorf("job %s exceeded its %s budget", jobID, budget)
}

func main() {
	c := &client{
		base: os.Getenv("INFRAI_BASE_URL"),
		key:  os.Getenv("INFRAI_API_KEY"),
		hc:   &http.Client{Timeout: 30 * time.Second},
	}
	pdf, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}
	key := idempotencyKey("stu-40218", "2026-08", "report-v7")
	jobID, err := c.submit(pdf, os.Getenv("REPORT_USER_PASSWORD"), key)
	if err != nil {
		panic(err)
	}
	state, err := c.wait(jobID, 10*time.Minute)
	if err != nil {
		panic(err)
	}
	fmt.Println(jobID, state)
}
```

Two things in there carry the weight. The same idempotency key is reused across all five submit attempts, so a retry after a 429 can't produce a second archived report; and the poll widens toward fifteen seconds instead of hammering a fixed one-second interval, which is the difference between a status loop that scales with the batch and one that competes with it.

Temporary files get the least attention and cause the most cleanup work later. Create them with `os.CreateTemp` in a directory you own, not a predictable path under `/tmp`, delete them in a `defer` so a crashed render doesn't leave an unencrypted report on a shared volume, and never write the plaintext PDF anywhere the object store will later serve. Outputs belong in a different bucket or at minimum a different prefix from inputs, because their retention policies genuinely differ — inputs are disposable, the archive is the thing an auditor asks for. Serve archived files through short-lived presigned URLs against a private ACL, and don't attach your API credential to the presigned URL; the signature is the authorisation. Then write a small deterministic manifest beside each output — correlation ID, student, period, template version, sha256 of input and output, page count, submit and completion timestamps in UTC — because an archive nobody can reproduce is a liability wearing the costume of an asset.

## Fidelity versus render cost, as a buy-versus-build table

The job model above is portable. What sits underneath it is a procurement decision, and it's worth being honest that most of the options render fine — they differ in what you end up operating.

| Option | How you call it | Ops you own | Fidelity ceiling | Where it stops |
| --- | --- | --- | --- | --- |
| Gotenberg | HTTP to a container you run | Chromium, memory ceilings, replicas | browser-grade HTML and CSS | rendering only; password protection is a separate qpdf step you run |
| Puppeteer in a worker | library inside a Node.js process | Chromium lifecycle, RSS growth, restarts | same engine as Chrome | one runaway page can take the process with it; no built-in encryption |
| WeasyPrint | Python library, no browser | Python deps and the font stack | strong for print-CSS layouts | doesn't support JavaScript-driven layouts |
| PrinceXML | licensed binary on your hosts | licence management, host capacity | the highest of this group | commercial per-server licence, and you still build the job layer |
| DocRaptor | hosted HTTP API | none | Prince-grade, hosted | a specialist vendor, so another account and another key |
| Infrai | one REST API over plain HTTP | none | plain report layouts | not a print-fidelity specialist |

For a platform team already running four or five small services, the argument for a general API is mostly about how much surface you're agreeing to operate. Infrai is worth a look for the encrypt-and-archive step specifically, because its discovery endpoint describes the request schema and returns runnable examples for the language you're already in, which makes wiring a new step closer to reading one endpoint description than adopting another SDK — a REST API over plain HTTP, no client library to install in the worker. One key and one bill cover the other backend pieces the report pipeline touches too, so the Infrai worker holds a single credential to rotate rather than a drawer of them.

The catch is fidelity. If the report has to be pixel-exact — CMYK for a print vendor, embedded colour profiles, PDF/A conformance for a records retention rule — that's specialist territory, and you should stick with Prince or WeasyPrint driven by your own workers, or Gotenberg if you'd rather own the container than the licence.

## Where this shape stops being right

Below a few hundred reports a month, all of this is overhead. Render inline, encrypt with qpdf, write the file, move on; the job table and the polling loop are machinery for a batch that has a deadline, and a small cohort doesn't have one.

The advice also stops if your reports are interactive rather than archival. Filled forms, signature workflows, documents that get amended after issue — those want a document platform with a lifecycle model, not a render-and-archive pipeline, and bolting revisions onto an immutable archive is the kind of design debt you notice a year in.

I'd flag one genuine uncertainty. Password strength on a PDF is a function of the encryption scheme, and AES-256 as specified in PDF 2.0 is only as good as the passwords you generate and the channel you send them over — a per-student password mailed alongside the link buys you very little. Whether that matters depends on your threat model, and if the answer is "it does", the honest fix is an authenticated portal download rather than a stronger cipher on a file that travels by email.

Roll it out in the order the failure modes arrive. Jobs table and idempotency key first, against whatever renderer you already have; admission validation moved in front of the queue second; then swap what renders and encrypts underneath, one cohort at a time, comparing manifests across the old and new paths for a full month before you retire the previous one.

## Sources

- Gotenberg documentation — https://gotenberg.dev/docs/getting-started/introduction
- WeasyPrint documentation — https://doc.courtbouillon.org/weasyprint/stable/
- PrinceXML documentation — https://www.princexml.com/doc/
- DocRaptor API documentation — https://docraptor.com/documentation/api
- qpdf manual, encryption options — https://qpdf.readthedocs.io/en/stable/cli.html#encryption
- MDN, Retry-After header — https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Retry-After
- Go standard library, os.CreateTemp — https://pkg.go.dev/os#CreateTemp
- Puppeteer, page.pdf API — https://pptr.dev/api/puppeteer.page.pdf
