# Spec: Push-based live updates for the ALPSS dashboard

Status: **Draft** · Branch: `spec/live-push-updates` · Owner: elbert

## 1. Problem

`tutorials/live_alpss_boxplot.ipynb` renders a live box plot of ALPSS results while
the lab fires shots. Today it stays current by **polling** `aimdl/count` every
`poll_seconds` (default 30) and, on any change, walking `aimdl/datafiles` newest-first.
Two consequences:

- **Latency vs. load trade-off.** A new result is invisible for up to one poll interval;
  polling faster to shrink that wastes requests when nothing is happening.
- **Clunky render.** Each tick re-encodes the whole Matplotlib figure to PNG and swaps
  `Image.value`. It works, but it flickers and ships ~50–100 KB per frame.

We want new results to appear **within a second or two of upload**, without hammering the
API, and with a smoother in-place render.

## 2. Key finding that shapes the design

There is **no push endpoint on the Girder server today.** The deployment is Girder
5.0.6 on uvicorn (ASGI); the classic SSE `/notification` and `/notification/stream`
routes both 404, and the full API surface exposes no WebSocket/SSE/feed. So push has to
be *added*, and the natural place to originate it is **where the result is produced**, not
in Girder.

`data_type: "pdv_alpss_result"` is set in the **ALPSS Dagster job**, not in Girder or the
OpenMSIStream consumers:

- `openmsi/alpss-dagster/alpss_dagster/assets.py:202`
  `"data_type": "pdv_alpss_result" if output_name == "results" else "pdv_alpss_output"`
- At that point the code already holds `item_id` (`assets.py:194`), `igsn`,
  `experiment_date`, and `context.run.run_id` — everything a notification needs.

So the emit is a few lines at the exact spot the result lands, and the client keeps a
cheap server-side delta (`aimdl/partition?since=`) as its fetch/reconciliation layer.

## 3. Target architecture

```
  ALPSS Dagster asset (PRODUCER)
    upload result → set data_type=pdv_alpss_result → publish(doorbell)
        │
        ▼
  TRANSPORT / relay  ── holds the WebSocket clients open, fans out doorbells
        │  (open decision: Kafka topic | Redis pub/sub | direct HTTP→relay)
        ▼
  Notebook client (CONSUMER)
    WebSocketSource → on doorbell: sync(since) → Store.ingest → render
                    → on (re)connect / periodic: sync(since) reconciliation
```

Three components, each behind a clean seam so transports can be swapped without touching
the others (see §7 for why almost none of the earlier polling work is wasted):

- **Trigger** — what says "new data exists." Poll timer *or* WebSocket message.
- **`sync(since)`** — idempotent "fetch everything since my watermark, parse, ingest."
  Shared by poll, doorbell, and reconnect. Built on `aimdl/partition?since=`.
- **Render** — `Store` → incremental frame. Transport-agnostic.

## 4. Component 1 — Producer (Dagster emit)

**Location:** `alpss-dagster/alpss_dagster/assets.py`, immediately after the
`addMetadataToItem` block (~line 216), inside the per-output loop.

**Gate:** `output_name == "results"` — this is exactly the `pdv_alpss_result` branch, so
one doorbell fires per new plottable data point (not per shot, not per run).

**Payload (the contract, see §6):**

```python
if output_name == "results":
    publish({
        "event": "pdv_alpss_result.created",
        "itemId": item_id,
        "igsn": igsn,
        "experiment_date": experiment_date,
        "runId": context.run.run_id,
        "url": uploaded_urls[output_name],
    })
```

**Requirements:**

- **Failure isolation.** A publish error must never fail or retry the asset. Wrap in
  `try/except`, log via `context.log.warning`, continue. The doorbell is best-effort;
  correctness is guaranteed by client-side reconciliation, not by delivery.
- **No new blocking dependency.** Publish must not add meaningful latency to the upload
  loop (fire-and-forget, short timeout).
- **`publish` is injected**, not hard-coded, so it can be a no-op when the transport is
  unconfigured (e.g. local Dagster runs) — read target from env/resource.

**Producer change ships as its own branch/PR in `alpss-dagster`**, not in this repo.

## 5. Component 2 — Transport / relay  *(open decision)*

The relay holds WebSocket connections open and fans each doorbell out to all connected
dashboards. `publish()` in the Dagster asset targets it. Options:

| Option | Producer side | Relay | Notes |
|---|---|---|---|
| **Direct HTTP → relay** | one `POST /publish` | tiny FastAPI/uvicorn app w/ `/ws` + in-memory fan-out | Simplest end-to-end. Relay is ~50 lines. No broker. Single instance. |
| **Kafka topic** | produce to `alpss.notifications` | relay consumes topic → fans out to ws | Reuses existing Kafka; durable; natural if other consumers want the event. More moving parts. |
| **Redis pub/sub** | `PUBLISH` | relay `SUBSCRIBE` → ws | Light broker; easy multi-instance fan-out. Adds Redis dep. |

**Recommendation:** start with **direct HTTP → relay** (a standalone uvicorn app exposing
`/ws` for dashboards and `POST /publish` for Dagster). It is the least infrastructure,
matches "single lab dashboard," and — because the client treats the socket as a doorbell —
can be replaced by Kafka/Redis later with zero client change. Revisit if we need
durability or multiple relay instances.

**Auth:** the relay's `/ws` needs some gate (shared token or Girder token check). TBD with
whoever stands up the endpoint. The dashboard is not public, so a shared token is likely
sufficient for v1.

## 6. Message schema (contract between all three components)

```jsonc
{
  "event": "pdv_alpss_result.created",   // string; allows future event types
  "itemId": "665f…",                     // Girder item id of the results CSV
  "igsn": "JHACRA00004-001",             // sample id (may be null)
  "experiment_date": "2026-07-06T…",     // ISO 8601; may be null upstream
  "runId": "…",                          // Dagster run id, for tracing
  "url": "https://…/#item/665f…"         // convenience link
}
```

Notes:

- **Doorbell, not payload.** The message carries enough to render *optimistically* (the
  client can fetch just `itemId`), but the source of truth for "what have I got" remains
  the client's own ingested set. The message is not required to be complete or ordered.
- The client advances its reconciliation watermark from **what it has actually ingested**
  (max item `created`), never from a field in the message — the message has no Girder
  `created` timestamp (that's server-assigned after Dagster's hands are off).

## 7. Component 3 — Client (notebook)

Refactor the notebook's fused `tick()` loop into the three seams from §3.

- **`sync(since)`** — replaces the current `count` + newest-first walk. Uses
  `aimdl/partition?since=<watermark>` to get the changed partitions, fetches/parses their
  new items, and calls `Store.ingest`. Idempotent: `Store.seen`/`itemId` dedup means
  running it twice is harmless. This is the single fetch path.
- **`WebSocketSource`** — connects to the relay, and:
  - on **connect**: `sync(since=last_ingested)` to backfill anything missed while
    disconnected (this subsumes the current seed-on-start).
  - on **message**: `sync(since=last_ingested)` (or fetch just `itemId` on the happy
    path), then `render`.
  - on **disconnect**: reconnect with backoff; on reconnect, backfill via `sync`.
  - **fallback heartbeat**: a slow `sync` every N seconds (e.g. 60–120 s) as a safety net
    for missed messages. This is the *only* residue of polling, and it's deliberate.
- **Render swap** — replace `savefig`→PNG→`Image.value` with an incremental widget.
  Candidate libs and trade-offs to be settled in implementation (Plotly `FigureWidget`,
  Bokeh `stream()` + `push_notebook()`, or ipympl). A box plot recomputes stats each
  frame regardless, so the win is "no PNG round-trip + in-place repaint," not avoiding
  recompute.

**Why the earlier polling work is not wasted:** WebSockets are lossy (reconnects, dropped
frames, sleeping kernels), so a "catch up since my watermark" fetch is mandatory *with or
without* push. That fetch is `sync(since)` built on `partition?since=`. The WebSocket
replaces only the **trigger**; `sync` and the entire render path carry over unchanged. The
sole throwaway is fast adaptive poll cadence, which degrades into the fallback heartbeat.

## 8. Delivery semantics & correctness

- **Best-effort delivery, exactly-correct state.** Doorbells may be lost, duplicated, or
  arrive out of order. Correctness comes from idempotent `sync(since)` + `itemId` dedup,
  never from the socket.
- **Dedup:** `Store.seen` already keys on `itemId`; a duplicated doorbell → duplicate
  `sync` → no new rows. Safe.
- **Backfill on connect** guarantees a dashboard opened mid-run, or one that dropped, is
  consistent within one `sync`.
- **Ordering:** `arrival_seq` in the store defines "new"; unaffected by socket ordering
  because `sync` ingests in a deterministic order.

## 9. Development plan (ordering)

Phased so each step is independently useful and nothing blocks on the server work:

1. **Client seams + render (this repo).** Refactor `tick()` into
   `sync(since)` / trigger / render, and swap the render path. Immediate UX win, no
   dependency on producer or transport. *Verify:* notebook still updates correctly under
   the existing poll trigger driving `sync`.
2. **`partition?since=` in `sync` (this repo).** Replace `count` + walk with the delta
   query. *Verify:* new results appear; a forced-missed interval is recovered on next
   `sync`.
3. **Relay (new component).** Stand up the chosen transport (§5) with `/ws` + publish
   ingress. *Verify:* a hand-posted doorbell reaches a connected notebook and triggers a
   `sync`.
4. **Producer emit (`alpss-dagster`).** Add the gated `publish(...)`. *Verify:* a real
   ALPSS run drives a live update end-to-end; publish failure leaves the asset green.
5. **Cut poll cadence to fallback heartbeat.** Push is now primary; polling is the safety
   net.

Steps 1–2 land value before any server/transport work exists.

## 10. Open decisions

- [ ] **Transport** (§5): direct HTTP relay (recommended) vs Kafka vs Redis.
- [ ] **Relay hosting & auth** — who runs it, shared token vs Girder token.
- [ ] **Render library** (§7): Plotly vs Bokeh vs ipympl.
- [ ] **Spec home** — kept in this repo (dashboard) for now; producer change PRs into
      `alpss-dagster`. Confirm this split.

## 11. Non-goals

- Adding a push endpoint *inside* Girder (rejected: Girder 5 dropped SSE; the producer is
  the right origin).
- Fat notifications that stream full result payloads over the socket (rejected: more
  producer work, still needs reconciliation; the doorbell + `sync` is simpler and robust).
- Replacing the polling model entirely with no fallback (rejected: the heartbeat is cheap
  insurance against lossy delivery).
```
