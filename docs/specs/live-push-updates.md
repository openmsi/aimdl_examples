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

## 2. Approach: publish to Girder's WebSocket from the producer (B1)

The Girder server **has a live WebSocket** for notifications. But Girder does not emit a
notification on item creation by itself — *something has to publish one.* The producer of
the result already knows the exact moment it lands and holds all the context, so it
publishes the notification directly to Girder's notification API; the dashboard subscribes
to Girder's WebSocket and updates.

`data_type: "pdv_alpss_result"` is set in the **ALPSS Dagster job**:

- `openmsi/alpss-dagster/alpss_dagster/assets.py:202`
  `"data_type": "pdv_alpss_result" if output_name == "results" else "pdv_alpss_output"`
- At that point the code already holds `item_id` (`assets.py:194`), `igsn`,
  `experiment_date`, `context.run.run_id`, and an authenticated Girder client (`gc`).

So the emit is a few lines at the exact spot the result lands — a Girder API call, not a
new service — and the client keeps a cheap server-side delta (`aimdl/partition?since=`) as
its reconciliation layer.

**Why B1 over the alternatives** (see §11): reusing Girder's running socket means **no
relay to build, host, or operate**, and auth is handled by existing Girder tokens. A
server-side Girder event hook (call it B2) would also work and would catch non-Dagster
uploads too, but it is server plugin code to deploy; B1 is a few lines in a job we already
own. Building our own relay adds a service for no benefit now that Girder can push.

## 3. Target architecture

```
  ALPSS Dagster asset (PRODUCER)
    upload result → set data_type=pdv_alpss_result
                  → gc → publish notification to Girder  (fire-and-forget)
        │
        ▼
  Girder WebSocket  ── fans the notification out to subscribed dashboards
        │
        ▼
  Notebook client (CONSUMER)
    subscribe to Girder ws → on notification: sync(since) → Store.ingest → render
                           → on (re)connect / periodic: sync(since) reconciliation
```

Three seams, so the trigger can change without touching fetch or render (see §7 for why
almost none of the earlier polling work is wasted):

- **Trigger** — what says "new data exists." Poll timer *or* a Girder ws notification.
- **`sync(since)`** — idempotent "fetch everything since my watermark, parse, ingest."
  Shared by poll, notification, and reconnect. Built on `aimdl/partition?since=`.
- **Render** — `Store` → incremental frame. Transport-agnostic.

## 4. Component 1 — Producer (Dagster emit)

**Location:** `alpss-dagster/alpss_dagster/assets.py`, immediately after the
`addMetadataToItem` block (~line 216), inside the per-output loop.

**Gate:** `output_name == "results"` — this is exactly the `pdv_alpss_result` branch, so
one notification fires per new plottable data point (not per shot, not per run).

**Emit — publish to Girder's notification API via the client already in hand:**

```python
if output_name == "results":
    try:
        girder.publish_notification(              # thin wrapper over Girder's ws publish API
            type="pdv_alpss_result.created",
            data={
                "itemId": item_id,
                "igsn": igsn,
                "experiment_date": experiment_date,
                "runId": context.run.run_id,
                "url": uploaded_urls[output_name],
            },
        )
    except Exception as e:                          # never fail the asset on a notify error
        context.log.warning("notify failed for %s: %s", item_id, e)
```

**Requirements:**

- **Failure isolation.** A publish error must never fail or retry the asset. The
  notification is best-effort; correctness is guaranteed by client-side reconciliation
  (§8), not by delivery.
- **No new blocking dependency.** Fire-and-forget with a short timeout; it must not slow
  the upload loop.
- **`publish_notification` is a thin method on the existing `GirderConnection` resource**
  (`alpss_dagster/resources.py`), wrapping whatever Girder route/model publishes a
  notification (§5, open item). It no-ops when unconfigured (local Dagster runs).

The exact Girder publish call is the one API detail still to confirm (§5). Everything else
in the producer is settled. **Producer change ships as its own branch/PR in
`alpss-dagster`**, not in this repo.

## 5. Transport — Girder WebSocket notifications  *(chosen; API details to confirm)*

Decision: **use Girder's existing WebSocket** (B1). No relay is built or hosted. What
remains is to pin the concrete API against the running Girder 5.0.6 deployment — these do
**not** block client phases 1–2, but must be answered before phases 3–4:

- [ ] **Publish API** — the route/model the Dagster job calls to emit a notification
      (e.g. an authenticated `gc.post(...)`), and its payload shape and size limits.
- [ ] **Subscribe API** — how a client opens the ws and receives notifications: the ws
      URL, the handshake/auth (Girder token via query param / header / subprotocol), and
      any subscribe/filter message.
- [ ] **Filtering & scoping** — can the client receive only `pdv_alpss_result.created`
      (by type), and are notifications broadcast to the dashboard's identity or scoped
      per-user? The dashboard's API-key user must actually receive them.
- [ ] **Type string** — the canonical notification `type` used to publish and filter
      (proposed: `pdv_alpss_result.created`).

Owner: sysadmin who stood up the WebSocket. Note: the classic Girder `/notification`
routes 404 on this deployment, so the publish/subscribe mechanism is whatever Girder 5
exposes — get it from the sysadmin rather than assuming the older SSE model.

## 6. Message schema (contract between producer and client)

```jsonc
{
  "type": "pdv_alpss_result.created",    // notification type; used to publish & filter
  "data": {
    "itemId": "665f…",                   // Girder item id of the results CSV
    "igsn": "JHACRA00004-001",           // sample id (may be null)
    "experiment_date": "2026-07-06T…",   // ISO 8601; may be null upstream
    "runId": "…",                        // Dagster run id, for tracing
    "url": "https://…/#item/665f…"       // convenience link
  }
}
```

(Envelope shape — `type`/`data` vs. Girder's own notification wrapper — is finalized once
§5's publish API is confirmed.)

Notes:

- **Doorbell, not payload.** The message carries enough to render *optimistically* (the
  client can fetch just `itemId`), but the source of truth for "what have I got" remains
  the client's own ingested set. The message is not required to be complete or ordered.
- The client advances its reconciliation watermark from **what it has actually ingested**
  (max item `created`), never from a field in the message — the message has no Girder
  `created` timestamp (that's server-assigned after Dagster's hands are off).

## 7. Component 2 — Client (notebook)

Refactor the notebook's fused `tick()` loop into the three seams from §3.

- **`sync(since)`** — replaces the current `count` + newest-first walk. Uses
  `aimdl/partition?since=<watermark>` to get the changed partitions, fetches/parses their
  new items, and calls `Store.ingest`. Idempotent: `Store.seen`/`itemId` dedup means
  running it twice is harmless. This is the single fetch path.
- **`GirderWebSocketSource`** — connects to Girder's ws with the dashboard's token, and:
  - on **connect**: `sync(since=last_ingested)` to backfill anything missed while
    disconnected (this subsumes the current seed-on-start).
  - on **notification** (`type == "pdv_alpss_result.created"`): `sync(since=last_ingested)`
    (or fetch just `itemId` on the happy path), then `render`.
  - on **disconnect**: reconnect with backoff; on reconnect, backfill via `sync`.
  - **fallback heartbeat**: a slow `sync` every N seconds (e.g. 60–120 s) as a safety net
    for missed notifications. This is the *only* residue of polling, and it's deliberate.
- **Render swap** — replace `savefig`→PNG→`Image.value` with an incremental widget.
  Candidate libs and trade-offs to be settled in implementation (Plotly `FigureWidget`,
  Bokeh `stream()` + `push_notebook()`, or ipympl). A box plot recomputes stats each
  frame regardless, so the win is "no PNG round-trip + in-place repaint," not avoiding
  recompute.

**Why the earlier polling work is not wasted:** WebSockets are lossy (reconnects, dropped
frames, sleeping kernels), so a "catch up since my watermark" fetch is mandatory *with or
without* push. That fetch is `sync(since)` built on `partition?since=`. Girder's ws
replaces only the **trigger**; `sync` and the entire render path carry over unchanged. The
sole throwaway is fast adaptive poll cadence, which degrades into the fallback heartbeat.

## 8. Delivery semantics & correctness

- **Best-effort delivery, exactly-correct state.** Notifications may be lost, duplicated,
  or arrive out of order. Correctness comes from idempotent `sync(since)` + `itemId`
  dedup, never from the socket.
- **Dedup:** `Store.seen` already keys on `itemId`; a duplicated notification → duplicate
  `sync` → no new rows. Safe.
- **Backfill on connect** guarantees a dashboard opened mid-run, or one that dropped, is
  consistent within one `sync`.
- **Ordering:** `arrival_seq` in the store defines "new"; unaffected by socket ordering
  because `sync` ingests in a deterministic order.

## 9. Development plan (ordering)

Phased so each step is independently useful and nothing blocks on the Girder ws API
details (§5):

1. **Client seams + render (this repo).** Refactor `tick()` into
   `sync(since)` / trigger / render, and swap the render path. Immediate UX win, no
   dependency on transport. *Verify:* notebook still updates correctly under the existing
   poll trigger driving `sync`.
2. **`partition?since=` in `sync` (this repo).** Replace `count` + walk with the delta
   query. *Verify:* new results appear; a forced-missed interval is recovered on next
   `sync`.
3. **Confirm Girder ws API (§5) + `GirderWebSocketSource` (this repo).** Wire the client
   subscribe/filter to Girder's ws. *Verify:* a hand-published `pdv_alpss_result.created`
   notification reaches a connected notebook and triggers a `sync`.
4. **Producer emit (`alpss-dagster`).** Add `girder.publish_notification(...)` on the
   `results` branch, plus the `GirderConnection.publish_notification` wrapper. *Verify:* a
   real ALPSS run drives a live update end-to-end; publish failure leaves the asset green.
5. **Cut poll cadence to fallback heartbeat.** Push is now primary; polling is the safety
   net.

Steps 1–2 land value before any transport work; step 3 only needs the sysadmin's ws
answers, not new infrastructure.

## 10. Open decisions

- [ ] **Girder ws API details** (§5): publish route, subscribe/handshake/auth, filtering,
      type string — from the sysadmin.
- [ ] **Render library** (§7): Plotly vs Bokeh vs ipympl.
- [ ] **Spec home** — kept in this repo (dashboard) for now; producer change PRs into
      `alpss-dagster`. Confirm this split.

## 11. Non-goals / rejected alternatives

- **Build our own notification relay** (rejected: Girder already runs a WebSocket; a
  separate service is infrastructure for no benefit under B1).
- **Server-side Girder event hook that fires on item create** (B2 — rejected for now: it
  is server plugin code to deploy; the Dagster job already owns the emit and knows the
  data_type. Revisit only if we need to notify on non-Dagster `pdv_alpss_result` uploads).
- **Fat notifications** that stream full result payloads over the socket (rejected: more
  producer work, still needs reconciliation; the doorbell + `sync` is simpler and robust).
- **Replacing polling entirely with no fallback** (rejected: the heartbeat is cheap
  insurance against lossy delivery).
```
