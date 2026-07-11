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

## 2. Approach: subscribe to Girder's WebSocket announcements (B1, confirmed live)

The Girder server runs a live WebSocket announcement stream, and the producer of each
ALPSS result publishes to it. This is now **implemented and observed working** — the
dashboard just needs to subscribe.

Two properties of the real implementation shape the client:

- **It's a *fat* notification.** The payload is the **entire ALPSS result row** (all
  metrics, `igsn`, filename, OK-flags), not a thin doorbell. This is advantageous here
  because the producer already holds that complete row — so there is *no extra producer
  work* and the dashboard's **live path needs no fetch at all**: it builds a plot row
  straight from the message.
- **It's a public firehose.** `/notifications/public` forwards all announcements;
  the client **filters by `message["type"] == "pdv_alpss_result"`**.

The REST path (`aimdl/partition?since=`, download, parse) does **not** go away — the
WebSocket only pushes results *going forward*, so historical **seeding** and
**reconnect/missed-message backfill** still use it. Live rows (from the socket) and
backfill rows (from REST) must therefore share one dedup key — see §7.

`data_type: "pdv_alpss_result"` originates in the ALPSS Dagster job
(`openmsi/alpss-dagster/alpss_dagster/assets.py:202`), which is where the published row
and the Girder `item_id` (`assets.py:194`) both exist.

## 3. Target architecture

```
  ALPSS Dagster asset (PRODUCER)               [server side: implemented]
    compute result row → upload → set data_type=pdv_alpss_result
                        → publish full row (+ itemId) to Girder ws
        │
        ▼
  Girder WebSocket  wss://girder.htmdec.org/notifications/public
        │  fans announcements out to authenticated subscribers
        ▼
  Notebook client (CONSUMER)                   [this repo: to build]
    subscribe + auth → filter type==pdv_alpss_result
      → row_from_notification(data) → Store.ingest → render      (live: zero fetch)
      → on (re)connect / periodic heartbeat: sync(since) reconciliation (REST)
```

Three seams, so the trigger can change without touching fetch or render (see §7 for why
almost none of the earlier polling work is wasted):

- **Trigger** — what says "new data exists." A Girder ws notification (primary) or the
  fallback poll/heartbeat.
- **`sync(since)`** — idempotent "fetch everything since my watermark, parse, ingest,"
  used for seed and reconnect backfill. Built on `aimdl/partition?since=`.
- **Render** — `Store` → incremental frame. Transport-agnostic.

## 4. Producer (server side — implemented; two fields requested)

The producer publishes the full ALPSS result row to the Girder ws at the point the result
is uploaded and classified `pdv_alpss_result` (Dagster job, `assets.py`, the `results`
branch). This side is already working (observed in a live announcement).

**One addition requested of the producer** (trivial — the value is already in scope at
the emit site):

- `itemId` — the Girder item id (`assets.py:194`). Gives the client a **stable dedup key
  that matches the REST backfill path**, plus a convenience link. Without it we would have
  to key on `File Name`, which is more fragile.

**`experiment_date` is *not* added to the payload** — the client derives it from the
`File Name` already in the message (see §6/§7). That filename is the same source the
OpenMSIStream consumer uses to set `meta.experiment_date`
(`xarthisius/girder-consumers/common/base.py` `parse_date`), so parsing it in the notebook
with the same rule keeps live rows consistent with REST-seeded rows — and adds no work for
the sysadmin. Note the payload's `Date`/`Time` fields are when *ALPSS ran*, not the shot;
do not use them for windowing (the clock trap documented in `src/aimdl_examples/live.py`).

Failure isolation still applies on the producer: a publish error must never fail the asset.

## 5. Transport — Girder WebSocket (resolved)

- **Endpoint:** `wss://girder.htmdec.org/notifications/public` (confirmed). Note the
  dashboard's REST base is `data.htmdec.org/api/v1`; same Girder, different hostname.
- **Auth handshake:** open the socket, then send
  `{"type": "auth", "token": <girder-token-id>}` as the first message. The notebook
  already authenticates a `GirderClient` with its API key; reuse that client's token
  (`gc.token`) — no separate exchange needed.
- **Subscription/filter:** the stream is a public firehose; there is no server-side
  per-type subscription. The client reads every message and keeps only
  `msg["type"] == "pdv_alpss_result"`.
- **Encoding:** messages are JSON text; the payload may contain bare `NaN` literals.
  Python's `json.loads` accepts these (→ `float('nan')`), which the row builder already
  treats as "no value." A stricter parser would need `parse_constant`.

## 6. Message schema (observed, with itemId added)

```jsonc
{
  "type": "pdv_alpss_result",
  "data": {
    // --- requested addition ---
    "itemId": "665f…",                          // dedup key; matches REST item _id

    // --- already present ---
    "igsn": "JHABOX00000",
    "File Name": "C1--JHABOX00000_2026-06-26_15-25-18_shot16--00000.csv",
                                                 // ↑ experiment_date is parsed from this
    "Velocity at Max Compression": 12.006,      // == METRICS[VELOCITY]["src"]
    "Spall Strength": NaN,                       // == METRICS[SPALL]["src"] (×1e-9)
    "Velocity OK": false, "Spall OK": false,     // flags; not used for plotting
    "Date": "Jun 29 2026", "Time": "05:14 PM",   // ALPSS RUN time — do NOT window on this
    "…": "…"                                      // full ALPSS output row
  }
}
```

Key alignment: the metric keys in `data` are exactly `METRICS[...]["src"]`
(`"Velocity at Max Compression"`, `"Spall Strength"`), so the client's metric extraction
is the same loop `parse_row` already runs — just reading from the message dict instead of
a CSV row.

`experiment_date` is derived from `File Name`, not sent: the `YYYY-MM-DD_HH-MM-SS` token
(`2026-06-26_15-25-18` above) is the shot time. Parse it with the same rule as
`girder-consumers/common/base.py` `parse_date` so it equals `meta.experiment_date` on the
REST-seeded rows.

## 7. Client (notebook — to build)

Refactor the notebook's fused `tick()` loop into the three seams from §3.

- **`row_from_notification(data)`** — new, small: build a `Store` row directly from a
  notification's `data`, reusing the existing `METRICS`/scale logic for the metric fields.
  - `itemId` ← `data["itemId"]` (dedup key; identical to the REST path's item `_id`)
  - `igsn` ← `data["igsn"]`
  - metrics ← same `spec["src"]` loop as `parse_row`
  - `experiment_ts` ← parse the `YYYY-MM-DD_HH-MM-SS` shot token out of
    `data["File Name"]`, mirroring `girder-consumers/common/base.py` `parse_date`, then
    `parse_experiment_ts`. Matches `meta.experiment_date` on REST-seeded rows.
  - `created` ← client receipt time (only a tiebreaker; the socket carries no Girder
    ingest time)
  - `arrival_seq` / `_jit` ← assigned by `Store._admit`, exactly as today
  No download, no CSV read on the live path.
- **`sync(since)`** — the REST path, used for **seed** (startup) and **reconnect backfill**
  only. Uses `aimdl/partition?since=<watermark>` for the delta, then the existing
  download+`parse_row`+`Store.ingest`. Idempotent via `itemId` dedup. The single fetch
  path.
- **`GirderWebSocketSource`** — connects to `wss://girder.htmdec.org/notifications/public`,
  sends the auth message, then:
  - on **connect**: `sync(since=last_ingested)` to backfill anything missed while
    disconnected (subsumes the current seed-on-start).
  - on **message** with `type == "pdv_alpss_result"`: `Store.ingest([row_from_notification(data)])`,
    then `render`. (Zero fetch.)
  - on **disconnect**: reconnect with backoff; on reconnect, backfill via `sync`.
  - **fallback heartbeat**: a slow `sync` every N seconds (e.g. 60–120 s) as a safety net
    for missed announcements. This is the *only* residue of polling, and it's deliberate.
- **Render swap** — replace `savefig`→PNG→`Image.value` with an incremental widget
  (Plotly `FigureWidget`, Bokeh `stream()`+`push_notebook()`, or ipympl — decided at
  implementation; §10). A box plot recomputes stats each frame regardless, so the win is
  "no PNG round-trip + in-place repaint," not avoiding recompute.

**Dedup across both paths is by `itemId`.** `Store.seen` already keys on it, so a result
that arrives first over the socket and later shows up in a reconnect `sync` (or vice
versa) is admitted once. This is exactly why the requested `itemId` field matters.

**Why the earlier polling work is not wasted:** WebSockets are lossy (reconnects, dropped
frames, sleeping kernels), so a "catch up since my watermark" fetch is mandatory *with or
without* push. That fetch is `sync(since)` on `partition?since=`. The socket replaces only
the **trigger** for the live path; `sync` and the entire render path carry over. The sole
throwaway is fast adaptive poll cadence, which degrades into the fallback heartbeat.

## 8. Delivery semantics & correctness

- **Best-effort delivery, exactly-correct state.** Announcements may be lost, duplicated,
  or arrive out of order. Correctness comes from idempotent `sync(since)` + `itemId`
  dedup, never from the socket.
- **Dedup:** `Store.seen` keys on `itemId`; a duplicated announcement or a live/backfill
  overlap → no double-count.
- **Backfill on connect** guarantees a dashboard opened mid-run, or one that dropped, is
  consistent within one `sync`.
- **Ordering:** `arrival_seq` defines "new"; unaffected by socket ordering because live
  ingestion is single-threaded and `sync` ingests deterministically.
- **Failed/low-quality results** are announced too (e.g. `Velocity OK: false`). The client
  applies the same rule as `parse_row`: plot whatever ALPSS reports; a metric that is
  `NaN` is simply absent from that metric's view.

## 9. Development plan (ordering)

Phased so each step is independently useful:

1. **Client seams + render (this repo).** Refactor `tick()` into
   `sync(since)` / trigger / render and swap the render path. Immediate UX win, no
   dependency on the socket. *Verify:* notebook still updates under a poll trigger driving
   `sync`.
2. **`partition?since=` in `sync` (this repo).** Replace `count` + walk with the delta
   query. *Verify:* new results appear; a forced-missed interval is recovered on next
   `sync`.
3. **`GirderWebSocketSource` + `row_from_notification` (this repo).** Subscribe, auth,
   filter, ingest live rows with zero fetch; wire connect/reconnect backfill and the
   heartbeat. *Verify:* a real announcement renders within ~1–2 s; killing the socket and
   reconnecting backfills the gap; the same result over socket then `sync` is admitted
   once.
4. **Confirm `itemId` in the payload.** Once `itemId` lands, key `Store.seen` on it across
   both paths. *Verify:* a result seen over the socket and again in a `sync` is admitted
   once (dedup key matches REST `_id`). `experiment_date` is filename-derived by design, no
   producer dependency.
5. **Cut poll cadence to fallback heartbeat.** Push is primary; polling is the safety net.

Steps 1–2 land value before the socket work; step 3 works against the already-live
endpoint.

## 10. Open decisions

- [ ] **Render library** (§7): Plotly vs Bokeh vs ipympl.
- [x] **Producer fields** (§4): `itemId` will be added (dedup key). `experiment_date` will
      *not* be added — derived in the notebook from `File Name`, matching how
      `meta.experiment_date` is set upstream.
- [ ] **Spec home** — kept in this repo (dashboard); producer change lives in
      `alpss-dagster`. Confirm this split.

## 11. Non-goals / rejected alternatives

- **Build our own notification relay** (rejected: Girder already runs the WebSocket and
  the producer already publishes to it).
- **Server-side Girder event hook on item create** (rejected: server plugin code; the
  producer already owns the emit and the full row. Revisit only to notify on
  non-Dagster `pdv_alpss_result` uploads).
- **Thin doorbell + always-fetch** (superseded: the implementation is fat and the producer
  already holds the full row, so the live path is zero-fetch. Reconciliation via REST is
  still kept for seed/backfill — that part is unchanged).
- **Replacing polling entirely with no fallback** (rejected: the heartbeat is cheap
  insurance against lossy delivery).
```
