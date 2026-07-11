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
  metrics, `igsn`, filename, OK-flags), not a thin doorbell — so the dashboard's **live
  path needs no fetch at all**: it builds a plot row straight from the message.
- **It's triggered server-side by Girder's `item.updated` event** (metadata change) on a
  `pdv_alpss_result` item (see §4), not by an explicit publish call in the producer.
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
  ALPSS Dagster asset (PRODUCER)               [no code change expected]
    compute result row → upload → addMetadataToItem(data_type=pdv_alpss_result, …)
        │  metadata mutation triggers a Girder-side hook, which
        ▼  assembles the full row (+ itemId, experiment_date) and publishes it
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

## 4. Producer / trigger (server side — implemented, no code change expected)

The announcement is emitted by a **server-side hook on Girder's `item.updated` event**
(fires when an item's metadata changes). This is deliberate: Dagster uploads in two steps —
create the file, then `addMetadataToItem` — and only *after* the metadata step is the item
known to be a `pdv_alpss_result`. So the hook keys on the metadata write
(`assets.py:196`), which means **no code change to `alpss-dagster` is expected** — the
write it already does is the trigger. Girder assembles the payload (the full ALPSS result
row plus `itemId` and `experiment_date`) and publishes it. A notification failure is a
Girder-side concern, decoupled from the upload; it cannot fail the asset.

**Caveat (from the sysadmin):** because the trigger is `item.updated`, *any* metadata
change to a matching item re-emits — including edits unrelated to a new result. The client
must be robust to repeat and spurious notifications; `itemId` dedup handles this (§8). This
is also exactly why the manual test in §9 works.

**Both fields we rely on are in the payload** (confirmed in a live announcement):

- `itemId` — the Girder item id (e.g. `6a5101df6d1201b93e76daf8`). The **stable dedup key
  that matches the REST backfill path** (item `_id`), plus a convenience link.
- `experiment_date` — e.g. `"2026-02-05"`. This is the **same value the producer writes to
  `meta.experiment_date`** (same `experiment_date` variable in `assets.py`), so using it
  makes live rows consistent with REST-seeded rows by construction. It is day-granular
  today and will gain a time component when the upstream date bug is fixed — both paths
  upgrade together automatically, no notebook change. **Prefer this field over parsing
  `File Name`**, which carries the fuller shot time (`…_2026-02-05_21-08-12_…`): that extra
  precision would *diverge* from the day-granular `meta.experiment_date` and desync the two
  paths.

Note the payload's `Date`/`Time` fields are when *ALPSS ran*, not the shot; do not use them
for windowing (the clock trap in `src/aimdl_examples/live.py`).

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

## 6. Message schema (observed — itemId and experiment_date present)

```jsonc
{
  "type": "pdv_alpss_result",
  "data": {
    // --- added at our request ---
    "itemId": "6a5101df6d1201b93e76daf8",       // dedup key; matches REST item _id
    "experiment_date": "2026-02-05",             // day-granular; == meta.experiment_date

    // --- already present ---
    "igsn": "JHAMAC00003-S7R5C3",
    "File Name": "JHAMAC00003-S7R5C3_2026-02-05_21-08-12_shot02_ch1.csv",
                                                 // carries full shot time; NOT used (see §4)
    "Velocity at Max Compression": 6.4599,      // == METRICS[VELOCITY]["src"]
    "Spall Strength": NaN,                       // == METRICS[SPALL]["src"] (×1e-9)
    "Velocity OK": false, "Spall OK": false,     // flags; not used for plotting
    "Date": "Jun 11 2026", "Time": "05:15 PM",   // ALPSS RUN time — do NOT window on this
    "…": "…"                                      // full ALPSS output row
  }
}
```

Key alignment: the metric keys in `data` are exactly `METRICS[...]["src"]`
(`"Velocity at Max Compression"`, `"Spall Strength"`), so the client's metric extraction
is the same loop `parse_row` already runs — just reading from the message dict instead of
a CSV row.

Use `data["experiment_date"]` directly (via `parse_experiment_ts`); it is the same value
as `meta.experiment_date` on REST-seeded rows, so the two paths stay consistent and both
upgrade to full precision together when the upstream date bug is fixed. Do **not** parse
the fuller shot time out of `File Name` — it would diverge from the day-granular metadata.

## 7. Client (notebook — to build)

Refactor the notebook's fused `tick()` loop into the three seams from §3.

- **`row_from_notification(data)`** — new, small: build a `Store` row directly from a
  notification's `data`, reusing the existing `METRICS`/scale logic for the metric fields.
  - `itemId` ← `data["itemId"]` (dedup key; identical to the REST path's item `_id`)
  - `igsn` ← `data["igsn"]`
  - metrics ← same `spec["src"]` loop as `parse_row`
  - `experiment_ts` ← `parse_experiment_ts(data["experiment_date"])` — the same value as
    `meta.experiment_date`, so live and REST rows agree by construction (do not parse
    `File Name`; see §4).
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
- **Dedup:** `Store.seen` keys on `itemId`. Because the trigger is `item.updated`, repeat
  and unrelated-edit notifications for an already-seen `itemId` are *expected* — they
  ingest as no-ops. Ingestion is **first-wins**: an already-seen item is not re-read, so a
  re-emission carrying *changed* values would not update the plot. That is fine for
  write-once ALPSS results; if corrected re-runs must update the live view, switch the
  store to upsert-by-`itemId` (small change; out of scope for v1).
- **Backfill on connect** guarantees a dashboard opened mid-run, or one that dropped, is
  consistent within one `sync`.
- **Ordering:** `arrival_seq` defines "new"; unaffected by socket ordering because live
  ingestion is single-threaded and `sync` ingests deterministically.
- **Failed/low-quality results** are announced too (e.g. `Velocity OK: false`). The client
  applies the same rule as `parse_row`: plot whatever ALPSS reports; a metric that is
  `NaN` is simply absent from that metric's view.

## 9. Development plan (ordering)

**Manual test trigger.** Editing metadata on any `pdv_alpss_result` item in the Girder UI
(e.g. `https://girder.htmdec.org/#item/6a5101df6d1201b93e76daf8`) fires a real
announcement. This drives client build/verification (phase 3) without waiting for a live
shot or re-running Dagster; the client dedups by `itemId`, so re-poking the same item is
harmless.

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
4. **Confirm end-to-end on a real run** — a live ALPSS shot should emit with **no
   `alpss-dagster` code change** (the existing `addMetadataToItem` is the trigger). Key
   `Store.seen` on `itemId` across both paths. *Verify:* a result seen over the socket and
   again in a `sync` is admitted once (dedup matches REST `_id`), and its `experiment_ts`
   equals the REST row's (same `experiment_date` value).
5. **Cut poll cadence to fallback heartbeat.** Push is primary; polling is the safety net.

Steps 1–2 land value before the socket work; step 3 works against the already-live
endpoint.

## 10. Open decisions

- [x] **Render library** (§7; decided at §12 S2): **Bokeh** (`ColumnDataSource` refill +
      `push_notebook()`). Boxes are drawn manually from Tukey stats (`box_glyph_data`);
      Plotly `FigureWidget` and ipympl were the alternatives.
- [x] **Producer fields** (§4): both `itemId` and `experiment_date` are in the payload
      (confirmed live). Use `experiment_date` directly (== `meta.experiment_date`); ignore
      the fuller shot time in `File Name` to stay consistent with REST rows.
- [ ] **Spec home** — kept in this repo (dashboard). Producer side appears to need no code
      change (server-side metadata-mutation trigger); if any `alpss-dagster` tweak turns
      out necessary it PRs there.

## 11. Non-goals / rejected alternatives

- **Build our own notification relay** (rejected: Girder already runs the WebSocket and
  the producer already publishes to it).
- **Server-side Girder event hook** — *not* rejected; this is in fact what the sysadmin
  implemented (the announcement fires on metadata mutation of a `pdv_alpss_result` item).
  The upside: the Dagster job needs no publish call — its existing metadata write is the
  trigger. Noted here to correct the earlier B1-vs-B2 framing.
- **Thin doorbell + always-fetch** (superseded: the implementation is fat and the producer
  already holds the full row, so the live path is zero-fetch. Reconciliation via REST is
  still kept for seed/backfill — that part is unchanged).
- **Replacing polling entirely with no fallback** (rejected: the heartbeat is cheap
  insurance against lossy delivery).

## 12. Build sequence & context strategy

Built as a series of small, independently committed steps, each done in its **own Claude
Code session** (`/clear` between them). The spec — not the chat history — is the memory: a
fresh session re-reads this file, finds where the work is, and continues. Keep each session
to **one step, then stop and report**, so context stays small and there is a review point
between steps.

**How a session orients (do this first, every session):**

1. Read this spec.
2. Find the next step to do: the lowest step in the checklist below that is both unchecked
   *and* has no matching `build(Sx): …` commit in `git log --oneline` on the
   `spec/live-push-updates` branch. (Check both, in case a prior session ticked the box but
   did not finish, or vice versa.)
3. Do **only** that step. Verify against its *Done when*. Tick its box in this file. Commit
   with the `build(Sx):` marker. Then stop and summarize — do not start the next step.

**Working conventions:**

- **Logic in `src/aimdl_examples/live.py`; keep the notebook thin** (wiring + widgets only).
  Verbose notebook JSON is expensive to re-read and hard to diff.
- **Clear notebook outputs before committing.**
- **Add/extend unit tests** for pure functions as the verify gate (`row_from_notification`,
  `itemId` dedup, date/consistency). Tests must pass with no live server.
- One step per session; don't jump ahead or bundle steps.
- Commit trailer: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

**Steps:**

- [x] **S1 — Extract seams (pure refactor, no behavior change).**
  Files: `live.py`, `tutorials/live_alpss_boxplot.ipynb`. Split the fused poll loop into
  `sync(since)` / trigger / render, still driven by the existing poll.
  *Done when:* the notebook behaves identically under the poll; the seams are importable
  from `live.py`. Commit `build(S1): extract sync/trigger/render seams`.

- [x] **S2 — Render swap.**
  Files: `live.py` render path, notebook. Replace `savefig`→PNG→`Image` with an incremental
  widget. **Chosen: Bokeh** (user-confirmed over the Plotly default). `build_bokeh_figure`
  builds the figure + `ColumnDataSource`s once; `update_bokeh` refills them and the notebook
  `push_notebook`s the diff — no PNG round-trip. Boxes are manual: `box_glyph_data` computes
  Tukey stats per category (median/quartiles/whiskers, `min_box_n` threshold), `point_glyph_data`
  the jittered + highlighted points; both pure and unit-tested with no live server.
  *Done when:* live updates repaint in place (no PNG swap); visual check of boxes + points.
  Commit `build(S2): incremental render via Bokeh`.

- [x] **S3 — `partition?since=` in `sync`.**
  Files: `live.py`. Replace `count` + newest-first walk with the delta query. `sync` now
  pulls the changed-partition delta (`partition_igsns` → per-IGSN `igsn_items` via the
  server-side `meta.igsn` filter), keeps only rows not in `store.seen`, and ingests
  oldest-first by server `created`. The watermark lives on `Store.since` (set by
  `seed_store`, advanced by `sync` to a time captured *before* the query, only on a clean
  pass) so a raised fetch re-queries the same window. `fetch_new_items` removed; the
  count trigger (`PollTrigger`/`poll_count`) is kept as an interim cheap gate and is
  retired with the trigger-seam rework in S4/S5, so the notebook is untouched here.
  *Done when:* new results appear; a forced-missed interval is recovered on the next
  `sync`; `itemId` dedup holds. Commit `build(S3): partition?since= delta in sync`.

- [ ] **S4 — `GirderWebSocketSource` + `row_from_notification`.**
  Files: `live.py`, notebook. Connect to `wss://girder.htmdec.org/notifications/public`,
  auth with `gc.token`, filter `type == "pdv_alpss_result"`, build rows with zero fetch;
  wire connect/reconnect backfill and the fallback heartbeat.
  *Done when:* the §9 manual metadata-edit trigger renders within ~1–2 s; reconnecting after
  a kill backfills the gap; a result seen over the socket then again via `sync` is admitted
  once. Commit `build(S4): GirderWebSocketSource + row_from_notification`.

- [ ] **S5 — Push primary, poll → heartbeat; end-to-end.**
  Files: notebook. Reduce polling to the slow fallback heartbeat.
  *Done when:* a real ALPSS shot drives a live update with no `alpss-dagster` change;
  polling is only the safety net. Commit `build(S5): push primary, poll heartbeat fallback`.
