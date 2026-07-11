# Sync watermark & per-item retry

Status: **hold-on-error shipped**; explicit retry set is a planned follow-up.
Scope: `sync` and `seed_store` in `src/aimdl_examples/live.py`.

This note records two watermark bugs found reviewing the `partition?since=` delta (S3),
the fix that shipped, why it was chosen over the more thorough alternative, and the plan to
do the thorough version later.

## Background: what the watermark is

`sync` is the single REST fetch path (seed + reconnect/heartbeat backfill). It pulls a
*delta*, not a full scan:

1. `partition_igsns(since=store.since)` → the base IGSN of every partition the server
   (re)computed **after** `store.since`.
2. For each, `igsn_items(igsn)` lists that sample's items; keep those **not in
   `store.seen`**.
3. Download + `parse_row` + `Store.ingest`, oldest-first.
4. Advance `store.since`.

`store.since` is the correctness-critical watermark: it defines the lower bound of "what
might have changed." An item is retried **only while its partition still falls within the
query window** — i.e. while `store.since` has not advanced past that partition's update
time. This is the crux of both bugs below: under the old newest-first walk, "leave it
unseen" was enough to guarantee a retry (every tick re-listed the newest items); under the
delta query it is **not** — an unseen item whose partition is behind the watermark is never
re-listed.

## Bug 1 — watermark advanced past per-item parse failures

`Store.ingest` intentionally leaves an errored item (`result is None`, e.g. a transient
download/parse failure) **unseen**, so it retries. But `sync` advanced `store.since`
*unconditionally* after ingest — including when the pass reported `errors > 0`. So:

- The failed item stayed unseen (correct), **but** its partition was now behind the
  watermark, so `partition_igsns` would not return that IGSN again.
- It was therefore retried only if (a) another shot later landed in the *same* IGSN
  partition (re-listing all its unseen items), or (b) a full `since=None` reconciliation
  ran — which nothing schedules.
- A transient failure on the **last shot of an IGSN** was stranded indefinitely.

Note the pre-existing `test_sync_watermark_holds_on_failure_then_recovers` only covered a
*raised whole-query* failure (the exception propagates before the advance line). The
per-item `None` path returned normally and advanced the watermark; it was untested.

### Fix shipped: hold-on-error

`sync` now advances the watermark **only on a fully clean pass** — no per-item errors and
no raised fetch:

```python
added, skipped, errors = store.ingest(items, results)
if errors == 0:                 # a per-item error holds the watermark so it re-lists
    store.since = before
return added, skipped, errors
```

A per-item error now behaves exactly like a raised fetch: `store.since` stays put, the next
`sync` re-queries the same window, re-lists the same partitions, `seen` filters out the
successes, and only the still-failed items re-download. Idempotent via `itemId` dedup.
Covered by `test_sync_watermark_holds_on_per_item_error_then_retries`.

### The tradeoff we accepted: head-of-line blocking

Holding the watermark on *any* error means a **persistently** failing item (a permanent
error that looks transient — a genuinely corrupt CSV, a deleted file, a parser bug) pins
`store.since` and makes every `sync`/heartbeat re-scan the held window forever. The cost is
bounded and self-healing:

- **Bounded** — the re-scan is one window: `partition_igsns` over `[since, now]` plus
  `igsn_items` for those IGSNs; only the still-unseen stuck item(s) actually re-download
  (everything else is `seen`).
- **Self-healing** — the instant the item parses (or is otherwise resolved), the pass goes
  clean and the watermark jumps forward again.

At the heartbeat cadence (~90 s) this is cheap insurance, and it is strictly safer than the
old always-advance behavior (which silently dropped data). That is why hold-on-error was the
right *first* move: minimal (a one-line guard), it makes the code match its own "unseen →
retried" comment, and it errs toward correctness over efficiency. Its only weakness is the
unbounded-in-*time* re-scan for a stuck item — which the retry set below removes.

## Bug 2 — seed watermark captured after the seed (fixed alongside)

`seed_store` captured `store.since = _utcnow_iso()` **after** paging + downloading + parsing
(seconds to minutes). A result uploaded *during* that window is missed by newest-first
offset paging and then excluded from the first delta (its partition time precedes the
post-seed mark). Fixed by snapshotting `before = _utcnow_iso()` at the **top** of
`seed_store` and setting `store.since = before` at the end, so the first live `sync` overlaps
the seed window and backfills anything concurrent. The overlap is cheap — historical seed
items predate `before`, so only genuinely concurrent changes re-list. Covered by
`test_seed_store_snapshots_watermark_before_paging`.

Caveat, shared with Bug 1: this does **not** rescue *seed-time* per-item errors — their
partitions predate `before`, so the first `sync` won't re-list them either. Only partition
churn or a `since=None` sweep recovers those. Out of scope here; the retry set below would
subsume it.

## Planned follow-up: an explicit retry set

**Goal:** retry a failed item **without** pinning the watermark, so a persistent failure
cannot cause an unbounded re-scan, and so seed-time errors are retried too.

**Sketch:**

- Add `Store.retry: dict[str, dict]` (or `set[str]` + a way back to the item) recording the
  itemIds that errored, with a small attempt counter.
- `sync` advances the watermark on any pass (drop the `errors == 0` guard) **but** each
  `sync` additionally re-lists/re-downloads the ids in `retry` regardless of the watermark,
  independent of `partition_igsns`. On success, drop from `retry` and admit; on repeated
  failure, increment the counter.
- Give up after N attempts: mark the item `seen` (so it stops re-downloading) and surface a
  visible "gave up on <itemId>" note — **never** silently. A permanently corrupt result then
  costs N downloads total, not one per heartbeat forever.
- This also covers seed-time errors: seed pushes its errored ids into `retry` instead of
  relying on the watermark.

**Why not do it now.** It adds mutable per-item state, an attempts policy, and a
give-up-with-notification path — more surface than the review that surfaced these bugs
warranted, and more to get right (e.g. not re-admitting a give-up item if it later shows up
via a partition change). Hold-on-error removes the data-loss bug immediately with a one-line,
fully-tested change; the retry set is a *robustness* upgrade (bounds the stuck-item re-scan,
retries seed errors) that can land on its own once someone needs it — i.e. if a real
persistent-failure item is observed re-scanning in production, or seed-time errors prove
common.

**Trigger to build it:** either symptom observed live — a stuck item re-scanning every
heartbeat, or seed-time parse errors that never recover.
