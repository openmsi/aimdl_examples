"""Offline tests for the live-dashboard seams (no live server).

Covers the seams §12 S1 extracted from the notebook's fused poll loop:
``PollTrigger`` (trigger), ``sync`` (fetch), and the ``Store`` dedup they rely on.
"""

import numpy as np
import pandas as pd

from aimdl_examples import live
from aimdl_examples.live import SKIP, VELOCITY, Config, PollTrigger, Store, sync


class FakeCountGC:
    """Serves gc.get('aimdl/count') from a scripted list of counts."""

    def __init__(self, counts):
        self.counts = list(counts)

    def get(self, path, parameters=None):
        assert path == "aimdl/count"
        return {"pdv_alpss_result": self.counts.pop(0)}


def _row(item_id):
    return {
        "itemId": item_id,
        "igsn": "JHAMAC00003-S7R5C3",
        "created": pd.Timestamp("2026-02-05", tz="UTC"),
        "experiment_ts": pd.Timestamp("2026-02-05", tz="UTC"),
        VELOCITY: 6.4599,
    }


# --------------------------------------------------------------------------- trigger

def test_poll_trigger_fires_only_on_count_change():
    trigger = PollTrigger(last=5)
    gc = FakeCountGC([5, 5, 7])
    cfg = Config()

    assert trigger.changed(gc, cfg) is None
    assert trigger.changed(gc, cfg) is None
    assert trigger.changed(gc, cfg) == 7


def test_poll_trigger_commit_is_separate_so_failed_sync_retries():
    trigger = PollTrigger(last=5)
    cfg = Config()

    assert trigger.changed(FakeCountGC([7]), cfg) == 7
    assert trigger.last == 5          # not committed: a raised sync would retry
    assert trigger.changed(FakeCountGC([7]), cfg) == 7

    trigger.commit(7)
    assert trigger.changed(FakeCountGC([7]), cfg) is None


# ------------------------------------------------------------------------------ sync

def test_sync_ingests_oldest_first_and_is_idempotent(monkeypatch):
    # fetch_new_items returns newest-first with stop-on-seen, like the real walk
    items = [{"_id": "newer"}, {"_id": "older"}]
    monkeypatch.setattr(live, "fetch_new_items",
                        lambda gc, cfg, seen: [i for i in items if i["_id"] not in seen])
    monkeypatch.setattr(live, "parse_many",
                        lambda gc, batch, workers: [_row(i["_id"]) for i in batch])

    store = Store()
    assert sync(None, store, Config()) == (2, 0, 0)

    seq = {r["itemId"]: r["arrival_seq"] for r in store.rows}
    assert seq["older"] < seq["newer"]          # arrival_seq ascends with shot order

    assert sync(None, store, Config()) == (0, 0, 0)   # itemId dedup: repeat is a no-op
    assert len(store.rows) == 2


# ----------------------------------------------------------------------------- store

def test_store_ingest_error_retries_but_skip_is_permanent():
    store = Store()
    items = [{"_id": "err"}, {"_id": "empty"}, {"_id": "good"}]

    assert store.ingest(items, [None, SKIP, _row("good")]) == (1, 1, 1)
    assert "err" not in store.seen              # transient: retried next sync
    assert {"empty", "good"} <= store.seen      # permanent: never re-downloaded

    assert store.ingest(items, [_row("err"), SKIP, _row("good")]) == (1, 0, 0)
    assert [r["itemId"] for r in store.rows] == ["good", "err"]


def test_store_jitter_is_stamped_once_per_row():
    store = Store(jitter=0.18)
    store.add([_row("a")])
    jit = store.rows[0]["_jit"]
    assert np.isfinite(jit) and abs(jit) <= 0.18
