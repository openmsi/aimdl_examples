"""Offline tests for the live-dashboard seams (no live server).

Covers the seams §12 S1 extracted from the notebook's fused poll loop:
``PollTrigger`` (trigger), ``sync`` (fetch), and the ``Store`` dedup they rely on; plus
the §12 S2 pure render helpers (``box_glyph_data`` / ``point_glyph_data``) that feed the
incremental Bokeh figure.
"""

import numpy as np
import pandas as pd
import pytest

from aimdl_examples import live
from aimdl_examples.live import (
    SKIP, VELOCITY, Config, PollTrigger, Store, View,
    box_glyph_data, point_glyph_data, sync,
)


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


# ----------------------------------------------------------------------- render (S2)

def _point(item_id, category, value, *, jit=0.0, material="steel"):
    return {"itemId": item_id, "igsn": category, "material_type": material,
            "_jit": jit, VELOCITY: value}


def _view(rows, categories):
    frame = pd.DataFrame(rows)
    return View(rows=frame, categories=categories, base=frame,
                stats={"n_all": len(frame), "n_no_ts": 0, "n_cats": len(categories)})


def test_box_glyph_data_only_boxes_categories_meeting_min_n():
    # A: 6 points -> box; B: 2 points -> no box (below min_box_n=5), points still plotted
    rows = [_point(f"a{i}", "IG-A", 6.0 + i) for i in range(6)]
    rows += [_point(f"b{i}", "IG-B", 20.0) for i in range(2)]
    box = box_glyph_data(_view(rows, ["IG-A", "IG-B"]), "igsn", VELOCITY, Config(), {})

    assert box["x"] == [0]                       # only IG-A, at its category index
    assert box["material"] == ["steel"]


def test_box_glyph_data_computes_tukey_stats():
    rows = [_point(f"a{i}", "IG-A", v) for i, v in enumerate([6, 7, 8, 9, 10, 11])]
    colors = {"steel": "#111111"}
    box = box_glyph_data(_view(rows, ["IG-A"]), "igsn", VELOCITY, Config(), colors)

    q1, q2, q3 = np.percentile([6, 7, 8, 9, 10, 11], [25, 50, 75])
    assert (box["q1"][0], box["q2"][0], box["q3"][0]) == (q1, q2, q3)
    assert (box["lower"][0], box["upper"][0]) == (6.0, 11.0)   # no outliers -> data extremes
    assert box["fill"] == ["#111111"]                          # material color mapped


def test_box_glyph_whisker_clips_outlier_to_tukey_fence():
    # one far outlier must not stretch the whisker past q3 + 1.5*IQR
    rows = [_point(f"a{i}", "IG-A", v) for i, v in enumerate([1, 2, 3, 4, 5, 100])]
    box = box_glyph_data(_view(rows, ["IG-A"]), "igsn", VELOCITY, Config(), {})
    assert box["upper"][0] < 100                 # 100 is a flier, excluded from the whisker


def test_point_glyph_data_plots_every_row_and_highlights_new():
    rows = [_point("a0", "IG-A", 6.0, jit=0.1), _point("a1", "IG-A", 7.0, jit=-0.1)]
    pts = point_glyph_data(_view(rows, ["IG-A"]), "igsn", VELOCITY, {"a1"}, highlight=True)

    assert len(pts["x"]) == 2                     # all rows, box-eligible or not
    assert pts["x"] == [0 + 0.1, 0 - 0.1]         # category index + stamped jitter
    assert pts["color"] == ["black", live._HIGHLIGHT]   # only the new itemId is red
    assert pts["size"][1] > pts["size"][0]


def test_point_glyph_highlight_off_suppresses_new_marking():
    rows = [_point("a0", "IG-A", 6.0)]
    pts = point_glyph_data(_view(rows, ["IG-A"]), "igsn", VELOCITY, {"a0"}, highlight=False)
    assert pts["color"] == ["black"]


def test_bokeh_figure_builds_and_updates_in_place():
    pytest.importorskip("bokeh")
    from aimdl_examples.live import build_bokeh_figure, update_bokeh

    rows = [_point(f"a{i}", "IG-A", 6.0 + i) for i in range(6)]
    view = _view(rows, ["IG-A"])
    p, sources = build_bokeh_figure(Config())
    update_bokeh(p, sources, view, Config(), xcol="igsn", metric=VELOCITY, new_ids={"a5"})

    assert len(sources["box"].data["x"]) == 1
    assert len(sources["pt"].data["x"]) == 6
    assert p.xaxis[0].major_label_overrides == {0: "IG-A (n=6)"}
    assert not sources["note"].visible           # note only for an empty timeframe

    empty = _view([], [])
    update_bokeh(p, sources, empty, Config(), xcol="igsn", metric=VELOCITY)
    assert sources["note"].visible and sources["pt"].data["x"] == []
