"""Offline tests for the live-dashboard seams (no live server).

Covers the seams §12 S1 extracted from the notebook's fused poll loop:
``PollTrigger`` (trigger), ``sync`` (fetch), and the ``Store`` dedup they rely on; plus
the §12 S2 pure render helpers (``box_glyph_data`` / ``point_glyph_data``) that feed the
incremental Bokeh figure.
"""

import json

import numpy as np
import pandas as pd
import pytest

from aimdl_examples import live
from aimdl_examples.live import (
    SKIP, SPALL, VELOCITY, Config, GirderWebSocketSource, PollTrigger, Store, View,
    box_glyph_data, point_glyph_data, row_from_notification, sync,
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
#
# sync now pulls the partition?since= delta (partition_igsns -> igsn_items) instead of a
# newest-first walk, so these monkeypatch those two network seams plus _utcnow_iso.

def _item(item_id, created):
    return {"_id": item_id, "created": created}


def test_sync_ingests_delta_oldest_first_and_dedups(monkeypatch):
    # one changed IGSN; its items come back newest-first, like the real listing
    items = [_item("newer", "2026-02-05T02:00:00+00:00"),
             _item("older", "2026-02-05T01:00:00+00:00")]
    monkeypatch.setattr(live, "partition_igsns", lambda gc, cfg, since: {"IG1"})
    monkeypatch.setattr(live, "igsn_items", lambda gc, cfg, igsn: items)
    monkeypatch.setattr(live, "parse_many",
                        lambda gc, batch, workers: [_row(i["_id"]) for i in batch])
    monkeypatch.setattr(live, "_utcnow_iso", lambda: "MARK")

    store = Store()
    assert sync(None, store, Config()) == (2, 0, 0)
    assert store.since == "MARK"                # watermark advanced past this sync

    seq = {r["itemId"]: r["arrival_seq"] for r in store.rows}
    assert seq["older"] < seq["newer"]          # oldest-first by server `created`

    assert sync(None, store, Config()) == (0, 0, 0)   # itemId dedup: repeat is a no-op
    assert len(store.rows) == 2


def test_sync_downloads_only_unseen_items(monkeypatch):
    # igsn_items lists a sample's whole history; only the unseen row is downloaded/parsed
    items = [_item("seen1", "2026-02-05T01:00:00+00:00"),
             _item("fresh", "2026-02-05T02:00:00+00:00")]
    monkeypatch.setattr(live, "partition_igsns", lambda gc, cfg, since: {"IG1"})
    monkeypatch.setattr(live, "igsn_items", lambda gc, cfg, igsn: items)
    downloaded = []

    def fake_parse(gc, batch, workers):
        downloaded.extend(i["_id"] for i in batch)
        return [_row(i["_id"]) for i in batch]

    monkeypatch.setattr(live, "parse_many", fake_parse)
    monkeypatch.setattr(live, "_utcnow_iso", lambda: "MARK")

    store = Store(seen={"seen1"})
    assert sync(None, store, Config())[0] == 1
    assert downloaded == ["fresh"]              # the already-seen item is never re-fetched


def test_sync_watermark_holds_on_failure_then_recovers(monkeypatch):
    queried = []
    calls = {"n": 0}

    def flaky_partition(gc, cfg, since):
        queried.append(since)
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("network down")
        return {"IG1"}

    monkeypatch.setattr(live, "partition_igsns", flaky_partition)
    monkeypatch.setattr(live, "igsn_items",
                        lambda gc, cfg, igsn: [_item("r", "2026-02-05T01:00:00+00:00")])
    monkeypatch.setattr(live, "parse_many",
                        lambda gc, batch, workers: [_row(i["_id"]) for i in batch])
    monkeypatch.setattr(live, "_utcnow_iso", lambda: "MARK")

    store = Store(since="T0")
    with pytest.raises(RuntimeError):
        sync(None, store, Config())
    assert store.since == "T0"                  # not advanced: the missed window retries

    assert sync(None, store, Config()) == (1, 0, 0)   # recovered on the next sync
    assert queried == ["T0", "T0"]              # re-queried the same window; no gap
    assert store.since == "MARK"


def test_seed_store_sets_watermark_to_bound_live_sync(monkeypatch):
    # one page covers the (single) required IGSN, then empty -> seed stops and marks the
    # watermark, so the first live sync pulls only partitions updated after the seed.
    monkeypatch.setattr(live, "_utcnow_iso", lambda: "SEED-MARK")
    monkeypatch.setattr(live, "page_items",
                        lambda gc, cfg, offset: [{"_id": "a"}] if offset == 0 else [])
    monkeypatch.setattr(live, "parse_many",
                        lambda gc, batch, workers: [_row(i["_id"]) for i in batch])

    store = Store()
    live.seed_store(None, store, Config(seed_igsns=1), log=lambda *_: None)
    assert store.since == "SEED-MARK"


# ------------------------------------------------------- push trigger / live path (S4)
#
# The socket path is zero-fetch: row_from_notification builds a plot row straight from the
# announcement's `data`, and GirderWebSocketSource.handle_raw ingests it under `itemId`
# dedup. These exercise the reconciling core with no live server (the socket IO is not
# unit-tested); a raw string mirrors what json.loads sees off the wire, bare NaN included.

def _notification(item_id="6a5101df6d1201b93e76daf8", *, velocity=6.4599,
                  experiment_date="2026-02-05", igsn="JHAMAC00003-S7R5C3",
                  extra=""):
    """A `pdv_alpss_result` frame as a JSON *string*, like the socket delivers (§6)."""
    fields = [
        f'"itemId": "{item_id}"',
        f'"experiment_date": "{experiment_date}"',
        f'"igsn": "{igsn}"',
        f'"Velocity at Max Compression": {velocity}',
        # File Name carries the fuller shot time; row_from_notification must ignore it (§4)
        f'"File Name": "{igsn}_2026-02-05_21-08-12_shot02_ch1.csv"',
    ]
    return '{"type": "pdv_alpss_result", "data": {' + ", ".join(fields) + extra + "}}"


def test_row_from_notification_builds_row_from_data_not_download():
    data = json.loads(_notification())["data"]
    row = row_from_notification(data, received=pd.Timestamp("2026-06-11", tz="UTC"))

    assert row["itemId"] == "6a5101df6d1201b93e76daf8"
    assert row["igsn"] == "JHAMAC00003-S7R5C3"
    assert row[VELOCITY] == 6.4599              # same METRICS["src"] loop as parse_row
    assert row["created"] == pd.Timestamp("2026-06-11", tz="UTC")   # client receipt time


def test_row_from_notification_uses_experiment_date_not_filename():
    # experiment_ts must equal parse_experiment_ts(experiment_date), NOT the fuller shot
    # time in File Name (21-08-12) -- else live rows desync from day-granular REST rows (§4).
    data = json.loads(_notification(experiment_date="2026-02-05"))["data"]
    row = row_from_notification(data)
    assert row["experiment_ts"] == live.parse_experiment_ts("2026-02-05")
    assert row["experiment_ts"] == pd.Timestamp("2026-02-05", tz="UTC")


def test_row_from_notification_scales_spall_like_parse_row():
    data = json.loads(_notification(extra=', "Spall Strength": 3.2e9'))["data"]
    row = row_from_notification(data)
    assert row[SPALL] == pytest.approx(3.2)     # ×1e-9, matching METRICS[SPALL]["scale"]


def test_row_from_notification_skips_when_no_metric_reported():
    # a failed shot is announced too (§8); bare NaN survives json.loads as float('nan') and
    # reads as "no value", so an all-NaN announcement is SKIP -- not admitted to the plot
    data = json.loads(_notification(velocity="NaN"))["data"]
    assert row_from_notification(data) is SKIP


def test_handle_raw_ingests_matching_and_filters_others():
    store = Store()
    src = GirderWebSocketSource(gc=None, store=store, cfg=Config())

    assert src.handle_raw(_notification(item_id="A")) == 1
    assert [r["itemId"] for r in store.rows] == ["A"]

    # firehose noise: a different type is dropped, nothing ingested
    assert src.handle_raw('{"type": "something_else", "data": {}}') is None
    assert len(store.rows) == 1

    # repeat announcement (item.updated re-emits) dedups by itemId -> no-op
    assert src.handle_raw(_notification(item_id="A")) == 0
    assert len(store.rows) == 1


def test_socket_then_sync_admits_item_once(monkeypatch):
    # a result seen first over the socket, then again in a backfill sync, is admitted once
    store = Store()
    src = GirderWebSocketSource(gc=None, store=store, cfg=Config())
    assert src.handle_raw(_notification(item_id="dup")) == 1

    monkeypatch.setattr(live, "partition_igsns", lambda gc, cfg, since: {"IG1"})
    monkeypatch.setattr(live, "igsn_items",
                        lambda gc, cfg, igsn: [_item("dup", "2026-02-05T01:00:00+00:00")])
    monkeypatch.setattr(live, "parse_many",
                        lambda gc, batch, workers: [_row(i["_id"]) for i in batch])
    monkeypatch.setattr(live, "_utcnow_iso", lambda: "MARK")

    assert src.backfill() == (0, 0, 0)          # itemId already seen -> not re-downloaded
    assert len(store.rows) == 1


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
