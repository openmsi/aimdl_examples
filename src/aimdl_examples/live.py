"""Incremental fetch and box-plot rendering for a live ALPSS dashboard.

Used by ``tutorials/live_alpss_boxplot.ipynb``, which polls the AIMDL API while the
lab is firing shots and redraws as results land.

Two things here are easy to get wrong and are worth reading before editing.

**Which clock.** A result carries three timestamps and only one is the experiment::

    item.created            when the result was INGESTED
    CSV 'Date' + 'Time'     when ALPSS dagster RAN
    meta.experiment_date    when the shot was FIRED    <- the one we window on

**Which items are "seen".** :meth:`Store.seen` holds every item ever *examined*, not
just the ones that produced a row. Only ~7% of results report a spall strength; if the
rest stayed unseen, every poll would re-walk and re-download them.
"""

from __future__ import annotations

import io
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Iterable, NamedTuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.colors import to_hex

from .alpss import ALPSS_MATERIAL_PROPS_FORM_ID
from .download import get_item_to_memory

__all__ = [
    "VELOCITY", "SPALL", "METRICS", "TIMEFRAMES", "UNKNOWN", "SKIP",
    "Config", "PollTrigger", "Store", "View",
    "parse_experiment_ts", "poll_count", "page_items", "fetch_new_items",
    "parse_row", "parse_many", "build_material_map", "material_for", "seed_store",
    "sync",
    "build_view", "new_item_ids", "material_colors", "stable_ylim",
    "box_glyph_data", "point_glyph_data", "build_bokeh_figure", "update_bokeh",
]


# --------------------------------------------------------------------------- metrics

VELOCITY = "Velocity at Max Compression (m/s)"
SPALL = "Spall Strength (GPa)"

#: Whatever ALPSS reports gets plotted. A analysis with Velocity, Spall Ok, etc,reports no value, and a row
#: with no value for the selected metric is simply absent from the view.
METRICS = {
    VELOCITY: {"src": "Velocity at Max Compression", "scale": 1.0,
               "ylabel": "Velocity at max compression (m/s)"},
    SPALL: {"src": "Spall Strength", "scale": 1e-9,
            "ylabel": "Spall strength (GPa)"},
}

TIMEFRAMES = {
    "all": None,
    "last hour": pd.Timedelta(hours=1),
    "last 24h": pd.Timedelta(days=1),
    "last 7d": pd.Timedelta(days=7),
    "last 30d": pd.Timedelta(days=30),
}

UNKNOWN = "unknown"

#: Parsed fine, nothing to plot — permanent. Distinct from ``None``, which means the
#: download failed and the item should be retried on the next poll.
SKIP = object()


@dataclass(frozen=True)
class Config:
    """Everything the fetch and render paths need. No module-level state."""

    data_type: str = "pdv_alpss_result"
    metric: str = VELOCITY

    # Seeding is driven by IGSN coverage of `metric`, not by an item budget: velocity
    # fills 12 IGSNs in ~400 items, spall takes ~1200.
    seed_igsns: int = 12
    max_seed_items: int = 1500

    poll_seconds: int = 30
    max_workers: int = 12

    window_size: int = 10        # boxes visible at once
    min_box_n: int = 5           # below this, points only -- a box over n=2 is noise
    highlight_last_k: int = 12   # rows counted as "new"
    iqr_mult: float = 3.0        # group-wise outlier trim
    jitter: float = 0.18


# ----------------------------------------------------------------------------- fetch

def parse_experiment_ts(value) -> pd.Timestamp:
    """``meta.experiment_date`` -> tz-aware UTC Timestamp (``NaT`` if absent).

    ``format="ISO8601"`` is load-bearing. ``experiment_date`` is day-granularity today
    (``2026-07-06``) and becomes a full timestamp (``2026-07-06T21:45:25+00:00``) once
    the upstream bug is fixed. Plain ``to_datetime(utc=True)`` infers the format from
    the *first* value it sees and silently coerces the other form to ``NaT`` -- so on
    the day the fix lands mid-corpus, every fixed timestamp would vanish from every
    window with no error. ``utc=True`` also reconciles the tz-naive date-only form
    against the tz-aware fixed form, which otherwise raises on comparison.
    """
    if value is None or value == "":
        return pd.NaT  # to_datetime(None) returns None, not NaT
    return pd.to_datetime(value, utc=True, errors="coerce", format="ISO8601")


def poll_count(gc, cfg: Config) -> int | None:
    """Cheap change-detector: one request, no downloads."""
    return gc.get("aimdl/count").get(cfg.data_type)


@dataclass
class PollTrigger:
    """The trigger seam, poll flavor: "new data exists" when the server count moves.

    ``changed`` and ``commit`` are deliberately separate: the watermark advances only
    after a sync succeeds, so a fetch that raises is retried on the next tick rather
    than silently skipped.
    """

    last: int | None = None

    def changed(self, gc, cfg: Config) -> int | None:
        """-> the new count if it differs from the committed one, else ``None``."""
        count = poll_count(gc, cfg)
        return None if count == self.last else count

    def commit(self, count: int | None) -> None:
        self.last = count


def page_items(gc, cfg: Config, offset: int, limit: int = 50) -> list[dict]:
    return gc.get("aimdl/datafiles", parameters={
        "dataType": cfg.data_type, "limit": limit, "offset": offset,
        "sort": "created", "sortdir": -1,
        "extraFields": '["meta.experiment_date"]',
    })


def fetch_new_items(gc, cfg: Config, seen_ids: set[str]) -> list[dict]:
    """Walk newest-first, stop at the first already-examined item. Newest-first order.

    ``aimdl/datafiles`` has no ``since`` parameter (only ``aimdl/partition`` does), and
    ``created`` is server-assigned at ingest, so newest-first plus stop-on-seen *is* the
    incremental fetch. This is why :func:`aimdl_examples.paginate_datafiles` is not
    reused: it walks every item and issues one GET per result.
    """
    out: list[dict] = []
    offset = 0
    while True:
        batch = page_items(gc, cfg, offset)
        if not batch:
            return out
        for item in batch:
            if item["_id"] in seen_ids:
                return out
            out.append(item)
        if len(batch) < 50:
            return out
        offset += 50


def parse_row(gc, item: dict) -> dict | object | None:
    """``item`` -> row dict, :data:`SKIP` (no metric data), or ``None`` (error).

    Never raises: one bad CSV must not kill a long-running loop.
    """
    try:
        buf = io.BytesIO(b"".join(get_item_to_memory(gc, item)))
        buf.seek(0)
        frame = pd.read_csv(buf)
        if frame.empty:
            return SKIP

        csv_row, meta = frame.iloc[0], item.get("meta", {})
        row = {
            "itemId": item["_id"],
            "igsn": meta.get("igsn"),
            "created": pd.to_datetime(item["created"], utc=True, errors="coerce",
                                      format="ISO8601"),
            "experiment_ts": parse_experiment_ts(meta.get("experiment_date")),
        }

        reported_any = False
        for name, spec in METRICS.items():
            value = pd.to_numeric(csv_row.get(spec["src"]), errors="coerce")
            row[name] = float(value) * spec["scale"] if pd.notna(value) else np.nan
            reported_any |= pd.notna(value)

        return row if reported_any else SKIP
    except Exception as exc:  # noqa: BLE001 - transient; the caller retries
        print(f'  retry {item.get("_id")}: {type(exc).__name__}: {exc}')
        return None


def parse_many(gc, items: list[dict], max_workers: int) -> list:
    if not items:
        return []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        return list(pool.map(lambda item: parse_row(gc, item), items))


def build_material_map(gc) -> dict[str, str]:
    """base IGSN -> materialType. Absent for a large fraction of samples."""
    entries = gc.get("entry", parameters={"formId": ALPSS_MATERIAL_PROPS_FORM_ID})
    return {e["data"]["IGSN"]: e.get("data", {}).get("materialType")
            for e in entries if e.get("data", {}).get("IGSN")}


def material_for(igsn, material_map: dict[str, str]) -> str:
    # base-IGSN split mirrors enrich_alpss_with_material_properties (alpss.py)
    return material_map.get(str(igsn).split("-")[0]) or UNKNOWN


# ----------------------------------------------------------------------------- store

@dataclass
class Store:
    """Rows accumulated across polls.

    ``arrival_seq`` is what "new" means -- an arrival counter, not a clock, so replayed
    and live rows travel the identical highlight path.
    """

    jitter: float = 0.18
    rows: list[dict] = field(default_factory=list)
    seen: set[str] = field(default_factory=set)
    seq: int = 0
    _rng: np.random.Generator = field(default_factory=lambda: np.random.default_rng(10))

    def _admit(self, row: dict) -> None:
        self.seen.add(row["itemId"])
        self.seq += 1
        row["arrival_seq"] = self.seq
        # Stamped once, never recomputed. Jitter recomputed per redraw makes every
        # point twitch on every frame.
        row["_jit"] = float(self._rng.uniform(-self.jitter, self.jitter))
        self.rows.append(row)

    def ingest(self, items: list[dict], results: list) -> tuple[int, int, int]:
        """-> ``(added, skipped, errors)``. Errors stay unseen, so they retry."""
        added = skipped = errors = 0
        for item, result in zip(items, results):
            if item["_id"] in self.seen:
                continue
            if result is None:              # transient: leave unseen for the next poll
                errors += 1
            elif result is SKIP:            # permanent: never download this again
                self.seen.add(item["_id"])
                skipped += 1
            else:
                self._admit(result)
                added += 1
        return added, skipped, errors

    def add(self, rows: Iterable[dict]) -> int:
        """Admit already-parsed rows (the replay path)."""
        count = 0
        for row in rows:
            if row and row is not SKIP and row["itemId"] not in self.seen:
                self._admit(row)
                count += 1
        return count

    def frame(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows) if self.rows else pd.DataFrame()

    def igsns(self, metric: str | None = None) -> set[str]:
        return {r["igsn"] for r in self.rows
                if metric is None or pd.notna(r.get(metric))}


def seed_store(gc, store: Store, cfg: Config,
               log: Callable[[str], None] = print) -> int:
    """Page newest-first until ``seed_igsns`` IGSNs report the metric, ingest oldest-first.

    The two orders are deliberate. Paging must go newest-first -- that is the only order
    ``aimdl/datafiles`` offers, and we want the most recent IGSNs. Ingestion must go
    oldest-first, because ``arrival_seq`` is what "new" means: ingest in page order and
    the highest ``arrival_seq`` lands on the *oldest* row, so the highlight marks
    exactly the wrong points.
    """
    items: list[dict] = []
    results: list = []
    offset = scanned = 0

    while scanned < cfg.max_seed_items:
        batch = page_items(gc, cfg, offset)
        if not batch:
            break
        items += batch
        results += parse_many(gc, batch, cfg.max_workers)
        scanned += len(batch)
        offset += len(batch)

        covered = {r["igsn"] for r in results
                   if r and r is not SKIP and pd.notna(r.get(cfg.metric))}
        log(f"  scanned {scanned:5} | IGSNs reporting {cfg.metric.split(' (')[0]}: {len(covered):3}")
        if len(covered) >= cfg.seed_igsns:
            break

    added, skipped, errors = store.ingest(list(reversed(items)), list(reversed(results)))
    log(f"  ingested +{added} | {skipped} without any metric | {errors} errors")
    return scanned


def sync(gc, store: Store, cfg: Config) -> tuple[int, int, int]:
    """The fetch seam: catch up on everything the store has not seen, parse, ingest.

    -> ``(added, skipped, errors)``. The watermark is ``store.seen`` (newest-first walk
    with stop-on-seen); ingestion goes oldest-first so ``arrival_seq`` ascends with shot
    order. Idempotent via ``itemId`` dedup in :meth:`Store.ingest`, so callers may
    invoke it on any trigger -- poll, reconnect, or heartbeat -- without double-adding.
    """
    items = list(reversed(fetch_new_items(gc, cfg, store.seen)))
    results = parse_many(gc, items, cfg.max_workers)
    return store.ingest(items, results)


# ------------------------------------------------------------------------------ view

class View(NamedTuple):
    """What the plot draws, plus the context it needs to describe itself."""

    rows: pd.DataFrame        # windowed + trimmed, one row per point
    categories: list          # x-axis order, oldest -> newest
    base: pd.DataFrame        # every row reporting the metric; fixes the y-axis
    stats: dict


def metric_base(store: Store, material_map: dict, metric: str) -> pd.DataFrame:
    """Every stored row that reports a value for `metric`, labelled with its material."""
    frame = store.frame()
    if frame.empty:
        return frame
    frame = frame[frame[metric].notna()].copy()
    frame["material_type"] = [material_for(i, material_map) for i in frame["igsn"]]
    return frame


def apply_timeframe(frame: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if timeframe == "all" or frame.empty:
        return frame
    cutoff = pd.Timestamp.now(tz="UTC") - TIMEFRAMES[timeframe]
    return frame[frame["experiment_ts"].notna() & (frame["experiment_ts"] >= cutoff)]


def iqr_filter(frame: pd.DataFrame, xcol: str, ycol: str, cfg: Config) -> pd.DataFrame:
    """Group-wise IQR trim, skipped for groups too small to have a meaningful IQR."""
    if frame.empty:
        return frame
    grouped = frame.groupby(xcol)[ycol]
    q1 = grouped.transform(lambda s: s.quantile(0.25))
    q3 = grouped.transform(lambda s: s.quantile(0.75))
    spread = q3 - q1
    too_small = grouped.transform("size") < cfg.min_box_n
    inside = (frame[ycol] >= q1 - cfg.iqr_mult * spread) & \
             (frame[ycol] <= q3 + cfg.iqr_mult * spread)
    return frame[too_small | inside]


def ordered_categories(frame: pd.DataFrame, xcol: str) -> list:
    """Oldest -> newest.

    ``experiment_ts`` is day-granular today, so every shot fired on one day ties;
    ``created`` breaks the tie with ingestion order. The tiebreak simply stops mattering
    once ``experiment_date`` carries a time.
    """
    if frame.empty:
        return []
    key = frame.groupby(xcol).agg(ts=("experiment_ts", "max"), created=("created", "max"))
    return key.sort_values(["ts", "created"], na_position="first").index.tolist()


def build_view(store: Store, material_map: dict, cfg: Config, *, metric: str, xcol: str,
               timeframe: str, window_size: int, window_offset: int) -> View:
    base = metric_base(store, material_map, metric)
    if base.empty:
        return View(base, [], base, {"n_all": 0, "n_no_ts": 0, "n_cats": 0})

    stats = {"n_all": len(base), "n_no_ts": int(base["experiment_ts"].isna().sum())}
    windowed = iqr_filter(apply_timeframe(base, timeframe), xcol, metric, cfg)

    every = ordered_categories(windowed, xcol)
    stats["n_cats"] = len(every)
    hi = max(0, len(every) - window_offset)
    categories = every[max(0, hi - window_size):hi]

    return View(windowed[windowed[xcol].isin(categories)], categories, base, stats)


def new_item_ids(base: pd.DataFrame, k: int) -> set[str]:
    """Newest arrivals among *plottable* rows.

    Ranking over the whole store instead would flag rows that could never appear under
    the current metric, and the "outside window" badge would count phantoms.
    """
    if base.empty or k <= 0:
        return set()
    return set(base.nlargest(min(k, len(base)), "arrival_seq")["itemId"])


# ---------------------------------------------------------------------------- render
#
# Incremental Bokeh render (§12 S2). The box stats are computed by two *pure* functions
# (box_glyph_data / point_glyph_data) that turn a View into parallel-list glyph columns;
# the figure builder and updater only push those dicts into ColumnDataSources and rescale
# the axes. So a live tick repaints in place via push_notebook -- no savefig/PNG swap.
# Bokeh is imported lazily inside the figure functions, so importing this module (and the
# offline tests over the pure functions) needs no Bokeh installed.

_GRAY_HEX = "#c7c7c7"
_HIGHLIGHT = "#d62728"

_BOX_W = 0.65   # box body width, in category units (matches the old Matplotlib widths)
_CAP_W = 0.2    # whisker cap width

_BOX_KEYS = ("x", "x0", "x1", "xcap0", "xcap1",
             "q1", "q2", "q3", "lower", "upper", "fill", "material")
_POINT_KEYS = ("x", "y", "color", "size")


def material_colors(materials: Iterable[str]) -> dict:
    """material -> hex color. Bokeh wants hex/named colors, not 0-1 RGB tuples."""
    known = sorted({m for m in materials if m and m != UNKNOWN})
    palette = plt.get_cmap("tab10").colors
    colors = {m: to_hex(palette[i % len(palette)]) for i, m in enumerate(known)}
    colors[UNKNOWN] = _GRAY_HEX
    return colors


def stable_ylim(series: pd.Series) -> tuple[float, float]:
    """Fixed off the whole metric-filtered store, not the window.

    A y-axis that rescales every tick makes the animation unreadable. Quantiles so one
    outlier cannot flatten it; the floor is not pinned to zero because velocity goes
    negative on bad fits.
    """
    values = series.dropna()
    if values.empty:
        return 0.0, 1.0
    low, high = min(0.0, float(values.quantile(0.005))), float(values.quantile(0.995))
    if high <= low:
        high = low + 1.0
    pad = 0.05 * (high - low)
    return low - (pad if low < 0 else 0.0), high + pad


def _whiskers(values: np.ndarray, q1: float, q3: float) -> tuple[float, float]:
    """Tukey whiskers: the most extreme datum within 1.5*IQR of the quartiles.

    Matches Matplotlib's default boxplot (whis=1.5, showfliers=False), so the Bokeh boxes
    look like the PNG they replace.
    """
    iqr = q3 - q1
    inside = values[(values >= q1 - 1.5 * iqr) & (values <= q3 + 1.5 * iqr)]
    if inside.size == 0:
        return q1, q3
    return float(inside.min()), float(inside.max())


def box_glyph_data(view: View, xcol: str, metric: str, cfg: Config, colors: dict) -> dict:
    """Per-category Tukey box stats as Bokeh glyph columns. Pure; no Bokeh import.

    Only categories with n >= min_box_n get a box -- a box over n=2 is noise, exactly the
    rule the Matplotlib path used. ``x`` is the category's index in ``view.categories``, so
    boxes and points (which share the same positions) line up.
    """
    data: dict[str, list] = {k: [] for k in _BOX_KEYS}
    positions = {c: i for i, c in enumerate(view.categories)}
    for category in view.categories:
        group = view.rows[view.rows[xcol] == category]
        values = group[metric].dropna().to_numpy()
        if values.size < cfg.min_box_n:
            continue
        q1, q2, q3 = (float(v) for v in np.percentile(values, [25, 50, 75]))
        lower, upper = _whiskers(values, q1, q3)
        material = (category if xcol == "material_type"
                    else group["material_type"].iloc[0])
        x = positions[category]
        data["x"].append(x)
        data["x0"].append(x - _BOX_W / 2)
        data["x1"].append(x + _BOX_W / 2)
        data["xcap0"].append(x - _CAP_W / 2)
        data["xcap1"].append(x + _CAP_W / 2)
        data["q1"].append(q1)
        data["q2"].append(q2)
        data["q3"].append(q3)
        data["lower"].append(lower)
        data["upper"].append(upper)
        data["fill"].append(colors.get(material, _GRAY_HEX))
        data["material"].append(material or UNKNOWN)
    return data


def point_glyph_data(view: View, xcol: str, metric: str,
                     new_ids: set, highlight: bool) -> dict:
    """Every plottable point as scatter columns (x = position + stamped jitter). Pure.

    All rows are drawn, including categories too small for a box, so single shots still
    show. New arrivals (by ``itemId``) are drawn larger and red, matching the old highlight.
    """
    if view.rows.empty:
        return {k: [] for k in _POINT_KEYS}
    positions = {c: i for i, c in enumerate(view.categories)}
    rows = view.rows
    is_new = rows["itemId"].isin(new_ids) & bool(highlight)
    x = rows[xcol].map(positions).astype(float) + rows["_jit"]
    return {
        "x": x.tolist(),
        "y": rows[metric].tolist(),
        "color": [_HIGHLIGHT if n else "black" for n in is_new],
        "size": [10.0 if n else 6.0 for n in is_new],
    }


def build_bokeh_figure(cfg: Config):
    """Create the figure + ColumnDataSources once. Returns ``(figure, sources)``.

    ``sources`` is ``{"box", "pt", "note"}``: the two data sources :func:`update_bokeh`
    refills each tick, plus the empty-timeframe Label. Bokeh is imported here so the module
    imports without it.
    """
    from bokeh.models import ColumnDataSource, Label
    from bokeh.plotting import figure

    box = ColumnDataSource({k: [] for k in _BOX_KEYS})
    pts = ColumnDataSource({k: [] for k in _POINT_KEYS})

    p = figure(width=880, height=420, tools="pan,wheel_zoom,box_zoom,reset,save",
               toolbar_location="above", x_range=(-0.5, 0.5))
    p.xgrid.visible = False
    p.ygrid.grid_line_dash = "dashed"
    p.ygrid.grid_line_alpha = 0.25
    p.xaxis.major_label_orientation = math.pi / 4

    # whiskers + caps, then the box body (colored by material) and median, then points
    p.segment("x", "upper", "x", "q3", source=box, line_color="black", line_width=0.8)
    p.segment("x", "lower", "x", "q1", source=box, line_color="black", line_width=0.8)
    p.segment("xcap0", "upper", "xcap1", "upper", source=box, line_color="black")
    p.segment("xcap0", "lower", "xcap1", "lower", source=box, line_color="black")
    p.vbar("x", _BOX_W, "q1", "q3", source=box, fill_color="fill", fill_alpha=0.35,
           line_color="black", line_width=0.8, legend_field="material")
    p.segment("x0", "q2", "x1", "q2", source=box, line_color="black", line_width=1.5)
    p.scatter("x", "y", source=pts, size="size", color="color", alpha=0.6,
              line_color="white", line_width=0.4)

    p.legend.label_text_font_size = "8px"
    p.legend.location = "top_left"
    p.legend.background_fill_alpha = 0.6

    note = Label(x=440, y=210, x_units="screen", y_units="screen", text="",
                 text_align="center", text_color="#777", text_font_size="12px",
                 visible=False)
    p.add_layout(note)

    return p, {"box": box, "pt": pts, "note": note}


def update_bokeh(p, sources: dict, view: View, cfg: Config, *, xcol: str, metric: str,
                 new_ids: set = frozenset(), highlight: bool = True) -> None:
    """Refill the figure's sources and rescale its axes for ``view``. In place; no PNG.

    Assigning ``source.data`` ships a diff over the widget comm, so the shown plot repaints
    where it is -- the caller only has to ``push_notebook`` afterwards.
    """
    empty = view.rows.empty
    colors = (material_colors(view.rows["material_type"]) if not empty
              else {UNKNOWN: _GRAY_HEX})
    sources["box"].data = box_glyph_data(view, xcol, metric, cfg, colors)
    sources["pt"].data = point_glyph_data(view, xcol, metric, new_ids, highlight)

    n = len(view.categories)
    p.x_range.start, p.x_range.end = -0.5, max(n - 0.5, 0.5)
    p.y_range.start, p.y_range.end = stable_ylim(
        view.base[metric] if not view.base.empty else pd.Series(dtype=float))

    counts = view.rows[xcol].value_counts().to_dict() if not empty else {}
    p.xaxis.ticker = list(range(n))
    p.xaxis.major_label_overrides = {
        i: f"{c} (n={counts.get(c, 0)})" for i, c in enumerate(view.categories)
    }
    p.xaxis.axis_label = {"igsn": "IGSN", "material_type": "Material"}[xcol]
    p.yaxis.axis_label = METRICS[metric]["ylabel"]
    # material colors need no legend when the x-axis already IS material
    p.legend.visible = (xcol == "igsn") and not empty

    note = sources["note"]
    if empty:
        newest = view.base["experiment_ts"].max() if not view.base.empty else pd.NaT
        text = f"no {metric} results in this timeframe"
        if pd.notna(newest):
            text += f" (newest: {newest.date()})"
        note.text, note.visible = text, True
    else:
        note.visible = False
