#!/usr/bin/env python3
"""
Filterable downloader for HTMDEC / AIMDL experimental data (Girder backend).

Select what to download by DATA TYPE, IGSN, MATERIAL, DATE RANGE and/or
FILENAME, then pull raw PDV traces, experiment logs, or processed (ALPSS)
outputs. Files are organized on disk by data type and sample (IGSN).

This mirrors the openmsi/aimdl_examples download flow
(https://github.com/openmsi/aimdl_examples) but adds combined filtering,
a dry-run/manifest mode, and has no dependency beyond `requests`.

--------------------------------------------------------------------------
Data types (file classes)         What it is
  pdv_trace                       Raw PDV velocity traces
  pdv_alpss_output                Processed ALPSS outputs (velocity, smooth,
                                  plots, fits, ...) -- use --filename to pick
  pdv_experiment_log              Experiment metadata / settings logs
  xrd_metadata / xrd_derived /    XRD data products
  xrd_visualization
(The live, authoritative list is printed by --list-types.)
--------------------------------------------------------------------------

Auth: needs a Girder API key. Resolved in this order --
  1. --key on the command line
  2. HTMDEC_API_KEY (or GIRDER_API_KEY) in the environment
  3. --key-file, if given
  4. htmdec_api_key.txt next to this script, or ~/.htmdec_api_key
The key file holds the bare key on one line (a KEY=value line and #comments
also work). Get a key at data.htmdec.org -> account settings -> API keys.
Keep the key file out of version control -- see .gitignore.

Usage
-----
NORMAL USE: edit the CONFIG block just below the imports, then run

    python HTMDEC_download.py

Nothing else is required -- no command-line flags. The CONFIG block sets the
data types, IGSN, date range and what MODE to run in:

    MODE = "dry_run"   preview matches + write manifest.csv, download nothing
    MODE = "disk"      download files into OUT_DIR
    MODE = "memory"    load files into RAM, write nothing to disk

To work with MODE="memory" results interactively:

    python -i HTMDEC_download.py     # then use DATA in the prompt
    DATA['somefile.csv'].df          # pandas DataFrame, or None
    DATA['somefile.csv'].bytes       # raw bytes, always present

or from a notebook / another script:

    from HTMDEC_download import fetch
    DATA = fetch()                        # uses the CONFIG block
    DATA = fetch(igsn=["JHAMAL00016-074"], date_from="2026-07-22")

Every CONFIG value also has a command-line flag that overrides it, so the
old flag-driven style still works (see --help).
"""

import argparse
import csv
import difflib
import io
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("This script needs 'requests'. Install with: pip install requests")

# Only needed for MODE="memory" table parsing; bytes still work without it.
try:
    import pandas as pd
except ImportError:
    pd = None


# ==========================================================================
#  CONFIG -- EDIT THIS, THEN RUN:  python HTMDEC_download.py
# ==========================================================================

# Which data types to pull. Run with --list-types to see all of them.
# PDV options: pdv_trace, pdv_experiment_log, pdv_alpss_output,
#              pdv_alpss_result
DATA_TYPE = ["pdv_trace", "pdv_experiment_log"]

# Sample IGSNs. Matches exact, base ("JHAMAL00016") or prefix, so
# "JHAMAL00016-074" also catches "JHAMAL00016-074-S1". [] = no IGSN filter.
IGSN = ["JHAMAC00003"]

# Experiment date range, "YYYY-MM-DD", both ends inclusive. Set the same
# date on both for a single day. None = no date limit.
# NOTE: any file missing an experiment_date is dropped while this is set --
# if a run returns less than you expect, set both to None and re-check.
DATE_FROM = "2026-07-14"
DATE_TO = "2026-07-14"

# Extra narrowing. FILENAME keeps only names containing this substring
# (e.g. "velocity" to thin out pdv_alpss_output). None / [] = no filter.
FILENAME = None
MATERIAL = []

# "dry_run" -> preview + manifest.csv only, nothing downloaded
# "disk"    -> download files into OUT_DIR
# "memory"  -> load into the DATA dict in RAM, nothing written to disk
MODE = "disk"

# Where MODE="disk" writes. TIMESTAMP_SUBFOLDER=True puts each run in its own
# dated folder; False writes straight into OUT_DIR and makes re-runs resume
# (files already present at the right size are skipped).
OUT_DIR = "./htmdec_data"
TIMESTAMP_SUBFOLDER = True

# Parallel download threads.
WORKERS = 8

# ==========================================================================
#  End of config -- you shouldn't need to change anything below.
# ==========================================================================

MODES = ("dry_run", "disk", "memory")
DEFAULT_API = os.environ.get("GIRDER_API_URL", "https://data.htmdec.org/api/v1")
# Searched in order when neither --key nor the env vars supply a key.
KEY_FILE_NAME = "htmdec_api_key.txt"
DEFAULT_KEY_FILES = [
    Path(__file__).resolve().parent / KEY_FILE_NAME,
    Path.cwd() / KEY_FILE_NAME,
    Path.home() / ".htmdec_api_key",
]
# Girder form holding per-sample material properties (IGSN -> material).
MATERIAL_FORM_ID = "6a0d9fb2f35ea45f3d915850"
# Fields we ask the server to include on each datafile record.
EXTRA_FIELDS = json.dumps(
    ["meta.igsn", "meta.experiment_date", "meta.data_type",
     "meta.prov", "meta.runId"]
)
_print_lock = threading.Lock()


# --------------------------------------------------------------------------
# API key resolution
# --------------------------------------------------------------------------
def _read_key_file(path):
    """Pull the key out of a key file, or return None if there isn't one.

    Accepts a bare key on its own line or a KEY=value line; blank lines and
    # comments are ignored.
    """
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            name, _, value = line.partition("=")
            if name.strip().upper() in ("HTMDEC_API_KEY", "GIRDER_API_KEY"):
                return value.strip().strip("'\"") or None
            continue
        return line
    return None


def load_api_key(cli_key=None, key_file=None):
    """Resolve the API key: --key, then env, then key file. None if absent."""
    if cli_key:
        return cli_key
    for var in ("HTMDEC_API_KEY", "GIRDER_API_KEY"):
        if os.environ.get(var):
            return os.environ[var]
    if key_file:
        # Explicitly requested -- a missing or empty file is an error, not a
        # silent fallthrough.
        path = Path(key_file).expanduser()
        if not path.is_file():
            sys.exit(f"Key file not found: {path}")
        key = _read_key_file(path)
        if not key:
            sys.exit(f"Key file is empty or has no key: {path}")
        return key
    for path in DEFAULT_KEY_FILES:
        if path.is_file():
            key = _read_key_file(path)
            if key:
                return key
    return None


# --------------------------------------------------------------------------
# Minimal Girder REST client (requests-based; no girder_client dependency)
# --------------------------------------------------------------------------
class Girder:
    def __init__(self, api_url, api_key):
        self.api = api_url.rstrip("/")
        self.s = requests.Session()
        r = self.s.post(f"{self.api}/api_key/token", params={"key": api_key})
        r.raise_for_status()
        self.token = r.json()["authToken"]["token"]
        self.s.headers.update({"Girder-Token": self.token})
        me = self.get("/user/me")
        if not me:
            raise SystemExit("Authentication failed: check your API key.")
        print(f"Authenticated as: {me.get('login', 'unknown')}  ({self.api})")

    def get(self, path, **params):
        r = self.s.get(f"{self.api}{path}", params=params)
        r.raise_for_status()
        return r.json()

    def datatypes(self):
        return self.get("/aimdl/datatype")

    def iter_datafiles(self, data_type, page=100, **extra):
        """Yield every datafile item of a given dataType (paginated)."""
        offset = 0
        while True:
            batch = self.get("/aimdl/datafiles", dataType=data_type,
                             limit=page, offset=offset,
                             extraFields=EXTRA_FIELDS, **extra)
            if not batch:
                return
            for item in batch:
                yield item
            if len(batch) < page:
                return
            offset += page

    def material_map(self):
        """Return {base_IGSN: materialType} from the material-properties form."""
        entries = self.get("/entry", formId=MATERIAL_FORM_ID)
        out = {}
        for e in entries or []:
            data = e.get("data", {})
            igsn = data.get("IGSN")
            if igsn:
                out[igsn] = data.get("materialType")
        return out

    def download_item(self, item, dest_dir):
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        path = dest / item["name"]
        size = item.get("size", 0)
        if path.exists() and path.stat().st_size == size and size:
            return {"name": item["name"], "path": str(path),
                    "size": path.stat().st_size, "skipped": True}
        tmp = path.with_suffix(path.suffix + ".part")
        with self.s.get(f"{self.api}/item/{item['_id']}/download",
                        stream=True) as r:
            r.raise_for_status()
            with open(tmp, "wb") as fh:
                for chunk in r.iter_content(chunk_size=1 << 16):
                    fh.write(chunk)
        os.replace(tmp, path)
        return {"name": item["name"], "path": str(path),
                "size": path.stat().st_size, "skipped": False}

    def fetch_item(self, item):
        """Download an item straight into memory; nothing touches disk."""
        r = self.s.get(f"{self.api}/item/{item['_id']}/download")
        r.raise_for_status()
        return r.content


# --------------------------------------------------------------------------
# In-memory results (MODE="memory")
# --------------------------------------------------------------------------
_TABLE_EXTS = (".csv", ".txt", ".tsv", ".dat", "")


class Fetched:
    """One downloaded file held in RAM.

    .bytes is always the raw content. .df is a pandas DataFrame when the file
    parsed as a table, else None -- so binaries and odd formats survive
    instead of being silently dropped.
    """

    __slots__ = ("name", "bytes", "df", "meta", "data_type")

    def __init__(self, name, raw, meta, data_type):
        self.name = name
        self.bytes = raw
        self.meta = meta
        self.data_type = data_type
        self.df = _try_parse(name, raw)

    @property
    def size(self):
        return len(self.bytes)

    def __repr__(self):
        shape = f"df{self.df.shape}" if self.df is not None else "bytes only"
        return f"<Fetched {self.name!r} {human(self.size)} {shape}>"


def _try_parse(name, raw):
    """Best-effort table parse. Returns a DataFrame or None; never raises."""
    if pd is None or Path(name).suffix.lower() not in _TABLE_EXTS:
        return None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None          # binary -- caller keeps .bytes
    for sep in (",", "\t", r"\s+"):
        try:
            df = pd.read_csv(io.StringIO(text), sep=sep, engine="python")
        except Exception:
            continue
        # A wrong separator yields a single glued column; keep looking.
        if df.shape[1] > 1 and len(df):
            return df
    return None


# --------------------------------------------------------------------------
# Filtering
# --------------------------------------------------------------------------
def _parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _exp_date(item):
    raw = item.get("meta", {}).get("experiment_date")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def make_filter(args, material_by_igsn):
    d_from = _parse_date(args.date_from) if args.date_from else None
    d_to = _parse_date(args.date_to) if args.date_to else None
    want_materials = {m.lower() for m in args.material} if args.material else None
    igsns = args.igsn or None
    fname = args.filename.lower() if args.filename else None

    def keep(item):
        meta = item.get("meta", {})
        igsn = meta.get("igsn", "") or ""
        base = igsn.split("-")[0] if igsn else ""
        if igsns and not any(igsn == q or igsn.startswith(q) or base == q
                             for q in igsns):
            return False
        if want_materials is not None:
            mat = (material_by_igsn.get(base) or
                   material_by_igsn.get(igsn) or "")
            if mat.lower() not in want_materials:
                return False
        if d_from or d_to:
            ed = _exp_date(item)
            if ed is None:
                return False
            if d_from and ed < d_from:
                return False
            if d_to and ed > d_to.replace(hour=23, minute=59, second=59):
                return False
        if fname and fname not in item.get("name", "").lower():
            return False
        return True

    return keep


def out_dir_for(item, base_dir, data_type):
    igsn = item.get("meta", {}).get("igsn", "")
    parts = [base_dir, data_type]
    if igsn:
        parts.append(igsn)
    return os.path.join(*parts)


def _closest(name, options):
    """Nearest valid data type to a typo, or None if nothing is close."""
    near = difflib.get_close_matches(name, [o for o in options if o],
                                     n=1, cutoff=0.6)
    return near[0] if near else None


def human(n):
    n = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f}{u}"
        n /= 1024
    return f"{n:.1f}PB"


def _fetch_to_memory(gc, matched, workers):
    """Download every match into RAM. Returns {filename: Fetched}."""
    print(f"\nLoading {len(matched)} file(s) into memory "
          f"with {workers} workers...")
    if pd is None:
        print("  (pandas not installed - .df will be None; .bytes still works)")

    data, n = {}, 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(gc.fetch_item, it): (it, dt)
                for it, dt, _ in matched}
        for fut in as_completed(futs):
            item, dt = futs[fut]
            f = Fetched(item["name"], fut.result(),
                        item.get("meta", {}), dt)
            # Same filename under two data types would collide; qualify it.
            key = f.name if f.name not in data else f"{dt}/{f.name}"
            data[key] = f
            n += 1
            with _print_lock:
                kind = f"df{f.df.shape}" if f.df is not None else "bytes"
                print(f"  [{n}/{len(matched)}] {f.name} "
                      f"({human(f.size)}, {kind})")

    parsed = sum(1 for f in data.values() if f.df is not None)
    print(f"\nIn memory: {len(data)} file(s), "
          f"{human(sum(f.size for f in data.values()))} "
          f"({parsed} parsed as tables). Nothing written to disk.")
    print("Access with:  DATA['<filename>'].df  /  .bytes")
    return data


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Download HTMDEC/AIMDL data filtered by type, IGSN, "
                    "material, date and filename. With no flags, uses the "
                    "CONFIG block at the top of this file.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--key",
                    help="Girder API key. Overrides HTMDEC_API_KEY and the "
                         "key file.")
    ap.add_argument("--key-file",
                    help=f"File holding the API key (default: search "
                         f"{KEY_FILE_NAME} beside this script or in the "
                         f"current directory, then ~/.htmdec_api_key).")
    ap.add_argument("--api-url", default=DEFAULT_API,
                    help=f"Girder API base (default: {DEFAULT_API}).")
    ap.add_argument("--data-type", nargs="+", default=list(DATA_TYPE),
                    help="One or more data types (e.g. pdv_trace "
                         "pdv_experiment_log pdv_alpss_output). "
                         "Use 'all' for every available type.")
    ap.add_argument("--igsn", nargs="+", default=list(IGSN),
                    help="Filter by IGSN (exact, base, or prefix match).")
    ap.add_argument("--material", nargs="+", default=list(MATERIAL),
                    help="Filter by material type (e.g. Tantalum).")
    ap.add_argument("--date-from", default=DATE_FROM,
                    help="Earliest experiment date (YYYY-MM-DD).")
    ap.add_argument("--date-to", default=DATE_TO,
                    help="Latest experiment date (YYYY-MM-DD).")
    ap.add_argument("--filename", default=FILENAME,
                    help="Only files whose name contains this substring "
                         "(e.g. velocity, plots -- useful for pdv_alpss_output).")
    ap.add_argument("--mode", choices=MODES, default=MODE,
                    help=f"dry_run = preview + manifest only; disk = save "
                         f"files; memory = load into RAM (default: {MODE}).")
    ap.add_argument("--out", default=OUT_DIR,
                    help=f"Output directory for --mode disk "
                         f"(default: {OUT_DIR}).")
    ap.add_argument("--no-timestamp", action="store_true",
                    default=not TIMESTAMP_SUBFOLDER,
                    help="Write straight into --out without a timestamp folder.")
    ap.add_argument("--workers", type=int, default=WORKERS,
                    help=f"Parallel download workers (default: {WORKERS}).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Alias for --mode dry_run.")
    ap.add_argument("--list-types", action="store_true",
                    help="Print available data types and exit.")
    ap.add_argument("--list-materials", action="store_true",
                    help="Print material types (and sample counts) and exit.")
    args = ap.parse_args(argv)
    if args.dry_run:
        args.mode = "dry_run"

    api_key = load_api_key(args.key, args.key_file)
    if not api_key:
        sys.exit(
            "No API key found. Do one of:\n"
            f"  - write the key into {DEFAULT_KEY_FILES[0]}\n"
            "  - export HTMDEC_API_KEY=...\n"
            "  - pass --key KEY or --key-file PATH")

    gc = Girder(args.api_url, api_key)

    if args.list_types:
        types = gc.datatypes()
        print("\nAvailable data types:")
        for t in (types or []):
            print(f"  {t}")
        return

    mat_map = {}
    if args.material or args.list_materials:
        mat_map = gc.material_map()

    if args.list_materials:
        counts = {}
        for mat in mat_map.values():
            counts[mat or "(none)"] = counts.get(mat or "(none)", 0) + 1
        print("\nMaterial types (sample count):")
        for mat, n in sorted(counts.items(), key=lambda x: -x[1]):
            print(f"  {n:4d}  {mat}")
        return

    if not args.data_type:
        sys.exit("Specify --data-type (or use --list-types to see options).")

    types = gc.datatypes() or []
    if len(args.data_type) == 1 and args.data_type[0].lower() == "all":
        requested = list(types)
    else:
        requested = args.data_type
        unknown = [t for t in requested if types and t not in types]
        if unknown:
            # Stop rather than warn: an unknown type just returns 0 files,
            # which reads like "no data exists" instead of "you typo'd".
            lines = ["Unknown data type(s) -- nothing was queried:"]
            for t in unknown:
                near = _closest(t, types)
                hint = f"   did you mean '{near}' ?" if near else ""
                lines.append(f"  {t!r}{hint}")
            lines.append("\nAvailable data types:")
            lines += [f"  {t}" for t in types if t]
            sys.exit("\n".join(lines))

    keep = make_filter(args, mat_map)
    base_out = args.out if args.no_timestamp else os.path.join(
        args.out, datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))

    matched = []   # (item, data_type, dest_dir)
    print("\nQuerying...")
    for dt in requested:
        n_type = 0
        for item in gc.iter_datafiles(dt):
            if keep(item):
                matched.append((item, dt, out_dir_for(item, base_out, dt)))
                n_type += 1
        print(f"  {dt}: {n_type} file(s) match")

    total_bytes = sum(it.get("size", 0) for it, _, _ in matched)
    print(f"\nTotal matched: {len(matched)} file(s), {human(total_bytes)}")

    if not matched:
        if args.date_from or args.date_to:
            print("Nothing matched. Note that a date range also excludes any "
                  "file with no experiment_date -- try clearing DATE_FROM/"
                  "DATE_TO to check.")
        return {} if args.mode == "memory" else None

    # ---- memory mode: nothing touches disk -------------------------------
    if args.mode == "memory":
        return _fetch_to_memory(gc, matched, args.workers)

    # Manifest (dry_run and disk)
    os.makedirs(base_out, exist_ok=True)
    manifest = os.path.join(base_out, "manifest.csv")
    with open(manifest, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["itemId", "name", "data_type", "igsn",
                    "experiment_date", "size", "dest"])
        for it, dt, dest in matched:
            m = it.get("meta", {})
            w.writerow([it["_id"], it["name"], dt, m.get("igsn", ""),
                        m.get("experiment_date", ""), it.get("size", 0),
                        os.path.join(dest, it["name"])])
    print(f"Manifest written: {manifest}")

    if args.mode == "dry_run":
        print("Dry run - nothing downloaded.")
        return

    # Per-(data_type,igsn) run metadata.json snapshot
    seen_meta = set()
    for it, dt, dest in matched:
        key = dest
        if key in seen_meta:
            continue
        seen_meta.add(key)
        m = it.get("meta", {})
        if m.get("runId") or "prov" in m:
            os.makedirs(dest, exist_ok=True)
            with open(os.path.join(dest, "metadata.json"), "w") as fh:
                json.dump({"runId": m.get("runId"),
                           "prov": m.get("prov")}, fh, indent=2)

    # Parallel download
    print(f"\nDownloading with {args.workers} workers...")
    done = {"n": 0, "bytes": 0}

    def work(triple):
        item, _, dest = triple
        return gc.download_item(item, dest)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(work, t): t for t in matched}
        for fut in as_completed(futs):
            res = fut.result()
            done["n"] += 1
            done["bytes"] += res["size"]
            tag = "skip" if res.get("skipped") else "ok  "
            with _print_lock:
                print(f"  [{done['n']}/{len(matched)}] {tag} {res['name']} "
                      f"({human(res['size'])})")

    print(f"\nDone. {done['n']} file(s), {human(done['bytes'])} -> {base_out}")


# --------------------------------------------------------------------------
# Notebook / REPL entry point
# --------------------------------------------------------------------------
DATA = {}


def fetch(**overrides):
    """Run using the CONFIG block, returning {filename: Fetched} in memory.

        from HTMDEC_download import fetch
        DATA = fetch()
        DATA = fetch(igsn=["JHAMAL00016-074"], date_from="2026-07-22")

    Any CONFIG name works as a keyword: data_type, igsn, material, date_from,
    date_to, filename, mode, workers.
    """
    # Default to memory mode -- that's the point of calling fetch().
    argv = ["--mode", str(overrides.pop("mode", "memory"))]
    for name, value in overrides.items():
        if value is None:
            continue
        flag = "--" + name.replace("_", "-")
        if isinstance(value, (list, tuple)):
            argv += [flag, *map(str, value)]
        else:
            argv += [flag, str(value)]
    return main(argv) or {}


if __name__ == "__main__":
    DATA = main() or {}