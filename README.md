## Data Download & Processing

This repository provides utilities for downloading and processing experimental data from Girder. The main workflows are organized by data modality:

- **`all_alpss/`** — PDV shock experiment data (HELIX)
  - `raw_data_download.ipynb` — Download raw PDV traces and derived outputs (e.g., velocity curves)
  - `alpss_results.ipynb` — Compile ALPSS results into a DataFrame for analysis

- **`all_maxima/`** — XRD diffraction data (MAXIMA)
  - `raw_data_download.ipynb` — Download raw XRD and derived processed files (e.g., intensity curves and scan images)

- **`HTMDEC_download.py`** — Standalone filtered downloader (no Jupyter required)
  - Select data by type, IGSN, material, date range and filename in one pass
  - See [Filtered downloads with `HTMDEC_download.py`](#filtered-downloads-with-htmdec_downloadpy) below

## Installation

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Create a `.env` file in the project root:

```
GIRDER_API_URL=https://data.htmdec.org/api/v1
GIRDER_API_KEY=your_api_key_here
```

Start Jupyter and open notebooks in the `all_alpss/` or `all_maxima/` directories.

---

# Filtered downloads with `HTMDEC_download.py`

A standalone script for pulling HTMDEC/AIMDL data without opening a notebook.
Where the notebooks walk through one modality at a time, this script lets you
combine filters — **data type + IGSN + material + date range + filename** — in a
single query, preview the result before committing to a download, and organize
what comes back by data type and sample.

Its only hard dependency is `requests`; `pandas` is optional and used solely to
parse tables in memory mode.

## Setup

Provide an API key using any one of these (checked in this order):

| Source | How |
|---|---|
| Command line | `--key YOUR_KEY` |
| Environment | `export HTMDEC_API_KEY=YOUR_KEY` (`GIRDER_API_KEY` also works) |
| Explicit file | `--key-file path/to/file` |
| Default file | `htmdec_api_key.txt` next to the script, or `~/.htmdec_api_key` |

The simplest setup is the key file:

```bash
cp htmdec_api_key.example.txt htmdec_api_key.txt
# paste your key into htmdec_api_key.txt, then restrict it:
chmod 600 htmdec_api_key.txt
```

The file holds the bare key on one line; `#` comments and blank lines are
skipped. `KEY=value` lines also parse, so the repo's existing `.env` works
directly:

```bash
python HTMDEC_download.py --key-file .env
```

`htmdec_api_key.txt`, `.htmdec_api_key` and `.env` are all gitignored. **Never
commit a real key.** If one is ever committed, rotate it at data.htmdec.org —
removing the file afterwards does not un-expose it.

## Basic use

Open `HTMDEC_download.py`, edit the `CONFIG` block near the top, and run it —
no command-line flags needed:

```python
DATA_TYPE = ["pdv_trace", "pdv_experiment_log"]
IGSN      = ["JHAMAC00003"]
DATE_FROM = "2026-07-14"
DATE_TO   = "2026-07-14"
FILENAME  = None
MATERIAL  = []

MODE = "dry_run"          # "dry_run" | "disk" | "memory"

OUT_DIR = "./htmdec_data"
TIMESTAMP_SUBFOLDER = True
WORKERS = 8
```

```bash
python HTMDEC_download.py
```

Every setting also has a command-line flag that overrides the file, so one-off
runs don't require editing anything:

```bash
python HTMDEC_download.py --data-type pdv_trace --igsn JHAMAC00003 --mode disk
```

## The three modes

| `MODE` | Downloads? | Writes to disk? | Use it to |
|---|---|---|---|
| `dry_run` | No | `manifest.csv` only | Check what matches, and how big, before committing |
| `disk` | Yes | Files + `manifest.csv` | Get data onto disk for later analysis |
| `memory` | Yes | **Nothing** | Load straight into pandas without leaving files behind |

**Always start with `dry_run`.** It prints a per-type match count and total size,
and writes a `manifest.csv` listing every matching file with its IGSN,
experiment date and size.

## Filters

| Setting | Effect |
|---|---|
| `DATA_TYPE` | One or more types; `["all"]` for everything. An unrecognized name stops the run and suggests the closest valid one. |
| `IGSN` | Exact, base or prefix match. `"JHAMAC00003"` catches every `JHAMAC00003-*` sub-sample. `[]` disables. |
| `DATE_FROM` / `DATE_TO` | Inclusive `YYYY-MM-DD`. Same value on both = a single day. `None` disables. |
| `FILENAME` | Substring match on the filename — e.g. `"velocity"` to thin out `pdv_alpss_output`. |
| `MATERIAL` | Material type, e.g. `["Tantalum"]`. Resolved through the per-sample material form. |

To discover valid values:

```bash
python HTMDEC_download.py --list-types        # every data type on the server
python HTMDEC_download.py --list-materials    # material types + sample counts
```

PDV types are `pdv_trace`, `pdv_experiment_log`, `pdv_alpss_output` and
`pdv_alpss_result`. XRD and XRF types are listed by `--list-types`.

## Output layout

`MODE = "disk"` groups files by data type and by each file's own IGSN:

```
htmdec_data/2026-07-28_14-21-33/
    manifest.csv
    pdv_trace/
        JHAMAC00003-S1R5C3/
            C1--JHAMAC00003-S1R5C3_..._shot01--00000.csv
            ...
        JHAMAC00003-S4R5C2/
            ...
```

Because grouping uses each file's own IGSN, a parent-IGSN query produces one
folder per sub-sample, not a single flat folder.

Set `TIMESTAMP_SUBFOLDER = False` to write straight into `OUT_DIR`. Re-runs then
skip files already present at the right size, which makes an interrupted
download resumable — at the cost of mixing differently-filtered runs in one tree.

## Working in memory

`MODE = "memory"` downloads into RAM and writes nothing. Use `python -i` to keep
the interpreter open afterwards:

```bash
python -i HTMDEC_download.py
```

```python
>>> list(DATA)[:2]
['C1--JHAMAC00003-S1R5C3_..._shot01--00000.csv', ...]
>>> DATA['C1--..._shot01--00000.csv'].df       # DataFrame, or None
>>> DATA['C1--..._shot01--00000.csv'].bytes    # raw bytes, always present
```

From a notebook or another script, `fetch()` takes per-call overrides:

```python
from HTMDEC_download import fetch

DATA = fetch()                                    # uses the CONFIG block
DATA = fetch(igsn=["JHAMAC00003"], date_from="2026-07-14",
             date_to="2026-07-14", data_type=["pdv_trace"])

for name, f in DATA.items():
    if f.df is not None:
        print(name, f.df.shape)
```

Each entry is a `Fetched` object:

| Attribute | Meaning |
|---|---|
| `.bytes` | Raw content — always present |
| `.df` | `pandas.DataFrame` if the file parsed as a table, otherwise `None` |
| `.name` | Filename |
| `.meta` | Girder metadata (IGSN, experiment date, run ID, provenance) |
| `.data_type` | The type it was fetched under |
| `.size` | Byte count |

Comma, tab and whitespace separators are all attempted. Binary files and
anything unparseable keep `.df = None` rather than being dropped, so nothing
disappears silently.

## Troubleshooting

**Fewer files than expected, or zero.** A date range also excludes any file with
no `experiment_date` recorded — the two causes look identical from the outside.
Set `DATE_FROM = DATE_TO = None` and re-run to tell them apart.

**`Unknown data type(s) -- nothing was queried`.** A type name is misspelled; the
error suggests the closest valid one. `pdv_experiment_log` has no "al" in it.

**More files than expected.** `IGSN` matches by prefix, so a parent IGSN pulls
every sub-sample. Narrow it by listing full child IGSNs, or add a `FILENAME`
filter.

**Large downloads.** Check the `dry_run` size first. Raw PDV traces run roughly
5 MB each, so a single sample-day can reach several hundred MB.