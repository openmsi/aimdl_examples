# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture

This is a data download and processing toolkit for experimental datasets (PDV shock data, XRD diffraction) stored in Girder. The codebase has two main parts:

**Core library** (`src/aimdl_examples/`):
- `download.py` — Girder client authentication, item download (disk/memory), CSV parsing, and paginated datafile queries with thread pool workers
- `alpss.py` — ALPSS-specific helpers: type coercion, version extraction, run metadata fetching/writing
- Exports are consolidated in `__init__.py` for use by notebooks

**Jupyter notebooks** (`all_alpss/`, `all_maxima/`):
- `data_download.ipynb` — Download raw data files (PDV traces, XRD scans) and derived outputs, organized by sample ID (IGSN)
- `alpss_results.ipynb` (ALPSS only) — Compile and analyze ALPSS results into a DataFrame

## Environment Setup

Create a `.env` file in the project root:
```
GIRDER_API_URL=https://data.htmdec.org/api/v1
GIRDER_API_KEY=your_api_key_here
```

Install dependencies:
```bash
pip install -r requirements.txt
```

Start a Jupyter kernel to run notebooks:
```bash
jupyter notebook
```

## API Reference

### Authentication (`download.py`)
- `get_girder_client(session=None)` — Create and authenticate a GirderClient from `GIRDER_API_KEY` and `GIRDER_API_URL` environment variables

### Downloading & Parsing (`download.py`)
- `download_item_to_disk(gc, item, output_dir)` — Download item to disk; returns dict with name, path, size, itemId
- `get_item_to_memory(gc, item)` — Stream item chunks into memory (generator)
- `fetch_and_parse(gc, item)` — Download item and parse ALPSS results CSV into dict with metadata enriched from item
- `parse_results(content, item)` — Parse ALPSS CSV content and enrich with IGSN, runId, version info from item metadata

### Pagination & Bulk Operations (`download.py`)
- `paginate_datafiles(gc, data_type, worker_fn, item_filter=None, max_workers=8, **query_params)` — Paginate `aimdl/datafiles` endpoint and fan out `worker_fn` over thread pool
  - Paginates with limit=50, offset increments; checks original batch size to detect end
  - Applies `item_filter(item) -> bool` client-side before dispatch if provided
  - Uses ThreadPoolExecutor (default 8 workers); worker exceptions re-raised as RuntimeError
  - Returns list of results from all workers

### ALPSS Data Transformation (`alpss.py`)
- `coerce_types(df)` — Convert DataFrame columns in `ALPSS_NUMERIC_COLUMNS` to float; returns modified df
- `extract_alpss_versions(generated_by_str)` — Parse alpss and alpss_dagster versions from `wasGeneratedBy` string; returns tuple (alpss_dagster_ver, alpss_ver) or (None, None)
- `get_run_metadata(gc, data_type)` — Fetch runId, alpss_version, dagster_version from first item in batch
- `fetch_and_write_run_metadata(gc, data_type, output_dir)` — Fetch metadata from first item and write `metadata.json` to output_dir

### Utilities (`download.py` and `alpss.py`)
- `get_output_dir(item, base_dir)` — Build output path organized by IGSN; falls back to base_dir if IGSN missing
- `ALPSS_NUMERIC_COLUMNS` (constant in `alpss.py`) — List of ~27 velocity, stress, and timing columns for type coercion

## Notebook Patterns

**Raw data downloads** (`data_download.ipynb` in both `all_alpss/` and `all_maxima/`):
- User selects DATA_TYPE (e.g., "pdv_trace", "pdv_alpss_output") and optional filename filter
- Fetches run metadata and writes `metadata.json`
- Fans out `download_item_to_disk` over a thread pool via `paginate_datafiles`
- Files organized by IGSN subdirectories via `get_output_dir`

**Analysis notebooks** (`alpss_results.ipynb` in `all_alpss/`):
- Queries all `pdv_alpss_result` items
- Uses `paginate_datafiles` with `fetch_and_parse` as worker to build list of result dicts
- Builds DataFrame from dicts and applies `coerce_types` for numeric column conversion

## Item Metadata Structure

Girder items returned by `aimdl/datafiles` have this nested structure:
```python
item = {
    "_id": "...",
    "name": "...",
    "meta": {
        "igsn": "ABC-12345",           # Sample identifier
        "runId": "run_001",            # Experiment run ID
        "prov": {
            "wasGeneratedBy": "alpss_dagster/0.1.1 alpss/1.7.1"
        }
    }
}
```

## Key Concurrency Detail

`paginate_datafiles` parallelizes work across batches of items using a ThreadPoolExecutor. Worker exceptions are aggregated and re-raised as a single RuntimeError after the batch completes, so all items in a batch are attempted even if some fail. The pagination loop detects batch exhaustion by checking whether the returned batch size is less than the limit (50), not by checking an explicit "done" flag.
