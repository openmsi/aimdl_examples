# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python utilities library for retrieving and processing experimental data stored in Girder. The project provides three main functions:

1. **`load_csv_numpy(client, file_link)`** — Downloads a CSV from Girder by file link and returns numeric columns as a NumPy array
2. **`find_xrd_files(igsn, client)`** — Searches for all XRD measurement CSVs associated with a sample by its IGSN identifier
3. **`fetch_alpss_metrics(PDV_FileName, ALPSS_FORM_ID, ALPSS_OUTPUT_FOLDER_ID, client)`** — Extracts shock experiment metrics (flyer velocity, spall strength) and velocity-time history traces from ALPSS form entries and output files

All functions operate on a GirderClient instance and are designed to be used interactively in Jupyter notebooks.

## Setup and Configuration

### Environment Setup

Create a `.env` file in the project root with:
```
GIRDER_API_URL=https://girder.igsn.xarthisius.xyz/api/v1
GIRDER_API_KEY=<your_api_key>
GIRDER_FOLDER_ID=<target_folder_id>
```

The environment variables are loaded via `python-dotenv` in notebooks.

### Dependencies

Install with `pip install -r requirements.txt` (numpy, pandas, girder-client, python-dotenv, openpyxl).

## Code Structure

- **`utilities.py`** — Contains the three main data access functions. Each function is standalone and imports only necessary dependencies (no interdependencies).
- **`examples.ipynb`** — Main example notebook showing how to authenticate with Girder, instantiate a client, and call each utility function.
- **`statistics.ipynb`** — Advanced analysis of retrieved data.
- **`all_alpss/alpss_results.ipynb`** — Specialized notebook for bulk ALPSS results processing.
- **`data/`** — Local test data files.

## Working with Girder API

All functions require an authenticated `GirderClient`. The typical usage pattern in notebooks:

```python
from girder_client import GirderClient
from dotenv import load_dotenv
import os

load_dotenv()
client = GirderClient(apiUrl=os.getenv('GIRDER_API_URL'))
client.authenticate(apiKey=os.getenv('GIRDER_API_KEY'))
```

### Key Girder Concepts

- **File links** returned by `find_xrd_files()` are in the format `https://data.htmdec.org/#<resource_type>/<resource_id>` and can be passed directly to `load_csv_numpy()`.
- **IGSN** (International GeoSample Number) is the primary identifier for samples used in `find_xrd_files()`.
- **PDV_FileName** (e.g., "C1--20250605--00087") is used to query ALPSS forms and locate shock experiment output folders.

### Form Queries and Metadata

The `fetch_alpss_metrics()` function queries form entries using:
- `formId` — ID of the ALPSS results form
- `field` — the field to search within (typically "file_name")
- Extracted metrics come from the form's `data` object (e.g., `data.get("velocity_at_max_compression")`)

File outputs are matched by metadata: items with `meta.alpss_output_name == "smooth_velocity"` contain velocity traces.

## Common Development Tasks

### Running Example Notebooks

Open and run `examples.ipynb` in Jupyter to see all functions in action:
```bash
jupyter notebook examples.ipynb
```

### Adding New Data Access Functions

When adding new utilities:
- Keep functions standalone (no interdependencies between utilities)
- Use the existing pattern: authenticate once in the notebook, pass `client` to functions
- Return raw data (NumPy arrays, file links, or native Python types) rather than wrapped objects
- Document the function signature and return type clearly in the docstring

### Testing Against Real Data

Test data is stored in `data/` and used by example notebooks. The repository also queries live Girder instances during development. The `seed_test_data.ipynb` notebook generates example data for testing.

## Important Notes

- **No tests currently exist** — all validation happens via notebooks against real or seeded data.
- **API Keys in .env** — ensure `.env` is in `.gitignore` (it is) and never commit credentials.
- **ALPSS metrics fallback** — `fetch_alpss_metrics()` returns `None` for metrics if no form entry is found, but still attempts to locate the velocity trace file.
- **CSV numeric filtering** — `load_csv_numpy()` automatically excludes non-numeric columns; use the original DataFrame if all columns are needed.
