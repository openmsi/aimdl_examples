## Data Download & Processing

This repository provides utilities for downloading and processing experimental data from Girder. The main workflows are organized by data modality:

- **`all_alpss/`** — PDV shock experiment data (HELIX)
  - `raw_data_download.ipynb` — Download raw PDV traces and derived outputs (e.g., velocity curves)
  - `alpss_results.ipynb` — Compile ALPSS results into a DataFrame for analysis

- **`all_maxima/`** — XRD diffraction data (MAXIMA)
  - `raw_data_download.ipynb` — Download raw XRD and derived processed files (e.g., intensity curves and scan images)

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