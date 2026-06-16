## Data Download & Processing

This repository provides utilities for downloading and processing experimental data from Girder. Workflows are organized by data modality:

- **`all_alpss/`** — PDV shock experiment data (HELIX)
  - `data_download.ipynb` — Download raw PDV traces and derived outputs (e.g., velocity curves)
  - `alpss_results.ipynb` — Compile ALPSS results into a DataFrame for analysis

- **`all_maxima/`** — XRD diffraction data (MAXIMA)
  - `data_download.ipynb` — Download raw XRD and derived processed files (e.g., intensity curves and scan images)

- **`tutorials/`** — Learning notebooks for the `aimdl/` API and data analysis
  - `01_api_endpoints.ipynb` — Raw coverage of all five `aimdl/` REST endpoints
  - `02_api_walkthrough.ipynb` — Guided tour of the `aimdl_examples` library (auth, pagination, downloads, parsing)
  - `03_alpss_analysis.ipynb` — Visualizations of ALPSS results (quality rates, velocity distributions, per-sample comparisons)
  - `04_xrd_visualization.ipynb` — 2theta vs. intensity plots for a chosen sample (IGSN)

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

Start Jupyter and open notebooks in any of the directories above:

```bash
jupyter notebook
```
