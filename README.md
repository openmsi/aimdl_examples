## Utilities

This repository provides helper functions for interacting with experimental data stored in Girder.
You can use these utilities to:

- find all XRD measurement files associated with a sample (through their [IGSN](https://ev.igsn.org/)),
- download XRD CSVs as clean numeric NumPy arrays through their Girder links,
- From sample shock experiments:
   - extract ALPSS-generated metrics (flyer velocity, spall strength),
   - and fetch velocity-time history traces through their Girder links.

Each function is standalone and can be directly imported into notebooks or scripts.

Examples:

```
from augment import find_xrd_files, load_csv_numpy, fetch_alpss_metrics

# Assuming you have an authenticated GirderClient called `client`

# Find XRD files for a sample
xrd_links = find_xrd_files("JHAMAD00001", client)

# Download an XRD CSV as a numeric numpy array
arr = load_csv_numpy(client, xrd_links["scan_point_0_xrd.csv"])

# Fetch ALPSS velocity trace + metrics
link, velocity, spall = fetch_alpss_metrics(
    "C1--20250605--00087",
    ALPSS_FORM_ID,
    RESULTS_FOLDER_ID,
    client
)

```

## Installation

Install Python dependencies:

``` 
pip install -r requirements.txt
```

Create a .env file containing:

```
GIRDER_API_URL=https://data.htmdec.org/api/v1
HTMDEC_GIRDER_API_KEY=your_api_key_here
```

## Usage

Open augment.ipynb to see how each utility works and how to apply them to real data.