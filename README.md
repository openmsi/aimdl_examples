# README

## Overview

This script processes **laser shock experiments**. Each experiment is associated with a **sample** identified by an **IGSN**, and each experiment has a corresponding **PDV trace**. The script performs the following tasks:

1. Reads an Excel spreadsheet containing at least:

   * `IGSN`
   * `PDV_FileName`

2. For each row:

   * Downloads the PDV file from Girder (using the filename and folder ID).
   * Runs ALPSS on the downloaded PDV file using a provided ALPSS config JSON.
   * Extracts:

     * `velocity-time history`
     * `Flyer velocity`
     * `spall strength`
   * Retrieves **associated EBSD/XRD file links** for that IGSN.
   * Writes the links into the column:

     > `Sample microstructure/material characterization (EBSD/XRD images)`

3. Writes all results into a new Excel file.

Configuration is handled entirely through a `.env` file. Dependencies should be installed using a `requirements.txt` file.

---

## Inputs

1. **Input Excel file** (must contain `IGSN` and `PDV_FileName` columns).
2. **ALPSS configuration JSON** (passed to ALPSS).
3. **`.env` file** containing:

   ```bash
   GIRDER_API_URL=...
   HTMDEC_GIRDER_API_KEY=...
   GIRDER_FOLDER_ID=...
   ```
4. A `requirements.txt` is provided for installing dependencies.

---

## Outputs

The output Excel file will contain all original columns plus:

* `velocity-time history`
* `Flyer velocity`
* `spall strength`
* `Sample microstructure/material characterization (EBSD/XRD images)` (comma-separated links)

---

## Usage

```bash
python augment.py \
    --input-xlsx input.xlsx \
    --output-xlsx output.xlsx \
    --alpss-config config.json
```

---

## Notes

* Existing columns with matching names will be overwritten.
* If a PDV file cannot be found through Girder, the script writes `None` for that experiment.
* EBSD/XRD links are collected using `get_igsn_xrd_links()` and inserted as a single string.
* The Girder client is automatically created using `.env` variables.

---

If you'd like, I can also provide an example `.env`, example Excel input, or a minimal working directory structure.
