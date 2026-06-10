"""Shared utilities for downloading data from Girder."""

import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from girder_client import GirderClient


def get_girder_client(session=None):
    """Create and authenticate a Girder client from environment variables."""
    gc = GirderClient(
        apiUrl=os.environ.get("GIRDER_API_URL", "https://girder.htmdec.org/api/v1")
    )
    gc._session = session
    try:
        gc.authenticate(apiKey=os.environ["GIRDER_API_KEY"])
    except KeyError:
        raise RuntimeError("GIRDER_API_KEY environment variable not set")

    me = gc.get("user/me")
    assert me is not None, "Failed to authenticate with Girder API"
    return gc


def paginate_datafiles(gc, data_type, worker_fn, item_filter=None, max_workers=8, **query_params):
    """Paginate aimdl/datafiles and fan out worker_fn over a thread pool.

    Args:
        gc: authenticated GirderClient
        data_type: str, the dataType query parameter (e.g. "pdv_alpss_result")
        worker_fn: callable(item) -> result; executed in thread pool
        item_filter: optional callable(item) -> bool; applied client-side before dispatch
        max_workers: int, max threads in pool (default 8)
        **query_params: additional query parameters forwarded to aimdl/datafiles

    Returns:
        list of results from worker_fn
    """
    results = []
    limit = 50
    offset = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            batch = gc.get(
                "aimdl/datafiles",
                parameters={
                    "limit": limit,
                    "offset": offset,
                    "dataType": data_type,
                    **query_params,
                },
            )
            if not batch:
                break

            # Check original batch size before filtering for pagination logic
            original_batch_size = len(batch)

            if item_filter:
                batch = [item for item in batch if item_filter(item)]

            futures = [executor.submit(worker_fn, item) for item in batch]

            for future in as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    raise RuntimeError(f"Worker failed: {e}") from e

            if original_batch_size < limit:
                break
            offset += limit

    return results


def download_item_to_disk(gc, item, output_dir):
    """Download a Girder item to disk."""
    item_id = item["_id"]
    filename = item["name"]
    filepath = Path(output_dir) / filename
    filepath.parent.mkdir(parents=True, exist_ok=True)

    response = gc.sendRestRequest(
        "GET", f"item/{item_id}/download", stream=True, jsonResp=False
    )
    response.raise_for_status()

    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(chunk_size=65536):
            f.write(chunk)

    return {
        "name": filename,
        "path": str(filepath),
        "size": filepath.stat().st_size,
        "itemId": item_id,
    }


def get_item_to_memory(gc, item):
    """Download a Girder item into memory."""
    item_id = item["_id"]
    response = gc.sendRestRequest(
        "GET", f"item/{item_id}/download", stream=True, jsonResp=False
    )
    response.raise_for_status()
    for chunk in response.iter_content(chunk_size=65536):
        yield chunk


def fetch_and_parse(gc, item):
    """Download a Girder item and parse its results."""
    content = io.BytesIO(b"".join(get_item_to_memory(gc, item)))
    return parse_results(content, item)


def parse_results(content, item):
    """Parse ALPSS results CSV from Girder item."""
    content.seek(0)
    df = pd.read_csv(content)
    # Take first row and convert to dict, dropping NaN values
    data = df.iloc[0].to_dict()
    # Merge Date and Time columns if Time exists
    if "Time" in data:
        data["Date"] = str(data["Date"]) + " " + str(data.pop("Time"))
    data["igsn"] = item["meta"]["igsn"]
    data["itemId"] = item["_id"]
    return data


def coerce_types(df):
    """Convert ALPSS results DataFrame columns to appropriate types."""
    # df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    # df["Run Time"] = pd.to_timedelta(df["Run Time"], errors="coerce")
    numeric_columns = [
        "Velocity at Max Compression",
        "Time at Max Compression",
        "Velocity at Max Tension",
        "Time at Max Tension",
        "Velocity at Recompression",
        "Time at Recompression",
        "Carrier Frequency",
        "Spall Strength",
        "Spall Strength Uncertainty",
        "Strain Rate",
        "Strain Rate Uncertainty",
        "Peak Shock Stress",
        "Spect Time Res",
        "Spect Freq Res",
        "Spect Velocity Res",
        "Signal Start Time",
        "Smoothing Characteristic Time",
        "HEL Strength (GPa)",
        "HEL Uncertainty (GPa)",
        "HEL Free Surface Velocity (m/s)",
        "HEL Time Detection (ns)",
        "HEL Consecutive Points",
        "HEL Segment Duration (ns)",
        "HEL Strain Rate",
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Boolean columns are already booleans from pandas read_csv

    return df
