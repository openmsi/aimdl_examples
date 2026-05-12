"""Shared utilities for downloading data from Girder."""

import io
import os
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
    data = {
        k: v.strip()
        for k, v in (line.decode("utf8").split(",") for line in content.readlines())
    }
    data["Date"] = data["Date"] + " " + data.pop("Time")
    data["igsn"] = item["meta"]["igsn"]
    data["itemId"] = item["_id"]
    return data


def coerce_types(df):
    """Convert ALPSS results DataFrame columns to appropriate types."""
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Run Time"] = pd.to_timedelta(df["Run Time"], errors="coerce")
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

    for col in ["Velocity OK", "Spall OK", "Uncertainty OK", "HEL Detected"]:
        if col in df.columns:
            df[col] = df[col].map({"True": True, "False": False})

    return df
