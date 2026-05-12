"""Shared utilities for downloading data from Girder."""

import os
from pathlib import Path

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
