import json
import io
import os
import pandas as pd
import numpy as np

# -------------------------------
# Convert a CSV file from Girder into numeric numpy array
# -------------------------------
def load_csv_numpy(client, file_link: str) -> np.ndarray:
    item_id = file_link.split('/')[-1]

    # Get item files
    files = client.get(f"item/{item_id}/files")
    if not files:
        raise ValueError(f"No files found for item {item_id}")

    file_id = files[0]["_id"]

    # Download
    buf = io.BytesIO()
    client.downloadFile(file_id, buf)
    buf.seek(0)

    # Load CSV
    try:
        df = pd.read_csv(buf)
    except Exception as e:
        raise ValueError(f"Failed reading CSV for {file_link}: {e}")

    # Keep numeric only
    numeric_df = df.select_dtypes(include=["number"])

    return numeric_df.to_numpy()

# -------------------------------
# Get all XRD CSV links (Kafka or Dataflow flagged)
# -------------------------------
def find_xrd_files(igsn: str, client):
    params = {
        "q": igsn,
        "mode": "igsn",
        "types": '["folder","item"]',
        "limit": 1000,
    }

    resources = client.get("resource/search", parameters=params)
    links = {}

    for resource_type_key, resource_type_list in resources.items():
        for resource in resource_type_list:
            if not resource['name'].endswith('_xrd.csv'):
                continue
            links[resource['name']] = f"https://data.htmdec.org/#{resource_type_key}/{resource['_id']}"

    return links


def fetch_alpss_metrics(PDV_FileName, ALPSS_FORM_ID, ALPSS_OUTPUT_FOLDER_ID, client):
    
    # 1) FORM EXTRACTION (flyer_velocity, spall_strength)

    print(f"[INFO] Extracting metrics for: {PDV_FileName}")
    # metrics extraction
    query = PDV_FileName
    results = client.get('entry', parameters={'formId': ALPSS_FORM_ID, 'query': PDV_FileName, 'field': 'file_name', 'limit': 100000})
    if len(results) == 0:
        flyer_velocity = None
        spall_strength = None
    else:
        result = results[0] # TODO: pick most recent — right now first wins
        data = result.get("data", {})
        flyer_velocity = data.get("velocity_at_max_compression")
        spall_strength = data.get("spall_strength")

    # 2) FILE EXTRACTION (velocity trace)

    print(f"[INFO] Extracting velocity trace for: {PDV_FileName}")
    # Find the ALPSS output folder
    folders = client.get(
        "folder",
        parameters={
            "parentType": "folder",
            "parentId": ALPSS_OUTPUT_FOLDER_ID,
            "name": PDV_FileName,
            "limit": 100000,
        },
    )

    if not folders:
        return None, flyer_velocity, spall_strength
    
    folder_id = folders[0]["_id"]

    # Get items under that folder
    items = client.get("item", parameters={"folderId": folder_id})

    velocity_time_history_id = None

    for item in items:
        if item.get("meta", {}).get("alpss_output_name") == "smooth_velocity":
            velocity_time_history_id = item["_id"]
            break

    if velocity_time_history_id:
        link = f"https://data.htmdec.org/#item/{velocity_time_history_id}"
    else:
        link = None

    return link, flyer_velocity, spall_strength