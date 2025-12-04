import json
import io
import os
import pandas as pd
import numpy as np

# -------------------------------
# Convert a CSV file from Girder into numeric numpy array
# -------------------------------
def download_csv_to_numpy(client, file_link: str) -> np.ndarray:
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

def get_igsn_xrd_links(igsn: str, client):
    params = {
        "q": igsn,
        "mode": "igsn",
        "types": '["folder","item"]',
        "limit": 1000,
    }

    resources = client.get("resource/search", parameters=params)
    links = []

    for resource_type_key, resource_type_list in resources.items():
        for resource in resource_type_list:
            if not resource['name'].endswith('_xrd.csv'):
                continue
            metadata = client.get(
                f"resource/{resource['_id']}",
                parameters={'type': resource_type_key}
            )

            meta = metadata.get("meta", {})
            kafka_ok = meta.get("KafkaTopic") == "aimdl-xrd"
            dataflow_ok = meta.get("dataflowId") == "6729631c1f198818440f687d"

            link = f"https://data.htmdec.org/#{resource_type_key}/{resource['_id']}"
            if kafka_ok or dataflow_ok:
                links.append(link)


    return links


def extract_alpss_from_portal(PDV_FileName, ALPSS_FORM_ID, ALPSS_OUTPUT_FOLDER_ID, client):

    # metrics extraction
    query = PDV_FileName
    results = client.get('entry', parameters={'formId': ALPSS_FORM_ID, 'query': PDV_FileName, 'field': 'file_name', 'limit': 100000})
    if len(results) == 0:
        return None, None, None
    result = results[0] # TODO: pick the most recent

    flyer_velocity = result['data'].get('velocity_at_max_compression', None)
    spall_strength = result['data'].get('spall_strength', None)

    # raw file extraction
    shot_alpss_folder = client.get('folder', parameters={'parentType': 'folder', 'parentId': ALPSS_OUTPUT_FOLDER_ID, 'name': PDV_FileName, 'limit': 100000})
    items = client.get('item', parameters={'folderId': shot_alpss_folder[0]['_id'], 'text': PDV_FileName})
    for item in items:
        if item['meta']['alpss_output_name'] == "smooth_velocity": 
            velocity_time_history_id = item['_id'] #TODO: Handle not found
    if velocity_time_history_id:
        velocity_time_history = f"https://data.htmdec.org/#item/{velocity_time_history_id}"
    else:
        velocity_time_history = None

    return velocity_time_history, flyer_velocity, spall_strength