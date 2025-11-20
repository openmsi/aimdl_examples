import json
import io
import os
import argparse
import pandas as pd
from alpss.alpss_main import alpss_main
from dotenv import load_dotenv


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


def process_pdv_file(
    client,
    folder_id: str,
    filename: str,
    config: dict,
    output_dir: str = ".",
    run_alpss: bool = True,
):
    items = client.get("item", parameters={"folderId": folder_id, "name": filename + ".csv"})

    if not items:
        print(f"[WARN] File not found in folder {folder_id}: {filename}")
        return None, None, None

    item_id = items[0]["_id"]
    files = client.get(f"item/{item_id}/files")

    if not files:
        print(f"[WARN] Item exists but has no files: {filename}")
        return None, None, None

    file_id = files[0]["_id"]

    buf = io.BytesIO()
    client.downloadFile(file_id, buf)

    os.makedirs(output_dir, exist_ok=True)
    local_path = os.path.join(output_dir, filename + ".csv")

    buf.seek(0)
    with open(local_path, "wb") as f:
        f.write(buf.read())

    print(f"[OK] Downloaded: {local_path}")

    config = config.copy()
    config["filepath"] = local_path

    if run_alpss:
        print(f"[ALPSS] Running ALPSS on: {local_path}")
        alpss_figure, alpss_artefacts = alpss_main(**config)
        alpss_results = alpss_artefacts["results"]
        return local_path, alpss_results, alpss_artefacts

    return local_path, None, None


def run_and_extract(fname, client, folder_id, config, output_dir="pdv_results"):
    if pd.isna(fname):
        return pd.Series([None, None, None])

    local, results, artefacts = process_pdv_file(
        client=client,
        folder_id=folder_id,
        filename=fname,
        config=config,
        output_dir=output_dir,
        run_alpss=True
    )

    if results is None:
        return pd.Series([None, None, None])

    velocity_time_history = artefacts["velocity"][-1]
    flyer_velocity = results["Velocity at Max Compression"]
    spall_strength = results["Spall Strength"]

    return pd.Series([velocity_time_history, flyer_velocity, spall_strength])


# -------------------------------------------------------
#                     MAIN()
# -------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("excel_path", help="Path to Excel sheet containing PDV_FileName column")
    parser.add_argument("--alpss-config", required=True,
                        help="Path to JSON file containing ALPSS config")
    parser.add_argument("--output-xlsx", default="pdv_results.xlsx",
                        help="Output XLSX file")

    args = parser.parse_args()

    # ---------------------------------------
    # Load environment variables
    # ---------------------------------------
    load_dotenv()

    GIRDER_API_URL = os.getenv("GIRDER_API_URL")
    API_KEY = os.getenv("HTMDEC_GIRDER_API_KEY")
    FOLDER_ID = os.getenv("GIRDER_FOLDER_ID")

    if not all([GIRDER_API_URL, API_KEY, FOLDER_ID]):
        raise RuntimeError("Missing GIRDER_API_URL, HTMDEC_GIRDER_API_KEY, or GIRDER_FOLDER_ID in .env")

    # ---------------------------------------
    # Load ALPSS config
    # ---------------------------------------
    with open(args.alpss_config) as f:
        alpss_config = json.load(f)
    alpss_config['out_files_dir'] = "pdv_results"

    # ---------------------------------------
    # Build Girder client
    # ---------------------------------------
    from girder_client import GirderClient

    client = GirderClient(apiUrl=GIRDER_API_URL)
    client.authenticate(apiKey=API_KEY)

    # ---------------------------------------
    # Load Excel
    # ---------------------------------------
    df = pd.read_excel(args.excel_path)

    if "PDV_FileName" not in df:
        raise ValueError("Excel sheet must contain a column named 'PDV_FileName'")


    df["Sample microstructure/material characterization (EBSD/XRD images)"] = df["Sample_ID"].apply(
        lambda x: get_igsn_xrd_links(x, client)
    )
    # ---------------------------------------
    # Process each PDV file
    # ---------------------------------------
    results = df["PDV_FileName"].apply(
        lambda x: run_and_extract(
            x,
            client=client,
            folder_id=FOLDER_ID,
            config=alpss_config,
            output_dir="pdv_results",
        )
    )

    df["velocity-time history"] = results.apply(lambda r: r[0])
    df["Flyer velocity"] = results.apply(lambda r: r[1])
    df["spall strength"] = results.apply(lambda r: r[2])




    # final = pd.concat([df, results_df], axis=1)

    # ---------------------------------------
    # Save XLSX
    # ---------------------------------------
    df.to_excel(args.output_xlsx, index=False)
    print(f"\n[DONE] Saved results → {args.output_xlsx}")


if __name__ == "__main__":
    main()
