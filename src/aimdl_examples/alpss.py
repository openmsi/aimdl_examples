import pandas as pd
from pathlib import Path

ALPSS_NUMERIC_COLUMNS = [
    "Velocity at Max Compression",
    "Time at Max Compression",
    "Velocity at Max Compression Freq Uncertainty",
    "Velocity at Max Compression Vel Uncertainty",
    "Velocity at Max Tension",
    "Time at Max Tension",
    "Velocity at Max Tension Freq Uncertainty",
    "Velocity at Max Tension Vel Uncertainty",
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

ALPSS_MATERIAL_PROPS_FORM_ID = '6a0d9fb2f35ea45f3d915850'

def coerce_types(df):
    """Convert ALPSS results DataFrame columns to appropriate types."""
    for col in ALPSS_NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def extract_alpss_versions(generated_by_str):
    """Extract alpss and alpss_dagster versions from wasGeneratedBy string.

    Args:
        generated_by_str: string like "alpss_dagster/0.1.1 alpss/1.7.1"

    Returns:
        tuple of (alpss_dagster_version, alpss_version) or (None, None)
    """
    import re
    if not generated_by_str:
        return None, None
    match = re.search(r'alpss_dagster/([\d.]+)\s+alpss/([\d.]+)', str(generated_by_str))
    if match:
        return match.group(1), match.group(2)
    return None, None

def get_run_metadata(gc, data_type):
    """Get run metadata from first item (single API call with limit=1)."""
    batch = gc.get(
        "aimdl/datafiles",
        parameters={
            "limit": 1,
            "offset": 0,
            "dataType": data_type,
            "extraFields" : '["meta.prov", "meta.runId"]'

        },
    )

    if not batch:
        return None

    item = batch[0]
    meta = item.get("meta", {})

    run_id = meta.get("runId")
    alpss_ver, dagster_ver = None, None
    if "prov" in meta:
        alpss_ver, dagster_ver = extract_alpss_versions(
            meta['prov']['wasGeneratedBy']
        )

    # Only return None if all three are missing
    if not any([run_id, alpss_ver, dagster_ver]):
        return None

    return {
        "runId": run_id,
        "alpss_version": alpss_ver,
        "dagster_version": dagster_ver,
    }


def write_run_metadata(run_id, alpss_version, dagster_version, output_dir):
    """Write metadata.json with run info to output directory."""
    import json
    metadata = {
        "runId": run_id,
        "alpss_version": alpss_version,
        "dagster_version": dagster_version,
    }
    filepath = Path(output_dir) / "metadata.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(metadata, f, indent=2)


def fetch_and_write_run_metadata(gc, data_type, output_dir):
    """Fetch metadata from first item and write metadata.json to output dir."""
    metadata = get_run_metadata(gc, data_type)
    if metadata:
        write_run_metadata(
            metadata["runId"],
            metadata["alpss_version"],
            metadata["dagster_version"],
            output_dir
        )



def enrich_alpss_with_material_properties(df, gc, igsn_column='igsn'):
    """Fetch material properties from entry form and add to ALPSS DataFrame.

    Queries the /entry endpoint for the given form_id, extracts c0, c_l, density
    from the first measurement in each entry, and adds them as new columns.
    Missing entries result in NaN values.

    Args:
        df: DataFrame with ALPSS results
        gc: authenticated Girder client
        form_id: Girder form ID for material properties (e.g., '6a0d9fb2f35ea45f3d915850')
        igsn_column: name of IGSN column in df (default 'igsn')

    Returns:
        DataFrame with material_c0, material_c_l, material_density columns added
    """
    df = df.copy()

    # Fetch all entries for this form
    entries = gc.get('entry', parameters={'formId': ALPSS_MATERIAL_PROPS_FORM_ID})

    # Build dict mapping IGSN -> entry
    entries_by_igsn = {}
    for entry in entries:
        
        igsn = entry.get('data', {}).get('IGSN')
        if igsn:
            entries_by_igsn[igsn] = entry

    # Initialize columns with NaN
    df['material_type'] = pd.NA
    df['material_thickness'] = pd.NA
    df['material_c0'] = pd.NA
    df['material_c_l'] = pd.NA
    df['material_density'] = pd.NA

    # Fill in values from entries
    for idx, row in df.iterrows():
        igsn = row[igsn_column]
        base_igsn = igsn.split('-')[0] if igsn else None
        entry = entries_by_igsn.get(base_igsn)
        if entry is not None:
            data = entry.get('data', {})
            df.at[idx, 'material_type'] = data.get('materialType')
            df.at[idx, 'material_thickness'] = data.get('materialThickness')

            measurements = data.get('measurements', [])
            if measurements and len(measurements) > 0:
                meas = measurements[0]
                df.at[idx, 'material_c0'] = meas.get('c0')
                df.at[idx, 'material_c_l'] = meas.get('c_l')
                df.at[idx, 'material_density'] = meas.get('density')

    return df
