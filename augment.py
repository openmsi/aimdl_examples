import json

def get_igsn_xrd_links(igsn: str, client):
    """
    Given an IGSN and a Girder client, return matching EBSD/XRD links.
    """

    params = {
        "q": igsn,
        "mode": "igsn",
        "types": '["folder","item"]',
        "limit": 1000,
    }

    resources = client.get("resource/search", parameters=params)
    links = []

    for resource_type_key, resource_type_list in resources.items():
        print(resource_type_key)
        for resource in resource_type_list:
            # Fetch full metadata
            metadata = client.get(
                f"resource/{resource['_id']}",
                parameters={'type': resource_type_key}
            )

            meta = metadata.get("meta", {})

            # Conditions you already use
            kafka_ok = meta.get("KafkaTopic") == "aimdl-xrd"
            dataflow_ok = "dataflowId" in meta and meta["dataflowId"] == "6729631c1f198818440f687d"
            # print(kafka_ok, dataflow_ok)

            link = f"https://data.htmdec.org/#{resource_type_key}/{resource['_id']}"
            # print(link)
            if kafka_ok or dataflow_ok:
                links.append(link)

    return links
