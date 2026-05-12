"""aimdl_examples: utilities for downloading experimental data from Girder"""

from .download import get_girder_client, download_item_to_disk

__all__ = ["get_girder_client", "download_item_to_disk"]
