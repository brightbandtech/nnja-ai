import logging
import os
from importlib import resources
from typing import Any, Dict, Optional

from nnja_ai import io
from nnja_ai.dataset import NNJADataset
from nnja_ai.io import _resolve_path

logger = logging.getLogger(__name__)

# Configuration parameters
STRICT_LOAD = os.getenv("STRICT_LOAD", default=True)

# Available data mirrors
MIRRORS = {
    "gcp_brightband": {
        "base_path": "gs://nnja-ai/data/v1",
        "catalog_json": "catalog.json",
    },
    "gcp_nodd": {
        "base_path": "gs://gcp-nnja-ai/data/v1",
        "catalog_json": "catalog.json",
    },
}

DEFAULT_MIRROR = "gcp_nodd"


class DataCatalog:
    """DataCatalog class for finding and loading NNJA datasets.

    The DataCatalog represents a collection of NNJADataset objects,
    and provides some basic search/list functionality.

    Attributes:
        base_path (str): Base path for resolving relative URIs.
        catalog_uri (str): Full URI to the catalog JSON file.
        catalog_metadata (dict): Metadata of the catalog, loaded from the JSON file.
        datasets (dict): Dictionary of dataset instances or subtypes.
    """

    def __init__(
        self,
        mirror: Optional[str] = DEFAULT_MIRROR,
        base_path: Optional[str] = None,
        catalog_json: Optional[str] = None,
    ):
        """
        Initialize the DataCatalog from a JSON metadata file.

        Args:
            mirror: Name of predefined mirror to use (e.g., 'gcp_nodd', 'aws_opendata').
            base_path: Custom base path for resolving relative URIs. Cannot be used with mirror.
            catalog_json: Custom catalog JSON path. Cannot be used with mirror.

        Note:
            Dataset manifests are now loaded lazily on first access for better performance.

        Raises:
            ValueError: If both mirror and custom parameters are specified.
        """
        # Validate parameters - no mix and match
        if mirror is not None and (base_path is not None or catalog_json is not None):
            raise ValueError(
                "Cannot specify both 'mirror' and custom parameters ('base_path', 'catalog_json'). "
                "Use either a predefined mirror or custom configuration."
            )

        # Configure based on mirror or custom parameters
        if mirror is not None:
            mirror_config = MIRRORS[mirror]  # Let this raise KeyError if invalid
            self.base_path = mirror_config["base_path"]
            catalog_relative_path = mirror_config["catalog_json"]
        else:
            if base_path is None or catalog_json is None:
                raise ValueError(
                    "When not using a predefined mirror, both 'base_path' and 'catalog_json' must be specified."
                )
            self.base_path = base_path
            catalog_relative_path = catalog_json

        # Resolve catalog URI
        self.catalog_uri = _resolve_path(self.base_path, catalog_relative_path)

        import nnja_ai.schemas

        catalog_schema = resources.files(nnja_ai.schemas).joinpath("catalog_schema_v1.json")

        self.catalog_metadata: Dict[str, Dict[str, Any]] = io.read_json(self.catalog_uri, schema_path=catalog_schema)
        self.datasets: Dict[str, NNJADataset] = self._parse_datasets()

    def __getitem__(self, dataset_name: str) -> NNJADataset:
        """
        Fetch a specific dataset by name.

        Args:
            dataset_name: The name of the dataset to fetch.

        Returns:
            NNJADataset: The dataset object.
        """
        return self.datasets[dataset_name]

    def _parse_datasets(self) -> Dict[str, NNJADataset]:
        """
        Parse datasets from the catalog metadata and initialize NNJADataset instances.

        Returns:
            dict: A dictionary of dataset instances or subtypes if multiple exist.

        Note:
            Dataset manifests are loaded lazily on first access for better performance.
        """
        datasets = {}
        for group, group_metadata in self.catalog_metadata.items():
            message_types = group_metadata.get("datasets", {})
            for msg_type, msg_metadata in message_types.items():
                # If there are multiple messages in a group, use message type name in the key.
                key = group if len(message_types) == 1 else group + "_" + msg_metadata["name"]
                try:
                    # Resolve dataset JSON path relative to base_path
                    dataset_json_uri = _resolve_path(self.base_path, msg_metadata["json"])
                    datasets[key] = NNJADataset(dataset_json_uri, self.base_path)
                except Exception as e:
                    if STRICT_LOAD:
                        raise RuntimeError(
                            f"Failed to load dataset for group '{group}', message type '{msg_type}': {e}"
                        ) from e
                    else:
                        logger.warning(f"Could not load dataset for group '{group}', message type '{msg_type}': {e}")
        return datasets

    def info(self) -> str:
        """Provide information about the catalog."""
        return "\n".join(
            f"{group}: {group_metadata['description']}" for group, group_metadata in self.catalog_metadata.items()
        )

    def list_datasets(self) -> list:
        """List all dataset groups."""
        return list(self.datasets.keys())

    def search(self, query_term: str) -> list:
        """
        Search datasets by name, tags, description, or variables.

        Args:
            query_term: The term to search for.

        Returns:
            list: A list of NNJADataset objects matching the search term.
        """
        results = []
        for dataset in self.datasets.values():
            for field in ["name", "description", "tags"]:
                result = getattr(dataset, field)
                if isinstance(result, list):
                    if query_term.lower() in [x.lower() for x in result]:
                        results.append(dataset)
                        break
                elif isinstance(result, str):
                    if query_term.lower() in result.lower():
                        results.append(dataset)
                        break
            for variable in dataset.variables.values():
                for field in ["id", "description"]:
                    result = getattr(variable, field)
                    if isinstance(result, str):
                        if query_term.lower() in result.lower():
                            results.append(dataset)
                            break
                if dataset in results:
                    break
        return results


def generate_catalog():
    # Right now I've hardcoded the catalog json; this should probably be generated
    # by parsing over the dataset jsons

    pass
    pass
